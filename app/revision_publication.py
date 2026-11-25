"""Publish counted cards through the existing storefront and order workflow."""
import json
import os
from datetime import datetime
from uuid import uuid4

from sqlalchemy import Column, ForeignKey, Integer, event, inspect, text
from sqlalchemy.orm import Session, relationship

from .inventory_revision import number_key


WAREHOUSE_SETTING = "revision_public_warehouse_id"
STOCK_MIGRATION_SETTING = "revision_stock_move_v1"
MOVED_NOTE_PREFIX = "Перенесено в ревізію: картка №"


def register_revision_publication(site):
    class RevisionPublication(site.Base):
        __tablename__ = "revision_publications"
        revision_id = Column(Integer, ForeignKey("revision_parts.id"), primary_key=True)
        part_id = Column(Integer, ForeignKey("parts.id", ondelete="CASCADE"), nullable=False, unique=True)

    site.Part.revision_publication = relationship(
        RevisionPublication, uselist=False, cascade="all, delete-orphan", lazy="selectin")

    def publication_warehouse(db):
        # All workers share one managed warehouse. Never adopt a user's warehouse by name.
        setting = db.query(site.ApiSetting).filter_by(setting_key=WAREHOUSE_SETTING).first()
        warehouse = db.get(site.Warehouse, int(setting.setting_value)) if setting and setting.setting_value.isdigit() else None
        if warehouse is not None:
            return warehouse
        if db.get_bind().dialect.name == "postgresql":
            db.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": 726576660002})
        setting = db.query(site.ApiSetting).filter_by(setting_key=WAREHOUSE_SETTING).first()
        warehouse = db.get(site.Warehouse, int(setting.setting_value)) if setting and setting.setting_value.isdigit() else None
        if warehouse is None:
            name = "Ревізія"
            suffix = 1
            while db.query(site.Warehouse.id).filter_by(name=name).first():
                name = f"Ревізія ({suffix})"
                suffix += 1
            warehouse = site.Warehouse(name=name, showcase_name="Перевірена наявність", markup_percent=0)
            db.add(warehouse)
            db.flush()
            site.set_setting(db, WAREHOUSE_SETTING, str(warehouse.id))
        return warehouse

    def stock_candidates(db):
        candidates = {}
        for identifier, number in db.query(site.Part.id, site.Part.part_number).filter(
                site.Part.is_deleted.is_(False), ~site.Part.revision_publication.has()).order_by(site.Part.id):
            key = number_key(number or "")
            if key:
                candidates.setdefault(key, []).append(identifier)
        return candidates

    def move_references(db, old_ids, part_id):
        # Preserve order snapshots/prices and integration mappings, changing only their stock pointer.
        for mapper in site.Base.registry.mappers:
            model = mapper.class_
            if model is RevisionPublication:
                continue
            for column in mapper.local_table.columns:
                if any(fk.target_fullname == "parts.id" for fk in column.foreign_keys):
                    db.query(model).filter(getattr(model, column.key).in_(old_ids)).update(
                        {column.key: part_id}, synchronize_session="fetch")

    def publish(db, row, warehouse=None, candidates=None):
        warehouse = warehouse or publication_warehouse(db)
        candidates = stock_candidates(db) if candidates is None else candidates
        identifiers = candidates.get(number_key(row.part_number), [])
        matches = db.query(site.Part).filter(site.Part.id.in_(identifiers),
            site.Part.is_deleted.is_(False), ~site.Part.revision_publication.has()).order_by(
                site.Part.id).with_for_update().all() if identifiers else []
        link = db.get(RevisionPublication, row.id)
        part = db.get(site.Part, link.part_id) if link else None
        if part is None:
            # Reuse the original card and its ID instead of introducing a second stock pool.
            part = matches[0] if matches else site.Part(
                warehouse_id=warehouse.id, part_number=row.part_number, created_at=row.created_at)
            if not matches:
                db.add(part)
                db.flush()
            if link:
                link.part_id = part.id
            else:
                part.revision_publication = RevisionPublication(revision_id=row.id)
        duplicates = [other for other in matches if other.id != part.id]
        if duplicates:
            move_references(db, [other.id for other in duplicates], part.id)
            for other in duplicates:
                # Recoverable archive: the original data is retained, but it cannot sell twice.
                other.qty = 0
                other.in_stock = False
                other.is_deleted = True
                other.deleted_at = datetime.utcnow()
                other.storage_location = ""
                other.stock_check_note = f"{MOVED_NOTE_PREFIX}{part.id}"
        part.warehouse = warehouse
        source_brand = part.brand_export or part.brand
        for field in ("part_number", "brand", "producer_type", "name", "description",
                      "price_usd", "qty", "photo_urls", "youtube_url"):
            setattr(part, field, getattr(row, field))
        # Legacy stock cards use EAN-8; revision also accepts longer manufacturer barcodes.
        # Preserve the manufacturer's code in revision without overflowing parts.barcode.
        if len(row.barcode or "") <= 8:
            part.barcode = row.barcode
        site.ensure_part_barcode(db, part)
        part.brand_export = row.brand or source_brand
        part.part_number_export = row.part_number
        part.avtopro_flag_1 = site.AVTOPRO_CONDITION_FLAGS[site.part_condition_code(row)]
        part.showcase_photo_urls = row.photo_urls
        part.storage_location = row.location
        part.has_photo = bool(site.parse_media_urls(row.photo_urls))
        part.has_description = bool(row.description)
        part.in_stock = row.qty > 0
        part.is_deleted = False
        part.deleted_at = None
        part.stock_checked_at = row.verified_at
        part.stock_check_status = "checked_ok"
        part.updated_at = row.verified_at
        return part

    def lock_publication(db, revision_id):
        link = db.get(RevisionPublication, revision_id)
        if link:
            # Checkout locks Part first; revision edits use the same order.
            db.query(site.Part).filter_by(id=link.part_id).with_for_update().first()

    def backup_before_move(db):
        """Back up stock and FK pointers before the one-time cleanup; never include credentials."""
        tables = {"parts", "part_templates", "receiving_draft_items", "transit_orders",
                  "revision_parts", "revision_publications", "warehouses", "storage_locations"}
        snapshot = {"created_at": datetime.utcnow().isoformat(), "tables": {}, "references": {}}
        for table in site.Base.metadata.sorted_tables:
            if table.name in tables:
                snapshot["tables"][table.name] = [dict(row) for row in db.execute(table.select()).mappings()]
            else:
                pointers = [column for column in table.columns
                            if any(fk.target_fullname == "parts.id" for fk in column.foreign_keys)]
                if pointers:
                    columns = list(table.primary_key.columns) + pointers
                    snapshot["references"][table.name] = [dict(row) for row in db.execute(
                        table.select().with_only_columns(*columns)).mappings()]
        site.BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        path = site.BACKUP_DIR / f"revision_stock_before_move_{datetime.utcnow():%Y%m%dT%H%M%S}_{uuid4().hex[:8]}.json"
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(snapshot, stream, ensure_ascii=False, default=str)
            stream.flush()
            os.fsync(stream.fileno())
        return path

    def backfill(db):
        if db.get_bind().dialect.name == "postgresql":
            db.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": 726576660003})
        migration = db.query(site.ApiSetting).filter_by(setting_key=STOCK_MIGRATION_SETTING).first()
        # Lock Part before RevisionPart, matching checkout's stock-lock order.
        db.query(site.Part).order_by(site.Part.id).with_for_update().all()
        backup = backup_before_move(db) if not migration else None
        warehouse = publication_warehouse(db)
        if not migration:
            db.query(site.Part).filter(site.Part.warehouse_id != warehouse.id).update(
                {"storage_location": ""}, synchronize_session="fetch")
            for model in (site.PartTemplate, site.ReceivingDraftItem, site.TransitOrder):
                db.query(model).update({"storage_location": ""}, synchronize_session="fetch")
        rows = db.query(site.RevisionPart).order_by(site.RevisionPart.id).all()
        candidates = stock_candidates(db)
        for row in rows:
            lock_publication(db, row.id)
            db.refresh(row, with_for_update=True)
            publish(db, row, warehouse, candidates)
        if not migration:
            site.set_setting(db, STOCK_MIGRATION_SETTING, json.dumps({"backup": str(backup),
                "completed_at": datetime.utcnow().isoformat()}, ensure_ascii=False))
            site.app.logger.info("Revision stock cleanup backed up to %s", backup)

    @event.listens_for(Session, "before_flush")
    def sync_counted_stock(db, flush_context, instances):
        # Checkout, cancellation and warehouse issue paths all change Part.qty.
        # Keep the counted balance in the same transaction, with no second stock pool.
        for part in list(db.dirty):
            if not isinstance(part, site.Part) or not inspect(part).attrs.qty.history.has_changes():
                continue
            link = part.revision_publication
            if not link:
                continue
            row = db.query(site.RevisionPart).filter_by(id=link.revision_id).with_for_update().first()
            if row and row.qty != part.qty:
                row.qty = max(int(part.qty or 0), 0)
                row.version = (row.version or 0) + 1
                part.in_stock = row.qty > 0

    site.RevisionPublication = RevisionPublication
    site.publish_revision_part = publish
    site.lock_revision_publication = lock_publication
    site.publish_existing_revision_parts = backfill
