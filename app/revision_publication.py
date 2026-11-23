"""Publish counted cards through the existing storefront and order workflow."""
from sqlalchemy import Column, ForeignKey, Integer, event, inspect, text
from sqlalchemy.orm import Session, relationship


WAREHOUSE_SETTING = "revision_public_warehouse_id"


def register_revision_publication(site):
    class RevisionPublication(site.Base):
        __tablename__ = "revision_publications"
        revision_id = Column(Integer, ForeignKey("revision_parts.id"), primary_key=True)
        part_id = Column(Integer, ForeignKey("parts.id", ondelete="CASCADE"), nullable=False, unique=True)

    site.Part.revision_publication = relationship(
        RevisionPublication, uselist=False, cascade="all, delete-orphan", lazy="selectin")

    def publication_warehouse(db):
        # All workers share one managed warehouse. Never adopt a user's warehouse by name.
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

    def publish(db, row, warehouse=None):
        link = db.get(RevisionPublication, row.id)
        part = db.get(site.Part, link.part_id) if link else None
        if part is None:
            warehouse = warehouse or publication_warehouse(db)
            part = site.Part(warehouse_id=warehouse.id, part_number=row.part_number, created_at=row.created_at)
            db.add(part)
            db.flush()
            if link:
                link.part_id = part.id
            else:
                part.revision_publication = RevisionPublication(revision_id=row.id)
        for field in ("part_number", "brand", "producer_type", "name", "description",
                      "price_usd", "qty", "photo_urls", "youtube_url"):
            setattr(part, field, getattr(row, field))
        # Legacy stock cards use EAN-8; revision also accepts longer manufacturer barcodes.
        # Preserve the manufacturer's code in revision without overflowing parts.barcode.
        if len(row.barcode or "") <= 8:
            part.barcode = row.barcode
        site.ensure_part_barcode(db, part)
        part.brand_export = row.brand
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

    def backfill(db):
        rows = db.query(site.RevisionPart).order_by(site.RevisionPart.id).all()
        if not rows:
            return
        warehouse = publication_warehouse(db)
        for row in rows:
            lock_publication(db, row.id)
            db.refresh(row, with_for_update=True)
            publish(db, row, warehouse)

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
