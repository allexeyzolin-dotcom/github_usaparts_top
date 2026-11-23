"""Required warehouse positions for manual inventory writes and imports."""
import json
import re
import unicodedata
from urllib.parse import urlsplit

from flask import g, has_request_context, jsonify, redirect, request, session, flash, url_for
from sqlalchemy import Column, Integer, String, event, inspect
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .inventory_revision import number_key

SITE = None


def revision_locations(db):
    """One transaction-local lookup, shared by warehouse cards and order payloads."""
    cached = db.info.get("revision_locations")
    if cached is not None:
        return cached
    with db.no_autoflush:
        rows = db.query(SITE.RevisionPart.id, SITE.RevisionPart.part_number, SITE.RevisionPart.location).order_by(
            SITE.RevisionPart.verified_at, SITE.RevisionPart.id).all()
    changes = [row for row in list(db.new) + list(db.dirty) if isinstance(row, SITE.RevisionPart)
               and (row in db.new or any(inspect(row).attrs[field].history.has_changes() for field in ("location", "part_number")))]
    replaced = {row.id for row in list(db.deleted) + changes if isinstance(row, SITE.RevisionPart) and row.id is not None}
    result = {}
    for row in rows:
        if row.id not in replaced and (row.location or "").strip():
            result[number_key(row.part_number or "")] = clean_location(row.location)
    # The source can be saved in the same flush as a newly received stock card.
    for row in changes:
        if (row.location or "").strip():
            result[number_key(row.part_number or "")] = clean_location(row.location)
    db.info["revision_locations"] = result
    return result


def effective_location(record=None, *, db=None, part_number=""):
    state = inspect(record, raiseerr=False) if record is not None else None
    db = db if db is not None else getattr(state, "session", None)
    number = getattr(record, "part_number", "") or part_number
    shared = revision_locations(db).get(number_key(number or ""), "") if db is not None and SITE else ""
    return shared or str(getattr(record, "storage_location", getattr(record, "location", "")) or "").strip()


def clean_location(value, required=True):
    if not isinstance(value, str):
        raise ValueError("Вкажіть місце зберігання на складі.")
    value = " ".join(unicodedata.normalize("NFKC", value).split())
    if len(value) > 255 or (required and not value):
        raise ValueError("Вкажіть місце зберігання на складі (до 255 символів).")
    return value


def request_location():
    return getattr(g, "stock_location", "") if has_request_context() else ""


def request_location_for(number):
    return (getattr(g, "stock_location_map", {}).get((number or "").upper(), request_location())
            if has_request_context() else "")


def migrate_storage_locations(engine):
    """Additive migration: existing stock quantities and cards are preserved."""
    with engine.begin() as connection:
        for table in ("parts", "part_templates", "receiving_draft_items", "transit_orders"):
            columns = {column["name"] for column in inspect(connection).get_columns(table)}
            if "storage_location" not in columns:
                connection.exec_driver_sql(f"ALTER TABLE {table} ADD COLUMN storage_location VARCHAR(255) NOT NULL DEFAULT ''")


def register_storage_locations(site):
    global SITE
    SITE = site
    site.app.jinja_env.globals["storage_location_for"] = effective_location
    class StorageLocation(site.Base):
        __tablename__ = "storage_locations"
        id = Column(Integer, primary_key=True)
        name = Column(String(255), nullable=False)
        name_key = Column(String(1024), nullable=False, unique=True, index=True)

    models = (site.Part, site.PartTemplate, site.ReceivingDraftItem, site.TransitOrder, site.RevisionPart)
    singles = {"create_part", "update_part", "admin_part_autosave",
               "admin_receiving_update", "admin_receiving_legacy",
               "api_mobile_receiving_add_item", "api_mobile_receiving_add_item_legacy",
               "revision", "api_mobile_revision_submit", "api_mobile_verification_check"}

    def rejected(message):
        if request.path.startswith("/api/") or request.is_json or request.endpoint == "admin_part_autosave":
            return jsonify(ok=False, error="storage_location_required", message=message), 400
        flash(message, "error")
        previous = urlsplit(request.referrer or "")
        target = previous.path + ("?" + previous.query if previous.query else "")
        if previous.netloc != request.host or not target.startswith("/admin/"):
            target = url_for("admin_parts")
        return redirect(target)

    @site.app.before_request
    def require_storage_location():
        if request.method != "POST" or not request.endpoint:
            return None
        if request.path.startswith("/admin/") and not session.get("admin_auth"):
            return None
        values = request.form if request.form else (request.get_json(silent=True) or {})
        if not hasattr(values, "get"):
            return None
        try:
            if request.endpoint in {"admin_transit_draft_add", "admin_transit_update"}:
                # A shipment has a carrier, not a required physical shelf.
                # Keep optional locations submitted by an older form/client.
                g.stock_location = clean_location(values.get("storage_location", ""), required=False)
                g.stock_location_number = str(values.get("part_number") or "").strip().upper()
                g.stock_location_order_id = (request.view_args or {}).get("order_id")
            elif request.endpoint in singles:
                g.stock_location = clean_location(values.get("storage_location", values.get("storageLocation", "")), required=False)
                g.stock_location_number = str(values.get("part_number") or values.get("partNumber") or "").strip().upper()
                g.stock_location_warehouse = values.get("warehouse_id", values.get("warehouseId"))
                g.stock_location_part_id = (request.view_args or {}).get("part_id") or values.get("partId")
                if not g.stock_location_number:
                    with site.SessionLocal() as db:
                        record = None
                        if g.stock_location_part_id:
                            record = db.get(site.Part, g.stock_location_part_id)
                        elif (request.view_args or {}).get("item_id"):
                            record = db.get(site.ReceivingDraftItem, request.view_args["item_id"])
                        elif (request.view_args or {}).get("order_id"):
                            record = db.get(site.TransitOrder, request.view_args["order_id"])
                        elif request.endpoint == "revision":
                            query = db.query(site.Part).filter_by(warehouse_id=request.view_args["warehouse_id"], is_deleted=False).order_by(site.Part.id)
                            idx = request.args.get("idx", 0, type=int) or 0
                            if idx < 0:
                                idx = query.count() + idx
                            record = query.offset(max(idx, 0)).first()
                            g.stock_location_part_id = record.id if record else None
                        if record:
                            g.stock_location_number = record.part_number.upper()
                g.stock_location_order_id = (request.view_args or {}).get("order_id")
                if request.endpoint == "api_mobile_verification_check":
                    with site.SessionLocal() as db:
                        barcode = re.sub(r"\D", "", str(values.get("barcode", "")))
                        part = db.query(site.Part).filter_by(barcode=barcode).first()
                        if part:
                            g.stock_location_part_id = part.id
                            g.stock_location_number = part.part_number.upper()
                with site.SessionLocal() as db:
                    g.stock_location = revision_locations(db).get(number_key(g.stock_location_number), "") or g.stock_location
                g.stock_location = clean_location(g.stock_location)
            elif request.endpoint == "admin_transit_create":
                # A batch can contain several different positions, one per draft row.
                g.stock_location_number = str(values.get("part_number") or "").strip().upper()
                drafts = site.get_transit_draft_items()
                if drafts:
                    g.stock_location_map = {row["partNumber"].upper(): clean_location(row.get("storageLocation", ""), required=False) for row in drafts}
                    if values.get("part_number"):
                        g.stock_location = clean_location(values.get("storage_location", ""), required=False)
                        g.stock_location_map[values["part_number"].strip().upper()] = g.stock_location
                else:
                    g.stock_location = clean_location(values.get("storage_location", ""), required=False)
            elif request.endpoint == "admin_transit_arrive":
                with site.SessionLocal() as db:
                    order = db.get(site.TransitOrder, request.view_args["order_id"])
                    if order:
                        g.stock_location = clean_location(values.get("storage_location") or order.storage_location)
                        g.stock_location_number = order.part_number.upper()
                        g.stock_location_order_id = order.id
                        g.stock_location_part_id = order.linked_part_id
            elif request.endpoint == "api_mobile_transit_scan":
                with site.SessionLocal() as db:
                    barcode = re.sub(r"\D", "", str(values.get("barcode", "")))
                    order = next((row for row in site.mobile_transit_orders(db) if row.barcode == barcode and int(row.accepted_qty or 0) < int(row.qty or 0)), None)
                    if order:
                        g.stock_location = clean_location(values.get("storageLocation") or order.storage_location)
                        g.stock_location_number = order.part_number.upper()
                        g.stock_location_order_id = order.id
            elif request.endpoint == "confirm_import" and values.get("action") != "skip_all":
                with site.SessionLocal() as db:
                    imported = db.get(site.ImportSession, request.view_args["session_id"])
                    if not imported:
                        return None
                    selected = db.query(site.ImportChange).filter_by(import_session_id=imported.id).all()
                    positions = {}
                    for row in selected:
                        payload = json.loads(row.payload_json or "{}")
                        field_name = f"storage_location_{row.id}"
                        location_changed = field_name in values and values[field_name].strip() != payload.get("storage_location", "")
                        if not row.apply_change and not location_changed and not (values.get("action") == "all" and row.change_type != "same"):
                            continue
                        location = revision_locations(db).get(number_key(row.part_number), "") or values.get(field_name, payload.get("storage_location", ""))
                        try:
                            positions[row.part_number.upper()] = clean_location(location)
                        except ValueError:
                            raise ValueError(f"Імпорт: вкажіть місце на складі для товару {row.part_number}. Відкрийте фільтр «Усі» для перевірки рядків.") from None
                    g.stock_location_map = positions
                    g.stock_location_warehouse = imported.warehouse_id
        except ValueError as exc:
            return rejected(str(exc))

    def remember(db, names):
        seen = db.info.setdefault("storage_location_keys_seen", set())
        for name in names:
            values = {"name": name, "name_key": name.casefold()}
            if values["name_key"] in seen:
                continue
            dialect = db.get_bind().dialect.name
            if dialect in {"postgresql", "sqlite"}:
                statement = (pg_insert if dialect == "postgresql" else sqlite_insert)(StorageLocation).values(**values)
                db.execute(statement.on_conflict_do_nothing(index_elements=[StorageLocation.name_key]))
            elif not db.query(StorageLocation).filter_by(name_key=values["name_key"]).first():
                db.add(StorageLocation(**values))
            seen.add(values["name_key"])

    @event.listens_for(Session, "after_commit")
    @event.listens_for(Session, "after_rollback")
    def reset_location_cache(db):
        db.info.pop("storage_location_keys_seen", None)
        db.info.pop("revision_locations", None)
        db.info.pop("revision_location_changes", None)
        db.info.pop("revision_locations_changed", None)

    def apply_revision_locations(db, positions):
        """Materialize only locations; quantities, prices, and item links stay intact."""
        if not positions:
            return
        with db.no_autoflush:
            for model in models:
                field = "location" if model is site.RevisionPart else "storage_location"
                groups = {}
                for identifier, number, previous in db.query(model.id, model.part_number, getattr(model, field)):
                    value = positions.get(number_key(number or ""))
                    if value and previous != value:
                        groups.setdefault(value, []).append(identifier)
                for value, identifiers in groups.items():
                    changes = {field: value}
                    if model is site.RevisionPart:
                        # A stale editor must not put an older shared position back.
                        changes["version"] = model.version + 1
                    for start in range(0, len(identifiers), 500):
                        db.query(model).filter(model.id.in_(identifiers[start:start+500])).update(
                            changes, synchronize_session="fetch")
        db.info.pop("revision_locations", None)

    @event.listens_for(Session, "after_flush_postexec")
    def share_revision_positions(db, *_):
        changed = db.info.pop("revision_location_changes", {})
        if db.info.pop("revision_locations_changed", False):
            db.info.pop("revision_locations", None)
        apply_revision_locations(db, {key: clean_location(row.location) for key, row in changed.items()
                                      if (row.location or "").strip()})

    @event.listens_for(Session, "before_flush")
    def persist_manual_position(db, *_):
        revision_changes = [row for row in list(db.new) + list(db.dirty) if isinstance(row, site.RevisionPart)
                            and (row in db.new or any(inspect(row).attrs[field].history.has_changes()
                                                     for field in ("location", "part_number")))]
        if revision_changes or any(isinstance(row, site.RevisionPart) for row in db.deleted):
            db.info.pop("revision_locations", None)
            db.info["revision_locations_changed"] = True
        for row in revision_changes:
            if (row.location or "").strip():
                db.info.setdefault("revision_location_changes", {})[number_key(row.part_number or "")] = row
        names = set()
        current = request_location()
        mapping = getattr(g, "stock_location_map", {}) if has_request_context() else {}
        warehouse_id = getattr(g, "stock_location_warehouse", None) if has_request_context() else None
        part_id = getattr(g, "stock_location_part_id", None) if has_request_context() else None
        target_number = getattr(g, "stock_location_number", "") if has_request_context() else ""
        order_id = getattr(g, "stock_location_order_id", None) if has_request_context() else None
        for row in list(db.new) + list(db.dirty):
            if not isinstance(row, models):
                continue
            field = "location" if isinstance(row, site.RevisionPart) else "storage_location"
            number = (row.part_number or "").upper()
            value = mapping.get(number, "") if mapping else current if number == target_number else ""
            if row in db.new and getattr(row, field, None):
                value = getattr(row, field)
            if value and not isinstance(row, site.RevisionPart):
                # Editing one warehouse row must not move other warehouse copies.
                applies = not isinstance(row, site.Part) or row in db.new
                if isinstance(row, site.TransitOrder) and order_id is not None and row not in db.new:
                    applies = row.id == order_id
                if isinstance(row, site.Part) and row not in db.new:
                    if part_id is not None:
                        applies = str(row.id) == str(part_id)
                    elif warehouse_id is not None:
                        applies = str(row.warehouse_id) == str(warehouse_id)
                    elif request.endpoint == "revision":
                        applies = True
                if applies:
                    setattr(row, field, value)
            position = clean_location(getattr(row, field, "") or "", required=False)
            if not isinstance(row, site.RevisionPart):
                position = revision_locations(db).get(number_key(row.part_number or ""), "") or position
            if position:
                setattr(row, field, position)
                if row in db.new or inspect(row).attrs[field].history.has_changes():
                    names.add(position)
        if names:
            remember(db, names)

    @site.app.route("/admin/api/storage-locations")
    @site.app.route("/api/mobile/storage-locations")
    def storage_location_suggestions():
        if request.path.startswith("/admin/") and not session.get("admin_auth"):
            return redirect(url_for("admin_login"))
        query = clean_location(request.args.get("q", "")[:255], required=False)
        if len(query) < 3:
            return jsonify(items=[])
        escaped = query.casefold().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        with site.SessionLocal() as db:
            names = db.query(StorageLocation.name).filter(StorageLocation.name_key.like(f"%{escaped}%", escape="\\")).order_by(StorageLocation.name_key).limit(20).all()
            return jsonify(items=[name for (name,) in names])

    def prepare_import_rows(db, rows, default=""):
        default = clean_location(default or "", required=False)
        for row in rows:
            value = revision_locations(db).get(number_key(row.get("part_number") or ""), "") or row.get("storage_location") or default
            if not value:
                part = db.query(site.Part).filter_by(part_number=row["part_number"], warehouse_id=int(request.form["warehouse_id"])).first()
                value = part.storage_location if part else ""
            row["storage_location"] = clean_location(value, required=False)
        return rows

    site.prepare_import_locations = prepare_import_rows
    def index_existing():
        with site.SessionLocal() as db:
            apply_revision_locations(db, revision_locations(db))
            names = set()
            for model in models:
                field = model.location if model is site.RevisionPart else model.storage_location
                for (value,) in db.query(field).filter(field != "").distinct():
                    name = clean_location(value, required=False)
                    if name:
                        names.add(name)
            remember(db, names)
            db.commit()
    site.index_existing_storage_locations = index_existing
    site.revision_locations = revision_locations
    return StorageLocation


def location_payload(record=None, *, db=None, part_number=""):
    value = effective_location(record, db=db, part_number=part_number)
    return {"storageLocation": value, "storageLocationLabel": "Місце на складі: " + value if value else "",
            "storageLocationHighlight": bool(value)}


def location_line(record=None, *, db=None, part_number=""):
    value = location_payload(record, db=db, part_number=part_number)["storageLocation"]
    return "📍 Місце на складі: " + value if value else ""


def location_entities(message):
    result, offset = [], 0
    for line in message.splitlines(keepends=True):
        length = len(line.rstrip("\r\n").encode("utf-16-le")) // 2
        if line.startswith("📍 Місце на складі:"):
            result.append({"type": "bold", "offset": offset, "length": length})
        offset += len(line.encode("utf-16-le")) // 2
    return result
