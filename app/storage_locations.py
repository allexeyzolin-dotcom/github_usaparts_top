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
    class StorageLocation(site.Base):
        __tablename__ = "storage_locations"
        id = Column(Integer, primary_key=True)
        name = Column(String(255), nullable=False)
        name_key = Column(String(1024), nullable=False, unique=True, index=True)

    models = (site.Part, site.PartTemplate, site.ReceivingDraftItem, site.TransitOrder, site.RevisionPart)
    singles = {"create_part", "update_part", "admin_part_autosave", "admin_transit_draft_add",
               "admin_transit_update", "admin_receiving_update", "admin_receiving_legacy",
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
            if request.endpoint in singles:
                g.stock_location = clean_location(values.get("storage_location", values.get("storageLocation", "")))
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
            elif request.endpoint == "admin_transit_create":
                # A batch can contain several different positions, one per draft row.
                g.stock_location_number = str(values.get("part_number") or "").strip().upper()
                drafts = site.get_transit_draft_items()
                if drafts:
                    g.stock_location_map = {row["partNumber"].upper(): clean_location(row.get("storageLocation", "")) for row in drafts}
                    if values.get("part_number"):
                        g.stock_location = clean_location(values.get("storage_location", ""))
                        g.stock_location_map[values["part_number"].strip().upper()] = g.stock_location
                else:
                    g.stock_location = clean_location(values.get("storage_location", ""))
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
                        location = values.get(field_name, payload.get("storage_location", ""))
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

    @event.listens_for(Session, "before_flush")
    def persist_manual_position(db, *_):
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
            value = row.get("storage_location") or default
            if not value:
                part = db.query(site.Part).filter_by(part_number=row["part_number"], warehouse_id=int(request.form["warehouse_id"])).first()
                value = part.storage_location if part else ""
            row["storage_location"] = clean_location(value, required=False)
        return rows

    site.prepare_import_locations = prepare_import_rows
    def index_existing():
        with site.SessionLocal() as db:
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
    return StorageLocation
