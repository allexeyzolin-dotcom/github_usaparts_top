"""Import the seller's opened Avto.pro order into the existing order board."""

import hashlib
import json
import re
import secrets
import threading
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from io import BytesIO
from pathlib import Path
from urllib.parse import urlsplit
from zipfile import ZIP_DEFLATED, ZipFile

from flask import abort, flash, jsonify, redirect, render_template, request, send_file, session, url_for
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Text, func, text


SOURCE = "autopro"
TOKEN_KEY = "avtopro_browser_token_hash"
HOSTS = {"avtopro.ua", "www.avtopro.ua", "avto.pro", "www.avto.pro"}
LOCK = threading.Lock()
MAX_BODY = 128 * 1024
CENT = Decimal("0.01")
FORM_MESSAGES = {"created": "Замовлення Автопро додано", "updated": "Дані Автопро оновлено для звірки",
                 "unchanged": "Цю картку Автопро вже передано", "cancelled": "Замовлення Автопро скасовано й перенесено в історію",
                 "return_review": "Автопро скасував замовлення. Звірте відправлення та резерв перед закриттям."}


def sha(value):
    return hashlib.sha256(value.encode()).hexdigest()


def string_field(mapping, name, length, required=False):
    value = mapping.get(name, "")
    if not isinstance(value, str):
        raise ValueError(f"Некоректне поле {name}")
    value = " ".join(value.replace("\0", "").split())
    if len(value) > length or (required and not value):
        raise ValueError(f"Перевірте поле {name}")
    return value


def amount(value):
    if not isinstance(value, str) or not re.fullmatch(r"\d{1,9}(?:\.\d{1,2})?", value):
        raise ValueError("Сума має бути в гривнях із точністю до копійок")
    return f"{Decimal(value):.2f}"


def cancellation_notice(value, external_id):
    prefix = rf"(?:(?:замовлення|заказ)\s+№?\s*{re.escape(external_id)}\s+)?"
    terminal = r"(?:скасовано|отменено|отмен[ёе]н)(?: (?:покупцем|продавцем|системою|автоматично|покупателем|продавцом|системой|автоматически))?"
    return bool(re.fullmatch(prefix + terminal + r"[.!]?", value.casefold()))


def validate_capture(raw):
    if not isinstance(raw, dict):
        raise ValueError("Очікується картка одного замовлення")
    order_id = string_field(raw, "orderId", 20, required=True)
    if not re.fullmatch(r"\d{3,20}", order_id):
        raise ValueError("Некоректний номер замовлення Автопро")
    url = urlsplit(string_field(raw, "sourceUrl", 200, required=True))
    if (url.scheme != "https" or url.netloc.lower() not in HOSTS
            or url.path.rstrip("/") != f"/orders/{order_id}"):
        raise ValueError("Адреса має вести до картки цього замовлення Автопро")
    status = string_field(raw, "status", 200, required=True)
    reference = {"orderId": order_id, "sourceUrl": f"https://{url.netloc.lower()}/orders/{order_id}/", "status": status}
    if raw.get("statusOnly") is True:
        if not cancellation_notice(status, order_id):
            raise ValueError("Потрібен підтверджений статус скасування цього замовлення")
        return reference
    lines = raw.get("items")
    if not isinstance(lines, list) or not 1 <= len(lines) <= 100:
        raise ValueError("Відкрийте повну картку з позиціями замовлення")
    items = []
    for item in lines:
        if not isinstance(item, dict) or type(item.get("qty")) is not int or not 1 <= item["qty"] <= 10000:
            raise ValueError("Перевірте кількість позицій у картці")
        items.append({"partNumber": string_field(item, "partNumber", 255, required=True),
                      "brand": string_field(item, "brand", 100),
                      "name": string_field(item, "name", 500, required=True),
                      "qty": item["qty"], "unitUah": amount(item.get("unitUah"))})
    if raw.get("currency") != "UAH":
        raise ValueError("Підтримуються замовлення з цінами у гривнях")
    phone = string_field(raw, "phone", 64)
    if phone and not re.fullmatch(r"\+?[0-9 ()-]{9,24}", phone):
        raise ValueError("Телефон на картці показано не повністю")
    return {**reference,
            "customer": string_field(raw, "customer", 255), "phone": phone,
            "delivery": string_field(raw, "delivery", 500), "payment": string_field(raw, "payment", 200),
            "totalUah": amount(raw.get("totalUah")), "items": items}


def initial_status(value, external_id):
    normalized = value.casefold()
    if cancellation_notice(value, external_id):
        return "cancelled"
    if re.search(r"завершен|виконан|выполнен", normalized):
        return "done"
    if re.search(r"відправлен|чекає отримання|очікує доставки або отримання|отправлен", normalized):
        return "shipped"
    return "new"


def inventory_match(site, db, item):
    matches = (db.query(site.Part).filter(site.Part.is_deleted.is_(False), site.Part.in_stock.is_(True), site.Part.qty >= item["qty"],
               func.upper(site.Part.part_number) == item["partNumber"].upper()).all())
    if item["brand"]:
        brand = item["brand"].casefold()
        matches = [p for p in matches if (p.brand or p.brand_export or "").strip().casefold() == brand]
    return matches[0].id if len(matches) == 1 else None


def delivery_mode(value):
    lowered = value.casefold()
    if re.search(r"нов[аоюі]\s+пошт[аоюі]", lowered):
        return "nova_poshta"
    if "самовив" in lowered:
        return "pickup"
    return "other"


def install(site):
    class AvtoproOrderCapture(site.Base):
        __tablename__ = "avtopro_order_captures"
        external_id = Column(String(20), primary_key=True)
        order_id = Column(Integer, ForeignKey("orders.id", ondelete="SET NULL"), nullable=True, unique=True)
        payload_json = Column(Text, nullable=False)
        payload_hash = Column(String(64), nullable=False)
        imported_payload_hash = Column(String(64), nullable=False)
        captured_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)

    def match_existing(db, external_id):
        return db.query(site.Order).filter_by(external_source=SOURCE, external_order_id=external_id).with_for_update().limit(2).all()

    def order_location(order):
        group = "history" if site.order_group_for_status(order.status) == "history" else "new"
        return url_for("admin_orders", group=group,
                       selected=order.id, q=order.external_order_id, _anchor=f"order-{order.id}")

    def apply_cancellation(db, order, status, source_was_shipped):
        if order.status == "cancelled":
            return "unchanged", False
        if source_was_shipped or order.status not in site.order_tracking.UNSHIPPED or order.ttn:
            return "return_review", False
        requests = (db.query(site.PackingRequest).filter_by(source_order_id=order.id, source_type="order")
                    .filter(site.PackingRequest.status != "deleted").order_by(site.PackingRequest.id)
                    .with_for_update().all())
        if any(pack.status in {"shipped", "applied"} for pack in requests):
            return "return_review", False
        if order.stock_reserved and (not order.items or any(not item.part_id for item in order.items)):
            return "return_review", False
        returned = site.release_order_inventory(db, order)
        order.status = order.external_status = "cancelled"
        order.cancel_reason = f"Автопро: {status}"
        order.is_processing = False
        order.updated_at = site.now()
        for pack in requests:
            pack.status = "deleted"
            pack.updated_at = order.updated_at
        site.flash_news(db, "orders", "Автопро скасував замовлення",
                        f"Замовлення #{order.id} перенесено в історію. Звільнено резерв: {returned} шт.")
        return "cancelled", bool(returned)

    def import_capture(db, payload):
        external_id = payload["orderId"]
        if db.get_bind().dialect.name == "postgresql":
            lock_id = int.from_bytes(hashlib.sha256(f"avtopro:{external_id}".encode()).digest()[:8], "big", signed=True)
            db.execute(text("SELECT pg_advisory_xact_lock(:id)"), {"id": lock_id})
        snapshot = db.get(AvtoproOrderCapture, external_id)
        matches = match_existing(db, external_id)
        if len(matches) > 1:
            raise ValueError("У кабінеті вже є кілька карток із цим номером. Потрібна ручна звірка.")
        order = matches[0] if matches else None
        if order is None and snapshot:
            raise ValueError("Це замовлення вже імпортувалося. Перевірте його історію в кабінеті.")
        previous = json.loads(snapshot.payload_json) if snapshot else None
        cancelled = cancellation_notice(payload["status"], external_id)
        if order and previous and cancellation_notice(previous["status"], external_id) and not cancelled:
            return "unchanged", order
        if "items" not in payload:
            if not order or not previous:
                raise ValueError("Замовлення ще не імпортоване. Передайте повну картку Автопро.")
            payload = {**previous, **payload}
        source_was_shipped = bool(previous and (previous.get("sourceWasShipped") or
            initial_status(previous["status"], external_id) in {"shipped", "done"}))
        if source_was_shipped:
            payload["sourceWasShipped"] = True
        body = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        fingerprint = sha(body)
        duplicate = bool(order and snapshot and snapshot.order_id == order.id and snapshot.payload_hash == fingerprint)

        created = order is None
        old_empty_draft = bool(order and order.external_status == "email_review" and order.status == "new"
                               and not order.items and not order.stock_reserved
                               and not site.latest_packing_request_for_order(db, order.id))
        if created or old_empty_draft:
            rate = Decimal(str(site.DEFAULT_UAH_RATE))
            def dollars(uah):
                return (Decimal(uah) / rate).quantize(CENT, rounding=ROUND_HALF_UP)

            if created:
                order = site.Order(external_source=SOURCE, external_order_id=external_id)
                db.add(order)
            order.external_status = payload["status"]
            order.status = initial_status(payload["status"], external_id)
            if order.status == "cancelled":
                order.cancel_reason = f"Автопро: {payload['status']}"
            order.customer_name = payload["customer"] or f"Автопро №{external_id}"
            order.phone = payload["phone"]
            order.total_usd = dollars(payload["totalUah"])
            order.delivery_type = delivery_mode(payload["delivery"])
            if old_empty_draft:
                order.comment = ""
            for item in payload["items"]:
                order.items.append(site.OrderItem(part_id=inventory_match(site, db, item),
                    part_number=item["partNumber"], name=item["name"], qty=item["qty"], price_usd=dollars(item["unitUah"])))
            db.flush()
        result = "created" if created else "unchanged" if duplicate else "updated"
        stock_changed = False
        if cancelled and not created:
            status_result, stock_changed = apply_cancellation(db, order, payload["status"], source_was_shipped)
            if status_result != "unchanged":
                result = status_result
        if duplicate and result != "cancelled":
            return result, order
        if not snapshot:
            snapshot = AvtoproOrderCapture(external_id=external_id, order_id=order.id,
                imported_payload_hash=fingerprint if created or old_empty_draft else "")
            db.add(snapshot)
        snapshot.payload_json, snapshot.payload_hash = body, fingerprint
        snapshot.captured_at = datetime.utcnow()
        db.commit()
        if stock_changed:
            site.public_cache_clear()
        return result, order

    @site.app.post("/api/integrations/avtopro/order")
    def receive_avtopro_order():
        header = request.headers.get("Authorization", "")
        if not re.fullmatch(r"Bearer [A-Za-z0-9_-]{43}", header):
            return jsonify(error="Вкажіть ключ із кабінету USAparts.top"), 401
        if not request.is_json or request.content_length is None or request.content_length > MAX_BODY:
            return jsonify(error="Некоректний розмір або формат картки"), 400
        with site.SessionLocal() as db:
            expected = db.query(site.ApiSetting).filter_by(setting_key=TOKEN_KEY).one_or_none()
            if not expected or not secrets.compare_digest(sha(header[7:]), expected.setting_value):
                return jsonify(error="Ключ імпорту недійсний. Перевірте налаштування розширення."), 401
            try:
                payload = validate_capture(request.get_json(silent=True))
                with LOCK:
                    status, order = import_capture(db, payload)
            except ValueError as exc:
                return jsonify(error=str(exc)), 422
            return jsonify(result=status, message=FORM_MESSAGES[status], orderId=order.id,
                           orderUrl=site.absolute_public_url(order_location(order))), 201 if status == "created" else 200

    @site.app.route("/admin/marketplaces/avtopro-orders", methods=["GET", "POST"])
    @site.admin_required
    def admin_avtopro_orders():
        csrf = session.setdefault("avtopro_import_csrf", secrets.token_urlsafe(32))
        token = ""
        with site.SessionLocal() as db:
            if request.method == "POST":
                if not secrets.compare_digest(csrf.encode(), request.form.get("csrf_token", "").encode()):
                    abort(403)
                if request.form.get("action") == "import":
                    value = request.form.get("capture", "")
                    if len(value.encode()) > MAX_BODY:
                        abort(413)
                    try:
                        payload = validate_capture(json.loads(value))
                        with LOCK:
                            result, order = import_capture(db, payload)
                    except json.JSONDecodeError:
                        flash("Вставте коректний JSON картки замовлення.", "error")
                        return redirect(url_for("admin_avtopro_orders"))
                    except ValueError as exc:
                        flash(str(exc), "error")
                        return redirect(url_for("admin_avtopro_orders"))
                    flash(FORM_MESSAGES[result], "warn" if result == "return_review" else "success")
                    return redirect(order_location(order))
                with LOCK:
                    if db.get_bind().dialect.name == "postgresql":
                        db.execute(text("SELECT pg_advisory_xact_lock(726576660014)"))
                    token = secrets.token_urlsafe(32)
                    site.set_setting(db, TOKEN_KEY, sha(token))
                    db.commit()
            captures = (db.query(AvtoproOrderCapture, site.Order).outerjoin(site.Order, AvtoproOrderCapture.order_id == site.Order.id)
                        .order_by(AvtoproOrderCapture.captured_at.desc()).limit(50).all())
            records = [{"order_url": order_location(order) if order else None,
                        "changed": capture.payload_hash != capture.imported_payload_hash,
                        "captured_at": capture.captured_at, "data": json.loads(capture.payload_json)} for capture, order in captures]
            response = site.app.make_response(render_template("admin_avtopro_orders.html", csrf=csrf, token=token,
                records=records, key_set=db.query(site.ApiSetting.id).filter_by(setting_key=TOKEN_KEY).first() is not None))
            response.headers.update({"Cache-Control": "no-store", "Referrer-Policy": "no-referrer"})
            return response

    @site.app.get("/admin/marketplaces/avtopro-mail")
    @site.admin_required
    def legacy_avtopro_mail():
        return redirect(url_for("admin_avtopro_orders"))

    @site.app.get("/admin/marketplaces/avtopro-extension.zip")
    @site.admin_required
    def download_avtopro_extension():
        directory = Path(__file__).with_name("avtopro_extension")
        output = BytesIO()
        with ZipFile(output, "w", ZIP_DEFLATED) as archive:
            for name in ("manifest.json", "popup.html", "popup.js", "popup.css", "capture.js"):
                archive.write(directory / name, f"usaparts-avtopro/{name}")
        output.seek(0)
        return send_file(output, download_name="usaparts-avtopro.zip", mimetype="application/zip", as_attachment=True)

    def order_details(db, orders):
        ids = [order.id for order in orders if order.external_source == SOURCE]
        if not ids:
            return {}
        rows = db.query(AvtoproOrderCapture).filter(AvtoproOrderCapture.order_id.in_(ids))
        captures = {}
        for row in rows:
            data = json.loads(row.payload_json)
            captures[row.order_id] = {"data": data, "changed": row.payload_hash != row.imported_payload_hash,
                                      "cancelled": cancellation_notice(data["status"], row.external_id)}
        return captures

    return order_details
