"""Import the seller's opened Avto.pro order into the existing order board."""

import hashlib
import json
import re
import secrets
import threading
from collections import Counter
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from io import BytesIO
from pathlib import Path
from urllib.parse import urlsplit
from zipfile import ZIP_DEFLATED, ZipFile

from flask import abort, flash, jsonify, redirect, render_template, request, send_file, session, url_for
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Text, func, text

from .avtopro_status import is_new_sale


SOURCE = "autopro"
TOKEN_KEY = "avtopro_browser_token_hash"
SERVER_TOKEN_KEY = "avtopro_daemon_token_hash"
SESSION_KEY = "avtopro_browser_session"
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


def completion_notice(value, external_id):
    prefix = rf"(?:(?:замовлення|заказ)\s+(?:№?\s*{re.escape(external_id)}\s+)?)?"
    return bool(re.fullmatch(prefix + r"(?:завершен[оё]?|виконано|выполн[её]н[о]?)[.!]?", value.casefold()))


def validate_capture(raw, *, allow_status_update=False):
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
    waybill = string_field(raw, "waybill", 14)
    if waybill:
        if not re.fullmatch(r"[0-9]{13,14}", waybill):
            raise ValueError("Некоректний номер накладної Автопро")
        reference["waybill"] = waybill
    if raw.get("statusOnly") is True:
        if not allow_status_update and not cancellation_notice(status, order_id):
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
    matches = (db.query(site.Part).filter(site.Part.is_deleted.is_(False),
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

    def dollars(uah):
        return (Decimal(uah) / Decimal(str(site.DEFAULT_UAH_RATE))).quantize(CENT, rounding=ROUND_HALF_UP)

    def apply_capture_fields(db, order, payload, *, replace_items):
        order.customer_name = payload["customer"] or f"Автопро №{payload['orderId']}"
        order.phone = payload["phone"]
        order.delivery_type = delivery_mode(payload["delivery"])
        order.total_usd = dollars(payload["totalUah"])
        order.external_status = payload["status"]
        order.status = initial_status(payload["status"], payload["orderId"])
        order.updated_at = site.now()
        if replace_items:
            order.items[:] = [site.OrderItem(part_id=inventory_match(site, db, item),
                part_number=item["partNumber"], name=item["name"], qty=item["qty"],
                price_usd=dollars(item["unitUah"])) for item in payload["items"]]

    def can_refresh_draft(db, order, snapshot, previous, payload):
        if (not snapshot or snapshot.imported_payload_hash != snapshot.payload_hash
                or order.status != "new" or order.is_processing or order.stock_reserved
                or order.ttn or order.prepayment_usd or order.cancel_reason
                or site.latest_packing_request_for_order(db, order.id)):
            return False
        if (order.customer_name != (previous["customer"] or f"Автопро №{order.external_order_id}")
                or order.phone != previous["phone"] or order.delivery_type != delivery_mode(previous["delivery"])
                or order.total_usd != dollars(previous["totalUah"])):
            return False
        if any(previous[field] and not payload[field] for field in ("customer", "phone", "delivery")):
            return False
        current = Counter((item.part_number, item.name, item.qty, item.price_usd) for item in order.items)
        original = Counter((item["partNumber"], item["name"], item["qty"], dollars(item["unitUah"]))
                           for item in previous["items"])
        if current != original:
            return False
        if payload["items"] == previous["items"]:
            return True
        # Only replace lines whose warehouse associations still match the automatic mapping.
        current_parts = Counter((item.part_number, item.name, item.qty, item.price_usd, item.part_id)
                                for item in order.items)
        matched_parts = Counter((item["partNumber"], item["name"], item["qty"], dollars(item["unitUah"]),
                                 inventory_match(site, db, item)) for item in previous["items"])
        return current_parts == matched_parts

    def waybill_conflict(order, payload):
        return bool(payload.get("waybill") and order.ttn
                    and payload["waybill"] != site.order_tracking.number(order.ttn))

    def apply_tracking(db, order, payload, *, update_status):
        if order.status in {"done", "cancelled"}:
            return False
        tracking = site.order_tracking
        waybill = payload.get("waybill", "")
        conflict = waybill_conflict(order, payload)
        changed = False
        if (not order.ttn and tracking.valid_number(waybill)
                and order.delivery_type == delivery_mode(payload["delivery"]) == "nova_poshta"):
            changed = tracking.set_waybill(order, waybill)
            tracking.mark_shipped(db, order, site.now())
        if update_status and order.external_status != payload["status"]:
            order.external_status = payload["status"]
            changed = True
        if (update_status and not conflict and not (payload.get("stockIssue") and not order.stock_reserved)
                and completion_notice(payload["status"], order.external_order_id)):
            pack = site.latest_packing_request_for_order(db, order.id)
            if order.status in {"new", "shipped"} and (not pack or pack.status in {"shipped", "applied"}):
                order.status = "done"
                order.is_processing = False
                changed = True
        if changed:
            order.updated_at = site.now()
        return changed

    def import_capture(db, payload, *, existing_only=False, sync_draft=False):
        external_id = payload["orderId"]
        if db.get_bind().dialect.name == "postgresql":
            lock_id = int.from_bytes(hashlib.sha256(f"avtopro:{external_id}".encode()).digest()[:8], "big", signed=True)
            db.execute(text("SELECT pg_advisory_xact_lock(:id)"), {"id": lock_id})
        snapshot = db.get(AvtoproOrderCapture, external_id)
        matches = match_existing(db, external_id)
        if len(matches) > 1:
            raise ValueError("У кабінеті вже є кілька карток із цим номером. Потрібна ручна звірка.")
        order = matches[0] if matches else None
        if existing_only and order is None:
            raise ValueError("Замовлення відсутнє в USAparts.top")
        if sync_draft and order is None and not is_new_sale(payload["status"]):
            raise ValueError("Автоімпорт створює лише нові продажі")
        if order is None and snapshot:
            raise ValueError("Це замовлення вже імпортувалося. Перевірте його історію в кабінеті.")
        previous = json.loads(snapshot.payload_json) if snapshot else None
        cancelled = cancellation_notice(payload["status"], external_id)
        if order and previous and cancellation_notice(previous["status"], external_id) and not cancelled:
            return "unchanged", order
        status_only = "items" not in payload
        if status_only:
            if not order or not previous:
                raise ValueError("Замовлення ще не імпортоване. Передайте повну картку Автопро.")
            payload = {**previous, **payload}
        if previous and previous.get("stockIssue"):
            payload["stockIssue"] = previous["stockIssue"]
        source_was_shipped = bool(previous and (previous.get("sourceWasShipped") or
            initial_status(previous["status"], external_id) in {"shipped", "done"}))
        if source_was_shipped or payload.get("waybill") or initial_status(payload["status"], external_id) in {"shipped", "done"}:
            payload["sourceWasShipped"] = True
        body = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        fingerprint = sha(body)
        duplicate = bool(order and snapshot and snapshot.order_id == order.id and snapshot.payload_hash == fingerprint)

        created = order is None
        old_empty_draft = bool(order and order.external_status == "email_review" and order.status == "new"
                               and not order.items and not order.stock_reserved
                               and not site.latest_packing_request_for_order(db, order.id))
        refreshed = created or old_empty_draft
        if refreshed:
            if created:
                order = site.Order(external_source=SOURCE, external_order_id=external_id)
                db.add(order)
            apply_capture_fields(db, order, payload, replace_items=True)
            if order.status == "cancelled":
                order.cancel_reason = f"Автопро: {payload['status']}"
            if old_empty_draft:
                order.comment = ""
            db.flush()
        elif sync_draft and not status_only and not cancelled and not duplicate and can_refresh_draft(db, order, snapshot, previous, payload):
            apply_capture_fields(db, order, payload, replace_items=payload["items"] != previous["items"])
            refreshed = True
        stock_changed = False
        issue_before = payload.get("stockIssue", "")
        new_inventory_sale = sync_draft and created
        may_retry_stock = (bool(issue_before) and sync_draft and not cancelled and not source_was_shipped
                           and not payload.get("sourceWasShipped") and order.status in site.order_tracking.UNSHIPPED
                           and not order.ttn and not site.latest_packing_request_for_order(db, order.id))
        if not order.stock_reserved and (new_inventory_sale or may_retry_stock):
            try:
                with db.begin_nested():
                    for item, source in zip(order.items, payload["items"]):
                        if (not item.part_id and item.part_number == source["partNumber"]
                                and item.name == source["name"] and item.qty == source["qty"]):
                            item.part_id = inventory_match(site, db, source)
                    db.flush()
                    stock_changed = bool(site.reserve_order_inventory(db, order))
            except ValueError as exc:
                reason = str(exc)
                payload["stockIssue"] = ("Недостатній залишок" if reason.startswith("not_enough:")
                                         else "Потрібне однозначне зіставлення позицій зі складом")
                if not issue_before:
                    site.flash_news(db, "orders", "Автопро потребує звірки залишків",
                                    f"Замовлення #{order.id}. {payload['stockIssue']}.", severity="warning")
        if order.stock_reserved:
            payload.pop("stockIssue", None)
        issue_changed = issue_before != payload.get("stockIssue", "")
        if issue_changed:
            body = json.dumps(payload, ensure_ascii=False, sort_keys=True)
            fingerprint = sha(body)
        tracking_changed = False
        if not cancelled:
            tracking_changed = apply_tracking(db, order, payload, update_status=sync_draft)
            if (status_only and snapshot and snapshot.imported_payload_hash == snapshot.payload_hash
                    and not waybill_conflict(order, payload)
                    and (not completion_notice(payload["status"], external_id) or order.status == "done")):
                refreshed = True
        result = "created" if created else "unchanged" if duplicate and not (tracking_changed or issue_changed) else "updated"
        if cancelled and not created:
            status_result, stock_changed = apply_cancellation(db, order, payload["status"],
                                                              source_was_shipped or bool(payload.get("waybill")))
            if status_result != "unchanged":
                result = status_result
        if duplicate and result != "cancelled" and not (tracking_changed or issue_changed or stock_changed):
            return result, order
        if not snapshot:
            snapshot = AvtoproOrderCapture(external_id=external_id, order_id=order.id, imported_payload_hash="")
            db.add(snapshot)
        elif snapshot.order_id != order.id:
            snapshot.order_id = order.id
        if refreshed or (issue_changed and duplicate and snapshot.imported_payload_hash == snapshot.payload_hash):
            snapshot.imported_payload_hash = fingerprint
        snapshot.payload_json, snapshot.payload_hash = body, fingerprint
        snapshot.captured_at = datetime.utcnow()
        db.commit()
        if stock_changed:
            site.public_cache_clear()
        return result, order

    def valid_key(db, key_name=TOKEN_KEY):
        header = request.headers.get("Authorization", "")
        if not re.fullmatch(r"Bearer [A-Za-z0-9_-]{43}", header):
            return False
        expected = db.query(site.ApiSetting).filter_by(setting_key=key_name).one_or_none()
        return expected is not None and secrets.compare_digest(sha(header[7:]), expected.setting_value)

    @site.app.post("/api/integrations/avtopro/session")
    def report_avtopro_session():
        if not request.is_json or request.content_length is None or request.content_length > 256:
            return jsonify(error="Некоректний формат"), 400
        with site.SessionLocal() as db:
            if not valid_key(db, SERVER_TOKEN_KEY):
                return jsonify(error="Ключ імпорту недійсний"), 401
            payload = request.get_json(silent=True)
            state = payload.get("state") if isinstance(payload, dict) else None
            if state not in ("active", "login_required"):
                return jsonify(error="Некоректний стан сесії"), 400
            with LOCK:
                if db.get_bind().dialect.name == "postgresql":
                    db.execute(text("SELECT pg_advisory_xact_lock(726576660015)"))
                setting = db.query(site.ApiSetting).filter_by(setting_key=SESSION_KEY).one_or_none()
                if not setting or setting.setting_value != state:
                    site.set_setting(db, SESSION_KEY, state)
                    if state == "login_required":
                        site.flash_news(db, "orders", "Автопро потребує входу",
                                        "Синхронізацію призупинено. Увійдіть через захищену консоль браузера.",
                                        severity="warning")
                    db.commit()
            return jsonify(state=state)

    @site.app.get("/api/integrations/avtopro/orders")
    def list_avtopro_orders():
        cursor = request.args.get("after", "")
        seen = request.args.get("seen")
        with site.SessionLocal() as db:
            if not valid_key(db, SERVER_TOKEN_KEY):
                return jsonify(error="Ключ імпорту недійсний"), 401
            if cursor and not re.fullmatch(r"[0-9]{3,20}", cursor):
                return jsonify(error="Некоректний номер замовлення"), 400
            rows = (db.query(site.Order.external_order_id).filter(
                site.Order.external_source == SOURCE,
                site.Order.status.notin_(("done", "cancelled")),
                site.Order.external_order_id > cursor,
            ).distinct().order_by(site.Order.external_order_id).limit(100))
            ids = [ident for ident, in rows if re.fullmatch(r"[0-9]{3,20}", ident)]
            result = {"ids": ids}
            if seen is not None:
                candidates = seen.split(",") if seen else []
                if len(candidates) > 100 or any(not re.fullmatch(r"[0-9]{3,20}", i) for i in candidates):
                    return jsonify(error="Некоректні номери замовлень"), 400
                known = db.query(site.Order.external_order_id).filter(
                    site.Order.external_source == SOURCE, site.Order.external_order_id.in_(candidates)
                ).union(db.query(AvtoproOrderCapture.external_id).filter(AvtoproOrderCapture.external_id.in_(candidates)))
                result["known"] = [ident for ident, in known]
            return jsonify(result), 200, {"Cache-Control": "no-store"}

    @site.app.post("/api/integrations/avtopro/order")
    def receive_avtopro_order():
        if not request.is_json or request.content_length is None or request.content_length > MAX_BODY:
            return jsonify(error="Некоректний розмір або формат картки"), 400
        sync_draft = request.args.get("sync_draft") == "1"
        with site.SessionLocal() as db:
            if not valid_key(db, SERVER_TOKEN_KEY if sync_draft else TOKEN_KEY):
                return jsonify(error="Ключ імпорту недійсний. Перевірте ключ підключення."), 401
            try:
                existing_only = request.args.get("existing_only") == "1"
                payload = validate_capture(request.get_json(silent=True), allow_status_update=existing_only and sync_draft)
                with LOCK:
                    status, order = import_capture(db, payload, existing_only=existing_only, sync_draft=sync_draft)
            except ValueError as exc:
                return jsonify(error=str(exc)), 422
            return jsonify(result=status, message=FORM_MESSAGES[status], orderId=order.id,
                           orderUrl=site.absolute_public_url(order_location(order))), 201 if status == "created" else 200

    @site.app.route("/admin/marketplaces/avtopro-orders", methods=["GET", "POST"])
    @site.admin_required
    def admin_avtopro_orders():
        csrf = session.setdefault("avtopro_import_csrf", secrets.token_urlsafe(32))
        token = ""
        server_key = False
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
                    server_key = request.form.get("action") == "server_key"
                    site.set_setting(db, SERVER_TOKEN_KEY if server_key else TOKEN_KEY, sha(token))
                    db.commit()
            captures = (db.query(AvtoproOrderCapture, site.Order).outerjoin(site.Order, AvtoproOrderCapture.order_id == site.Order.id)
                        .order_by(AvtoproOrderCapture.captured_at.desc()).limit(50).all())
            records = [{"order_url": order_location(order) if order else None,
                        "changed": capture.payload_hash != capture.imported_payload_hash,
                        "captured_at": capture.captured_at, "data": json.loads(capture.payload_json)} for capture, order in captures]
            response = site.app.make_response(render_template("admin_avtopro_orders.html", csrf=csrf, token=token,
                server_key=server_key, records=records,
                key_set=db.query(site.ApiSetting.id).filter_by(setting_key=TOKEN_KEY).first() is not None,
                server_key_set=db.query(site.ApiSetting.id).filter_by(setting_key=SERVER_TOKEN_KEY).first() is not None))
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
        orders_by_id = {order.id: order for order in orders}
        for row in rows:
            data = json.loads(row.payload_json)
            order = orders_by_id[row.order_id]
            captures[row.order_id] = {"data": data, "changed": row.payload_hash != row.imported_payload_hash,
                                      "stockIssue": data.get("stockIssue", "") if not order.stock_reserved and order.status != "cancelled" else "",
                                      "waybillConflict": waybill_conflict(order, data),
                                      "cancelled": cancellation_notice(data["status"], row.external_id)}
        return captures

    return order_details
