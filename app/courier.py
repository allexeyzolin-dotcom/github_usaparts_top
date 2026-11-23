"""Private courier queue, device pairing and guarded waybill submission."""
import hashlib
import hmac
import json
import re
import secrets
import time
from datetime import datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from urllib.parse import parse_qsl, urlsplit
from zoneinfo import ZoneInfo

from flask import abort, flash, jsonify, redirect, render_template, request, session, url_for
from itsdangerous import BadData, URLSafeTimedSerializer
from sqlalchemy import Column, DateTime, ForeignKey, Integer, Numeric, String, Text, text

from . import marketplaces, order_tracking
from .telegram_stock import api as telegram_api, CommandError

SITE = MODELS = None
LIFETIME = 60 * 60 * 24 * 30
CANCELLED = {"canceled", "cancelled"}


class CourierError(ValueError):
    pass


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def amount(raw):
    raw = str(raw if raw is not None else "").strip().replace(",", ".")
    if not re.fullmatch(r"[0-9]{1,10}(?:\.[0-9]{1,2})?", raw):
        raise CourierError("Введіть контроль оплати у гривнях. Якщо все оплачено, введіть 0.")
    return Decimal(raw)


def config(db, lock=False):
    query = db.query(MODELS.Config).filter_by(id=1)
    if lock:
        query = query.populate_existing().with_for_update()
    row = query.one_or_none()
    if row is None:
        # Both supported databases implement atomic insert-on-conflict.
        db.execute(text("INSERT INTO courier_config (id) VALUES (1) ON CONFLICT (id) DO NOTHING"))
        row = query.one()
    return row


def check_admin_csrf():
    expected = session.get("marketplace_csrf", "")
    if not expected or not hmac.compare_digest(expected.encode(), request.form.get("csrf_token", "").encode()):
        abort(403)


def is_closed(order):
    return order.status not in order_tracking.UNSHIPPED or order.external_status in CANCELLED or bool(order.ttn)


def handoff(db, order, payment, packing=None):
    if is_closed(order) or SITE.effective_packing_delivery_type(packing, order) != "nova_poshta":
        raise CourierError("Передати можна активне відправлення Нової пошти без ТТН.")
    if db.get(MODELS.Dispatch, order.id):
        raise CourierError("Замовлення вже передано кур’єру.")
    if packing and (packing.status == "issue" or any(i.status in {"missing", "partial"} for i in packing.items)):
        raise CourierError("Спочатку звірте розбіжності під час збирання.")
    if order.external_source == "autopro":
        capture = SITE.avtopro_order_details(db, [order]).get(order.id, {})
        if any(capture.get(key) for key in ("cancelled", "stockIssue", "waybillConflict")):
            raise CourierError("Звірте стан замовлення Автопро перед передачею.")
    if order.external_source == "prom" and not order.stock_reserved:
        raise CourierError("Спочатку підтвердьте резерв товарів у замовленні Prom.")
    if not order.items or any(not item.part_id or int(item.qty or 0) < 1 for item in order.items):
        raise CourierError("Зіставте кожну позицію зі складом перед передачею кур’єру.")
    if not order.stock_reserved:
        try:
            SITE.reserve_order_inventory(db, order)
        except ValueError as exc:
            raise CourierError(SITE.inventory_reserve_error_message(str(exc))) from None
    if packing and packing.delivery_type == "nova_poshta":
        base = SITE.order_np_payload(order)
        for key, value in SITE.packing_request_np_payload(packing).items():
            if value:
                base[key] = value
        SITE.apply_np_payload_to_order(order, "nova_poshta", base)
    order.prepayment_usd = payment
    order.status = "awaiting_shipment"
    order.is_processing = True
    order.updated_at = SITE.now()
    if packing:
        packing.status = "awaiting_shipment"
        packing.control_payment_uah = payment
        packing.updated_at = order.updated_at
    db.add(MODELS.Dispatch(order_id=order.id, payment=payment, created_at=order.updated_at))
    SITE.flash_news(db, "orders", "Передано на доставку", f"Замовлення #{order.id} передано кур’єру.", "success")


def item_card(db, item):
    part = db.get(SITE.Part, item.part_id) if item.part_id else None
    gallery = SITE.part_gallery_urls(part)
    photo = gallery[0] if gallery else ""
    if photo and not (photo.startswith("/") and not photo.startswith("//") or urlsplit(photo).scheme in ("https", "http")):
        photo = ""
    return {"oem": (part.part_number if part else "") or item.part_number,
            "name": item.name or (part.name if part else ""),
            "description": (part.description or "") if part else "",
            "qty": item.qty, "photo": photo,
            "warehouse": part.warehouse.name if part and part.warehouse else "",
            "location": (part.storage_location or "") if part else ""}


def card(db, order, dispatch):
    destination = SITE.order_np_payload(order)
    target = (", ".join(filter(None, (destination["street_name"], destination["house"])))
              if destination["service_type"] == "address" else destination["warehouse_label"])
    city = destination["city_name"]
    data = {"id": order.id, "name": order.customer_name, "phone": order.phone,
            "address": " · ".join(dict.fromkeys(filter(None, (city, target)))),
            "service": {"address": "Адресна доставка", "postomat": "Поштомат"}.get(destination["service_type"], "Відділення"),
            "payment": format(dispatch.payment, ".2f"), "ttn": order.ttn or "",
            "items": [item_card(db, i) for i in sorted(order.items, key=lambda i: i.id)]}
    data["version"] = digest(json.dumps(data, sort_keys=True))
    return data


def queue(db, archived=False):
    O, D = SITE.Order, MODELS.Dispatch
    rows = db.query(O, D).join(D, D.order_id == O.id)
    if archived:
        rows = rows.filter(O.ttn != "")
    else:
        rows = rows.filter(O.ttn == "", O.status.in_(order_tracking.UNSHIPPED), ~O.external_status.in_(CANCELLED))
    return rows.order_by(D.created_at.desc(), O.id.desc())


def submit(db, order_id, value, version):
    O = SITE.Order
    order = db.query(O).filter_by(id=order_id).with_for_update().one_or_none()
    dispatch = db.get(MODELS.Dispatch, order_id)
    if not order or not dispatch:
        abort(404)
    number = order_tracking.number(value)
    if not order_tracking.valid_number(number):
        raise CourierError("ТТН Нової пошти має містити 14 цифр.")
    if order.ttn == number:
        return
    if is_closed(order) or SITE.infer_order_delivery_type(order) != "nova_poshta":
        raise CourierError("Відправлення вже закрите, змінене або має іншу ТТН. Оновіть картку.")
    if version != card(db, order, dispatch)["version"]:
        raise CourierError("Дані замовлення змінилися. Відкрийте картку знову та звірте дані.")
    if db.bind.dialect.name == "postgresql":
        db.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": int(number)})
    if db.query(O.id).filter(O.id != order_id, O.ttn == number).first():
        raise CourierError("Цю ТТН уже збережено для іншого замовлення.")
    order_tracking.set_waybill(order, number)
    order_tracking.mark_shipped(db, order, SITE.now())
    marketplaces.queue_waybill(db, order, "")
    SITE.queue_telegram_message(db, SITE.build_order_ttn_telegram_message(order, number))
    SITE.flash_news(db, "orders", "Кур’єр додав ТТН", f"Замовлення #{order.id} відправлено.", "success")


def signer():
    return URLSafeTimedSerializer(SITE.app.secret_key, salt="courier-session-v1")


def auth(db):
    raw = request.cookies.get("courier_access", "")
    try:
        channel, epoch, csrf = signer().loads(raw, max_age=LIFETIME)
    except (BadData, ValueError, TypeError):
        abort(401)
    cfg = config(db)
    if channel not in {"web", "tg"} or not epoch or getattr(cfg, channel + "_epoch") != epoch:
        abort(401)
    if request.method == "POST" and not hmac.compare_digest(csrf.encode(), request.headers.get("X-Courier-CSRF", "").encode()):
        abort(403)
    return csrf


def signed_in(cfg, channel):
    epoch = getattr(cfg, channel + "_epoch")
    if not epoch:
        epoch = secrets.token_hex(20)
        setattr(cfg, channel + "_epoch", epoch)
    csrf = secrets.token_urlsafe(24)
    res = jsonify(ok=True)
    res.set_cookie("courier_access", signer().dumps([channel, epoch, csrf]), max_age=LIFETIME,
                   secure=True, httponly=True, samesite="Lax", path="/courier")
    return res


def pair(cfg, code, channel):
    if (channel not in ("web", "tg") or cfg.invite_channel != channel or not cfg.invite_until
            or cfg.invite_until < datetime.utcnow() or not code or len(code) > 128
            or not hmac.compare_digest(cfg.invite_hash or "", digest(code))):
        return False
    cfg.invite_hash = cfg.invite_until = None
    setattr(cfg, channel + "_epoch", secrets.token_hex(20))
    return True


def telegram_user(init, token):
    if not isinstance(init, str) or len(init) > 10000:
        return ""
    try:
        params = dict(parse_qsl(init, strict_parsing=True))
        signature = params.pop("hash")
        stamp = int(params["auth_date"])
        key = hmac.digest(b"WebAppData", token.encode(), "sha256")
        data = "\n".join(f"{k}={v}" for k, v in sorted(params.items()))
        if not hmac.compare_digest(hmac.digest(key, data.encode(), "sha256").hex(), signature):
            return ""
        if not -30 <= time.time() - stamp <= 600:
            return ""
        return str(json.loads(params["user"])["id"])
    except (KeyError, ValueError, TypeError, UnicodeError):
        return ""


def bot_token(cfg):
    return marketplaces.cipher().decrypt(cfg.bot_token.encode()).decode() if cfg.bot_token else ""


def verify_bot_setup(db):
    """Verify the new credential before replacing any connected bot."""
    now = datetime.utcnow()
    # Setup submissions and the worker acquire locks in the same order.
    cfg = config(db, True)
    pending = db.query(MODELS.BotSetup).filter(MODELS.BotSetup.next_try <= now).with_for_update(skip_locked=True).first()
    if not pending:
        return
    token = marketplaces.cipher().decrypt(pending.credential.encode()).decode()
    step = "Перевірка бота"
    try:
        me = telegram_api(token, "getMe", timeout=(8, 65))
        step = "Перевірка webhook"
        if telegram_api(token, "getWebhookInfo", timeout=(8, 30)).get("url"):
            raise CourierError("Цей бот уже використовує webhook іншого сервісу.")
        if me.get("first_name") != "Доставка товару":
            step = "Назва бота"
            telegram_api(token, "setMyName", {"name": "Доставка товару"})
    except (CommandError, CourierError) as exc:
        pending.error = f"{step}. {exc}"
        if isinstance(exc, CourierError) or getattr(exc, "status", 0) in {400, 401, 403, 404}:
            pending.next_try = None
        else:
            pending.attempts += 1
            retry_after = int(getattr(exc, "retry_after", 0) or 0)
            retry = retry_after if retry_after > 0 else 30 * 2 ** min(pending.attempts, 5)
            pending.next_try = datetime.utcnow() + timedelta(seconds=retry + 2)
        db.commit()
        return
    if cfg.bot_username != me["username"]:
        cfg.chat_id = cfg.invite_hash = cfg.tg_epoch = cfg.bot_offset = None
    cfg.bot_username, cfg.bot_token, cfg.bot_error = me["username"], pending.credential, ""
    db.delete(pending)
    db.commit()


def install(site):
    global SITE, MODELS
    SITE = site

    class Config(site.Base):
        __tablename__ = "courier_config"
        id = Column(Integer, primary_key=True)
        invite_hash = Column(String(64))
        invite_channel = Column(String(8))
        invite_until = Column(DateTime)
        web_epoch = Column(String(64))
        tg_epoch = Column(String(64))
        bot_token = Column(Text)
        bot_username = Column(String(64))
        chat_id = Column(String(32))
        bot_offset = Column(Integer)
        bot_error = Column(String(255))

    class Dispatch(site.Base):
        __tablename__ = "courier_dispatches"
        order_id = Column(Integer, ForeignKey("orders.id", ondelete="CASCADE"), primary_key=True)
        payment = Column(Numeric(12, 2), nullable=False)
        created_at = Column(DateTime, nullable=False, index=True)
        notified_chat = Column(String(32))
        notice_id = Column(Integer)
        notice_version = Column(String(64))

    class BotSetup(site.Base):
        __tablename__ = "courier_bot_setup"
        id = Column(Integer, primary_key=True)
        credential = Column(Text, nullable=False)
        next_try = Column(DateTime)
        attempts = Column(Integer, nullable=False, default=0)
        error = Column(String(255), nullable=False, default="")

    MODELS = SimpleNamespace(Config=Config, Dispatch=Dispatch, BotSetup=BotSetup)

    @site.app.after_request
    def courier_headers(res):
        if request.path.startswith(("/courier", "/admin/courier")):
            res.headers.update({"Cache-Control": "no-store", "Referrer-Policy": "no-referrer",
                                "X-Robots-Tag": "noindex, nofollow"})
        return res

    @site.app.route("/admin/courier", methods=["GET", "POST"])
    @site.admin_required
    def admin_courier():
        with site.SessionLocal() as db:
            action = request.form.get("action") if request.method == "POST" else None
            cfg = config(db, action in ("web", "tg", "revoke", "bot"))
            code = link = ""
            if request.method == "POST":
                check_admin_csrf()
                if action in ("web", "tg"):
                    if action == "tg" and not cfg.bot_username:
                        abort(400)
                    code = secrets.token_urlsafe(18)
                    cfg.invite_hash, cfg.invite_channel = digest(code), action
                    cfg.invite_until = datetime.utcnow() + timedelta(minutes=15)
                    link = (f"https://t.me/{cfg.bot_username}?start={code}" if action == "tg"
                            else site.public_site_base_url() + "/courier/#invite=" + code)
                elif action == "revoke":
                    cfg.web_epoch = cfg.tg_epoch = cfg.chat_id = cfg.invite_hash = None
                    flash("Доступ кур’єра відкликано.", "success")
                elif action == "bot":
                    token = request.form.get("token", "").strip()
                    try:
                        if not re.fullmatch(r"[0-9]{5,20}:[A-Za-z0-9_-]{30,100}", token):
                            raise CourierError("Перевірте токен BotFather.")
                        if token == site.telegram_settings(db)["bot_token"]:
                            raise CourierError("Вкажіть окремого бота для кур’єра.")
                        pending = db.query(BotSetup).filter_by(id=1).with_for_update().one_or_none()
                        same_pending = pending and pending.next_try is not None and hmac.compare_digest(
                            marketplaces.cipher().decrypt(pending.credential.encode()), token.encode())
                        if same_pending:
                            flash("Цей токен уже очікує перевірки. Час повторної спроби збережено.", "success")
                        else:
                            db.execute(text("""
                                INSERT INTO courier_bot_setup (id, credential, next_try, attempts, error)
                                VALUES (1, :credential, :next_try, 0, '')
                                ON CONFLICT (id) DO UPDATE SET credential = excluded.credential,
                                    next_try = excluded.next_try, attempts = 0, error = ''
                            """), {"credential": marketplaces.cipher().encrypt(token.encode()).decode(),
                                   "next_try": datetime.utcnow()})
                            flash("Токен збережено. Сервер перевіряє підключення до Telegram.", "success")
                    except CourierError as exc:
                        flash(str(exc), "error")
                    db.commit()
                    return redirect(url_for("admin_courier"))
                else:
                    abort(400)
            db.commit()
            pending = db.get(BotSetup, 1)
            retry_at = (pending.next_try.replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo("Europe/Kyiv"))
                        .strftime("%d.%m.%Y %H:%M:%S") if pending and pending.next_try else "")
            return render_template("admin_courier.html", cfg=cfg, pending=pending, retry_at=retry_at, code=code, link=link,
                csrf=session.setdefault("marketplace_csrf", secrets.token_urlsafe(32)))

    @site.app.post("/admin/orders/<int:order_id>/courier")
    @site.admin_required
    def admin_courier_handoff(order_id):
        check_admin_csrf()
        with site.SessionLocal() as db:
            order = db.query(site.Order).filter_by(id=order_id).with_for_update().one_or_none()
            if not order:
                abort(404)
            try:
                handoff(db, order, amount(request.form.get("control_payment_uah")), site.latest_packing_request_for_order(db, order_id))
                db.commit()
                flash("Замовлення передано кур’єру.", "success")
            except CourierError as exc:
                db.rollback()
                flash(str(exc), "error")
        return redirect(url_for("admin_orders", selected=order_id))

    @site.app.get("/courier/")
    def courier_home():
        return render_template("courier.html")

    @site.app.post("/courier/login")
    def courier_login():
        data = request.get_json(silent=True) or {}
        if not isinstance(data, dict):
            abort(400)
        with site.SessionLocal() as db:
            cfg = config(db, True)
            if data.get("telegram") and cfg.chat_id and cfg.bot_token:
                if telegram_user(data["telegram"], bot_token(cfg)) != cfg.chat_id:
                    abort(403)
                channel = "tg"
            elif isinstance(data.get("code"), str) and pair(cfg, data["code"].strip(), "web"):
                channel = "web"
            else:
                abort(403)
            res = signed_in(cfg, channel)
            db.commit()
            return res

    @site.app.get("/courier/list")
    def courier_list():
        archived = request.args.get("tab") == "archive"
        page = max(1, min(request.args.get("page", 1, type=int), 100000))
        with site.SessionLocal() as db:
            csrf = auth(db)
            rows = queue(db, archived).offset((page-1)*25).limit(26).all()
            return jsonify(csrf=csrf, more=len(rows)>25, rows=[{"id": o.id, "name": o.customer_name,
                "city": o.city, "qty": sum(i.qty for i in o.items), "ttn": o.ttn} for o, _ in rows[:25]])

    @site.app.route("/courier/orders/<int:order_id>", methods=["GET", "POST"])
    def courier_detail(order_id):
        with site.SessionLocal() as db:
            csrf = auth(db)
            if request.method == "POST":
                data = request.get_json(silent=True) or {}
                if not isinstance(data, dict):
                    abort(400)
                try:
                    submit(db, order_id, data.get("ttn"), data.get("version"))
                    db.commit()
                except CourierError as exc:
                    return jsonify(error=str(exc)), 409
                return jsonify(ok=True)
            dispatch = db.get(Dispatch, order_id)
            order = db.get(site.Order, order_id) if dispatch else None
            if not order or (not order.ttn and is_closed(order)):
                abort(404)
            return jsonify(csrf=csrf, card=card(db, order, dispatch))
    return MODELS
