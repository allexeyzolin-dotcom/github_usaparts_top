"""Receive seller order notifications from the shop's n8n workflow."""
import hashlib
import json
import re
import secrets
import threading
from datetime import datetime

from flask import abort, flash, jsonify, redirect, render_template, request, session, url_for
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Text, text


SOURCE = "autopro"
TOKEN_KEY = "avtopro_mail_token_hash"
LOCK = threading.Lock()
MAIL_LIMIT = 32768
EVENT_LABELS = {"new": "Нове замовлення", "paid": "Оплата", "cancelled": "Скасування", "review": "Потребує звірки"}
ORDER_ID = re.compile(r"(?:замовлення|заказ(?:а)?)\s*(?:(?:№|#|номер\b)\s*|(?=\d))([0-9]{3,20})(?!\d)", re.IGNORECASE)


def classify(subject):
    subject = subject.casefold()
    if re.search(r"(?:скасован|скасував|скасувала|отмен[её]н|отменил)", subject):
        return "cancelled"
    if re.search(r"(?:сплатив|сплачено|оплачен|оплатив|оплатил)", subject):
        return "paid"
    if re.search(r"нов(?:е|ий|ое|ый)\s+(?:замовлення|заказ)", subject):
        return "new"
    return "review"


def parse_message(payload):
    if not isinstance(payload, dict):
        abort(400)
    def field(name, limit):
        value = payload.get(name, "")
        if not isinstance(value, str):
            abort(400)
        return value.strip()[:limit]

    sender = field("sender", 320).lower()
    if not re.fullmatch(r"[^\s<>@]+@(?:[a-z0-9-]+\.)*(?:avto\.pro|avtopro\.ua)", sender):
        abort(422, "Expected an Avtopro notification sender")
    subject = " ".join(field("subject", 500).split())
    message_id = field("messageId", 998)
    body = field("text", 5000).replace("\0", "")
    if not subject or not message_id:
        abort(422, "Expected subject and mailbox message ID")
    explicit_ids = set(ORDER_ID.findall(subject + "\n" + body))
    order_number = explicit_ids.pop() if len(explicit_ids) == 1 else ""
    return {
        "message_key": hashlib.sha256(message_id.encode()).hexdigest(),
        "subject": subject,
        "email_text": body,
        "event_kind": classify(subject),
        "order_number": order_number,
    }


def install(site):
    class AvtoproMailEvent(site.Base):
        __tablename__ = "avtopro_mail_events"
        message_key = Column(String(64), primary_key=True)
        order_number = Column(String(20), nullable=False, index=True)
        event_kind = Column(String(16), nullable=False)
        subject = Column(String(500), nullable=False)
        email_text = Column(Text, nullable=False)
        order_id = Column(Integer, ForeignKey("orders.id", ondelete="SET NULL"), nullable=True, index=True)
        received_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)

    @site.app.post("/api/integrations/n8n/avtopro-mail")
    def receive_avtopro_mail():
        if not request.is_json or request.content_length is None:
            abort(415)
        if request.content_length > MAIL_LIMIT:
            abort(413)
        header = request.headers.get("Authorization", "")
        if not re.fullmatch(r"Bearer [A-Za-z0-9_-]{43}", header):
            abort(401)
        supplied_hash = hashlib.sha256(header[7:].encode()).hexdigest()
        with site.SessionLocal() as db:
            expected = db.query(site.ApiSetting).filter_by(setting_key=TOKEN_KEY).one_or_none()
            if not expected or not secrets.compare_digest(supplied_hash, expected.setting_value):
                abort(401)
            try:
                message = parse_message(json.loads(request.get_data(cache=False)))
            except (UnicodeError, json.JSONDecodeError):
                abort(400)
            with LOCK:
                if db.get_bind().dialect.name == "postgresql":
                    db.execute(text("SELECT pg_advisory_xact_lock(726576660012)"))
                existing = db.get(AvtoproMailEvent, message["message_key"])
                if existing:
                    return jsonify(result="duplicate", orderId=existing.order_id)
                number = message["order_number"]
                order = None
                if number:
                    matches = db.query(site.Order).filter_by(external_source=SOURCE, external_order_id=number).limit(2).all()
                    prior_cancel = db.query(AvtoproMailEvent.message_key).filter_by(
                        order_number=number, event_kind="cancelled").first()
                    if len(matches) == 1:
                        order = matches[0]
                    elif not matches and not prior_cancel and message["event_kind"] == "new":
                        order = site.Order(customer_name=f"Автопро №{number}", external_source=SOURCE,
                            external_order_id=number, external_status="email_review", status="new",
                            comment="Потребує звірки з Автопро. Додайте перевірені позиції до замовлення.")
                        db.add(order)
                        db.flush()
                    if order:
                        message["order_id"] = order.id
                        # Link an earlier payment or notice that arrived before this order.
                        db.query(AvtoproMailEvent).filter_by(order_number=number, order_id=None).update(
                            {"order_id": order.id}, synchronize_session=False)
                db.add(AvtoproMailEvent(**message))
                result = {"result": "received", "orderId": message.get("order_id")}
                db.commit()
        return jsonify(result), 201

    @site.app.route("/admin/marketplaces/avtopro-mail", methods=["GET", "POST"])
    @site.admin_required
    def admin_avtopro_mail():
        csrf = session.setdefault("avtopro_mail_csrf", secrets.token_urlsafe(32))
        token = ""
        with site.SessionLocal() as db:
            if request.method == "POST":
                if not secrets.compare_digest(csrf.encode(), request.form.get("csrf_token", "").encode()):
                    abort(403)
                token = secrets.token_urlsafe(32)
                site.set_setting(db, TOKEN_KEY, hashlib.sha256(token.encode()).hexdigest())
                db.commit()
                flash("Ключ для n8n створено", "success")
            events = db.query(AvtoproMailEvent).order_by(AvtoproMailEvent.received_at.desc()).limit(50).all()
            response = site.app.make_response(render_template("admin_avtopro_mail.html", csrf=csrf,
                token=token, key_set=db.query(site.ApiSetting.id).filter_by(setting_key=TOKEN_KEY).first() is not None,
                events=events, labels=EVENT_LABELS))
            response.headers.update({"Cache-Control": "no-store", "Referrer-Policy": "no-referrer"})
            return response

    def order_notices(db, orders):
        ids = [order.id for order in orders if order.external_source == SOURCE]
        notices = {}
        if ids:
            events = db.query(AvtoproMailEvent.order_id, AvtoproMailEvent.event_kind).filter(
                AvtoproMailEvent.order_id.in_(ids)).distinct()
            for order_id, kind in events:
                notices.setdefault(order_id, set()).add(kind)
        return notices

    return order_notices
