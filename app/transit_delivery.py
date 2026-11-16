"""Inbound carriers, Nova Poshta tracking and durable Kyiv-time reminders."""
import hashlib
import json
import re
import secrets
import threading
import time
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from flask import abort, flash, redirect, request, session, url_for
from sqlalchemy import Boolean, Column, Date, DateTime, ForeignKey, Integer, String, Text, event, or_, text

from .telegram_stock import api as telegram_api

SITE = None
MODELS = None
STARTED = False
LOCK = threading.Lock()
WAKE = threading.Event()
KYIV = ZoneInfo("Europe/Kyiv")
ARRIVED = {"7", "8"}
COLLECTED = {"9", "10", "11"}
FORM_KEYS = ("carrier", "carrier_name", "expected_date", "ttn")


class DeliveryError(ValueError):
    pass


def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def local_now(value=None):
    value = value or utc_now()
    return (value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value).astimezone(KYIV)


def parse_np_datetime(value):
    value = str(value or "").strip()
    if not value or value.startswith(("0000", "00.00")):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        parsed = None
        for pattern in ("%d.%m.%Y %H:%M:%S", "%d-%m-%Y %H:%M:%S", "%d.%m.%Y %H:%M", "%d-%m-%Y %H:%M", "%d.%m.%Y", "%d-%m-%Y"):
            try:
                parsed = datetime.strptime(value, pattern)
                break
            except ValueError:
                continue
    if parsed is None:
        return None
    return (parsed.replace(tzinfo=KYIV) if parsed.tzinfo is None else parsed).astimezone(KYIV)


def read_form(remember=False):
    previous = session.get("transit_delivery_draft", {})
    values = {key: str(request.form.get(key, previous.get(key, "")) or "").strip() for key in FORM_KEYS}
    if remember:
        session["transit_delivery_draft"] = values
    return values


def validate(values):
    carrier = values.get("carrier", "")
    if carrier not in {"nova_poshta", "other"}:
        raise DeliveryError("Оберіть перевізника: Нова Пошта або Інші.")
    name = str(values.get("carrier_name", "")).strip()
    if len(name) > 255 or (carrier == "other" and not name):
        raise DeliveryError("Вкажіть, хто привезе товар (до 255 символів).")
    raw_date = values.get("expected_date", "")
    try:
        expected = date.fromisoformat(raw_date) if raw_date else None
    except (TypeError, ValueError):
        raise DeliveryError("Вкажіть коректну орієнтовну дату прибуття.") from None
    if carrier == "other" and expected is None:
        raise DeliveryError("Для іншого перевізника вкажіть орієнтовну дату прибуття.")
    ttn = re.sub(r"\s", "", str(values.get("ttn", ""))) if carrier == "nova_poshta" else ""
    if ttn and not re.fullmatch(r"[0-9]{14}", ttn):
        raise DeliveryError("ТТН Нової пошти має містити 14 цифр.")
    return {"carrier": carrier, "carrier_name": name if carrier == "other" else "",
            "expected_date": expected, "manual_date": expected is not None, "ttn": ttn}


def serialize(db, batch_id):
    if not batch_id:
        shipment = None
    else:
        cache = db.info.setdefault("transit_delivery_cache", {})
        if batch_id not in cache:
            cache[batch_id] = db.get(MODELS.Shipment, batch_id)
        shipment = cache[batch_id]
    if not shipment:
        return {"carrier": "", "carrierName": "", "label": "Перевізник не вказаний",
                "expectedDate": "", "expectedLabel": "Не визначена", "ttn": "", "npStatus": "", "error": "", "pickedAt": ""}
    return {"carrier": shipment.carrier, "carrierName": shipment.carrier_name,
            "label": carrier_label(shipment), "expectedDate": shipment.expected_date.isoformat() if shipment.expected_date else "",
            "expectedLabel": shipment.expected_date.strftime("%d.%m.%Y") if shipment.expected_date else "Не визначена",
            "ttn": shipment.ttn, "npStatus": shipment.np_status, "error": shipment.last_error,
            "pickedAt": shipment.picked_at.strftime("%d.%m.%Y %H:%M:%S") if shipment.picked_at else ""}


def carrier_label(shipment):
    return "Нова Пошта" if shipment.carrier == "nova_poshta" else "Інші: " + shipment.carrier_name


def orders_for(db, batch_id):
    return db.query(SITE.TransitOrder).filter_by(batch_id=batch_id).order_by(SITE.TransitOrder.id).all()


def remaining(orders):
    return [(row, max(int(row.qty or 0) - int(row.accepted_qty or 0), 0)) for row in orders
            if row.status != "cancelled" and int(row.qty or 0) > int(row.accepted_qty or 0)]


def queue_notice(db, shipment, kind, key, payload=None):
    if db.query(MODELS.Notice.id).filter_by(event_key=key).first():
        return
    db.add(MODELS.Notice(batch_id=shipment.batch_id, kind=kind, event_key=key,
                        payload=json.dumps(payload or {}, ensure_ascii=False)))
    db.info["transit_delivery_wakeup"] = True


def save_shipment(db, batch_id, values, *, created=False):
    clean = validate(values)
    shipment = db.get(MODELS.Shipment, batch_id)
    if not shipment:
        shipment = MODELS.Shipment(batch_id=batch_id)
        db.add(shipment)
    elif (not shipment.manual_date and clean["expected_date"] == shipment.expected_date and
          shipment.carrier == clean["carrier"]):
        # Saving an unchanged, API-filled date must not turn it into a manual estimate.
        clean["manual_date"] = False
    changed_ttn = shipment.ttn != clean["ttn"] or shipment.carrier != clean["carrier"]
    before = (shipment.carrier, shipment.carrier_name, shipment.expected_date, shipment.ttn)
    for key, value in clean.items():
        setattr(shipment, key, value)
    if changed_ttn:
        if not shipment.manual_date:
            shipment.expected_date = None
        shipment.np_code = shipment.np_status = shipment.sender_city = shipment.recipient_city = ""
        shipment.arrival_notified = shipment.pickup_notified = False
        shipment.picked_at = shipment.pickup_detected_at = shipment.checked_at = None
        shipment.last_error = ""
    shipment.finished_at = None
    shipment.updated_at = utc_now()
    db.flush()
    if changed_ttn:
        db.query(MODELS.Notice).filter_by(batch_id=batch_id, status="pending").filter(
            MODELS.Notice.kind.in_(["arrived", "picked", "picked_time", "reminder"])).update(
                {MODELS.Notice.status: "cancelled"}, synchronize_session=False)
    after = (shipment.carrier, shipment.carrier_name, shipment.expected_date, shipment.ttn)
    if created:
        queue_notice(db, shipment, "created", "created:" + batch_id)
    elif before != after:
        digest = hashlib.sha256(repr(after).encode()).hexdigest()[:20]
        queue_notice(db, shipment, "updated", "updated:" + batch_id + ":" + digest)
    return shipment


def position_lines(positions):
    grouped = {}
    for row, qty in positions:
        key = (row.part_number, row.title[:160])
        grouped[key] = grouped.get(key, 0) + qty
    return [f"• {number} — {title} — {qty} шт." for (number, title), qty in grouped.items()]


def split_messages(lines, limit=3500):
    parts, current = [], ""
    for line in lines:
        candidate = current + ("\n" if current else "") + line
        if len(candidate.encode("utf-16-le")) // 2 > limit and current:
            parts.append(current)
            current = line
        else:
            current = candidate
    if current:
        parts.append(current)
    if len(parts) > 1:
        parts = [f"{body}\n\nЧастина {index + 1}/{len(parts)}" for index, body in enumerate(parts)]
    return parts


def render_notice(shipment, notice, orders, now):
    outstanding = remaining(orders)
    if notice.kind == "reminder" and not outstanding:
        return []
    payload = json.loads(notice.payload or "{}")
    title = orders[0].short_description or orders[0].title if orders else "Замовлення в дорозі"
    header = {"created": "📦 Замовлено товар у дорозі", "updated": "🚚 Доставку замовлення оновлено",
              "arrived": "📬 Товар прибув на Нову пошту", "picked": "✅ Товар забрали з Нової пошти",
              "picked_time": "🕒 Нова пошта уточнила час отримання", "reminder": "⏰ Товар ще не прийнятий на складі або не прибув"}[notice.kind]
    lines = [header, "", "Замовлення: " + title[:300], "Перевізник: " + carrier_label(shipment),
             "Орієнтовна дата: " + (shipment.expected_date.strftime("%d.%m.%Y") if shipment.expected_date else "ще не визначена; очікуємо дані ТТН")]
    if shipment.ttn:
        lines.append("ТТН: " + shipment.ttn)
    if notice.kind == "arrived":
        sender = payload.get("sender") or "не вказане Новою поштою"
        city = payload.get("recipient") or "місто призначення"
        lines.extend(["Місто відправника: " + sender,
                      ("Товар прибув у Ромни, НП." if "ромни" in city.casefold() else "Товар прибув: " + city + ", НП."),
                      "Не забудьте роздрукувати етикетки для швидкого прийому товару:"])
    elif notice.kind in {"picked", "picked_time"}:
        lines.append("Час отримання за даними НП: " + (payload.get("picked") or "точний час Нова пошта ще не передала"))
        lines.append("Позиції у відправленні:")
    elif notice.kind == "reminder":
        if shipment.pickup_notified:
            lines.append("Відправлення вже забрали з НП; вказані нижче позиції ще не прийняті на складі.")
        elif shipment.arrival_notified:
            lines.append("Відправлення прибуло на НП; вказані нижче позиції ще не прийняті на складі.")
        lines.extend(["Стан на " + local_now(now).strftime("%d.%m.%Y %H:%M") + " (Київ).", "Неприйняті артикули та кількість:"])
    else:
        lines.append("Замовлені позиції:")
    positions = outstanding if notice.kind == "reminder" else [(row, int(row.qty or 0)) for row in orders]
    lines.extend(position_lines(positions))
    return split_messages(lines)


def apply_np_snapshot(db, shipment, snapshot, now):
    code = str(snapshot.get("StatusCode") or "")
    shipment.np_code = code
    shipment.np_status = str(snapshot.get("Status") or "")[:500]
    def city(name):
        for key in (name, name + "Description"):
            value = str(snapshot.get(key) or "").strip()
            if value and not re.fullmatch(r"[0-9a-fA-F-]{36}", value):
                return value[:255]
        return ""
    shipment.sender_city = city("CitySender") or shipment.sender_city
    shipment.recipient_city = city("CityRecipient") or shipment.recipient_city
    estimated = parse_np_datetime(snapshot.get("ScheduledDeliveryDate") or snapshot.get("ScheduledDeliveryDateFrom"))
    if estimated and not shipment.manual_date:
        shipment.expected_date = estimated.date()
    shipment.last_error = ""
    key = shipment.batch_id + ":" + shipment.ttn
    if code in COLLECTED:
        # RecipientDateTime is the handover time; DateScan/ActualDeliveryDate
        # can refer to arrival or another scan and must not be invented as pickup.
        raw_pickup = str(snapshot.get("RecipientDateTime") or "").strip()
        picked = parse_np_datetime(raw_pickup) if re.search(r"\d{1,2}:\d{2}", raw_pickup) else None
        had_exact_time = shipment.picked_at is not None
        if picked:
            shipment.picked_at = picked.replace(tzinfo=None)  # NP timestamps are stored/displayed as Kyiv local time.
        payload = {"picked": picked.strftime("%d.%m.%Y %H:%M:%S") + " (Київ)" if picked else ""}
        if not shipment.pickup_notified:
            queue_notice(db, shipment, "picked", "picked:" + key, payload)
            shipment.pickup_notified = True
            shipment.pickup_detected_at = now
        elif picked and not had_exact_time:
            queue_notice(db, shipment, "picked_time", "picked-time:" + key, payload)
        # A waybill first added after collection must not send a stale 'at NP' alert.
        shipment.arrival_notified = True
    elif code in ARRIVED and not shipment.arrival_notified and not shipment.pickup_notified:
        queue_notice(db, shipment, "arrived", "arrived:" + key,
                     {"sender": shipment.sender_city, "recipient": shipment.recipient_city})
        shipment.arrival_notified = True


def update_tracking(db, shipments, now):
    due = [row for row in shipments if row.carrier == "nova_poshta" and row.ttn and
           (not row.pickup_notified or (row.picked_at is None and row.pickup_detected_at and
                                       row.pickup_detected_at > now - timedelta(days=3))) and
           (row.checked_at is None or row.checked_at <= now - timedelta(minutes=5))]
    if not due:
        return
    settings = SITE.get_api_settings_map(db)
    if not settings.get("nova_poshta_api_key"):
        for row in due:
            row.last_error = "Не налаштовано API Нової пошти. Додайте ключ у вкладці API."
            row.checked_at = now
        return
    by_number = {}
    for row in due:
        by_number.setdefault(row.ttn, []).append(row)
    numbers = list(by_number)
    for start in range(0, len(numbers), 100):
        batch = numbers[start:start + 100]
        for number in batch:
            for row in by_number[number]:
                row.checked_at = now
        try:
            items, raw = SITE.np_request(db, "TrackingDocument", "getStatusDocuments",
                                        {"Documents": [{"DocumentNumber": number} for number in batch]})
            if not raw.get("success", True):
                raise DeliveryError("Нова пошта не підтвердила запит. Перевірте API-ключ і ТТН.")
            found = set()
            for item in items:
                number = str(item.get("Number") or item.get("DocumentNumber") or "")
                if number not in by_number:
                    continue
                found.add(number)
                for row in by_number[number]:
                    apply_np_snapshot(db, row, item, now)
            for number in set(batch) - found:
                for row in by_number[number]:
                    row.last_error = "Нова пошта поки не повернула дані цієї ТТН. Перевірка повториться автоматично."
        except Exception as error:
            message = str(error) if isinstance(error, DeliveryError) else "Не вдалося перевірити ТТН. Спробуємо автоматично через 5 хвилин."
            for number in batch:
                for row in by_number[number]:
                    row.last_error = message


def process_due(db, now):
    shipments = db.query(MODELS.Shipment).filter(MODELS.Shipment.finished_at.is_(None)).all()
    groups = {row.batch_id: orders_for(db, row.batch_id) for row in shipments}
    live = [row for row in shipments if groups[row.batch_id]]
    update_tracking(db, live, now)
    local = local_now(now)
    for shipment in shipments:
        orders = groups[shipment.batch_id]
        pending = remaining(orders)
        if (pending and shipment.expected_date and shipment.expected_date <= local.date() and local.hour >= 8 and
                shipment.reminded_on != local.date()):
            queue_notice(db, shipment, "reminder", "reminder:" + shipment.batch_id + ":" + local.date().isoformat(),
                         {"day": local.date().isoformat()})
            shipment.reminded_on = local.date()
        tracking_done = shipment.carrier != "nova_poshta" or not shipment.ttn or (
            shipment.pickup_notified and (shipment.picked_at is not None or
             (shipment.pickup_detected_at and shipment.pickup_detected_at < now - timedelta(days=3))))
        if not orders or (not pending and tracking_done):
            shipment.finished_at = now


def deliver_one(db, now):
    notice = (db.query(MODELS.Notice).filter_by(status="pending")
              .filter(or_(MODELS.Notice.next_attempt.is_(None), MODELS.Notice.next_attempt <= now))
              .order_by(MODELS.Notice.id).with_for_update(skip_locked=True).first())
    if not notice:
        return False
    shipment = db.get(MODELS.Shipment, notice.batch_id)
    orders = orders_for(db, notice.batch_id)
    day = json.loads(notice.payload or "{}").get("day", "")
    stale_reminder = notice.kind == "reminder" and shipment and (
        not remaining(orders) or not shipment.expected_date or shipment.expected_date > local_now(now).date() or
        (day and day < local_now(now).date().isoformat()))
    if not shipment or not orders or stale_reminder:
        notice.status = "cancelled"
        db.commit()
        return True
    config = SITE.telegram_settings(db)
    try:
        if not config["bot_token"] or not config["chat_id"]:
            raise DeliveryError("Не налаштовано Telegram bot token або chat ID.")
        chunks = json.loads(notice.chunks or "[]")
        if not chunks or (notice.kind == "reminder" and notice.sent_chunks == 0):
            chunks = render_notice(shipment, notice, orders, now)
            notice.chunks = json.dumps(chunks, ensure_ascii=False)
        if notice.sent_chunks < len(chunks):
            telegram_api(config["bot_token"], "sendMessage", {"chat_id": config["chat_id"],
                         "text": chunks[notice.sent_chunks], "disable_web_page_preview": True})
            notice.sent_chunks += 1
        notice.last_error = ""
        if notice.sent_chunks >= len(chunks):
            notice.status, notice.sent_at = "sent", now
        db.commit()
        return True
    except Exception as error:
        notice.attempts += 1
        notice.last_error = str(error) if isinstance(error, (DeliveryError, ValueError)) else "Не вдалося надіслати сповіщення Telegram."
        notice.next_attempt = now + timedelta(seconds=min(60 * 2 ** min(notice.attempts - 1, 4), 900))
        db.commit()
        return False


def run_once(now=None):
    if not LOCK.acquire(blocking=False):
        return False
    connection = None
    try:
        if SITE.engine.dialect.name == "postgresql":
            connection = SITE.engine.connect()
            if not connection.execute(text("SELECT pg_try_advisory_lock(77303273)")).scalar():
                return False
        now = now or utc_now()
        with SITE.SessionLocal() as db:
            process_due(db, now)
            db.commit()
            return deliver_one(db, now)
    finally:
        if connection:
            connection.execute(text("SELECT pg_advisory_unlock(77303273)"))
            connection.close()
        LOCK.release()


def start_worker():
    global STARTED
    if STARTED:
        return
    def loop():
        delay = 0
        while True:
            WAKE.wait(delay)
            WAKE.clear()
            try:
                if run_once():
                    # Telegram's per-chat rate applies even when a new commit
                    # wakes the worker while a previous notice is being sent.
                    time.sleep(1.2)
                    delay = 0
                else:
                    delay = 30
            except Exception:
                SITE.app.logger.warning("Inbound delivery scheduler failed")
                delay = 30
    threading.Thread(target=loop, name="transit-delivery", daemon=True).start()
    STARTED = True


def install(site):
    global SITE, MODELS
    SITE = site

    class Shipment(site.Base):
        __tablename__ = "transit_shipments"
        batch_id = Column(String(64), primary_key=True)
        carrier = Column(String(32), nullable=False)
        carrier_name = Column(String(255), default="", nullable=False)
        expected_date = Column(Date, nullable=True)
        manual_date = Column(Boolean, default=False, nullable=False)
        ttn = Column(String(14), default="", nullable=False, index=True)
        sender_city = Column(String(255), default="", nullable=False)
        recipient_city = Column(String(255), default="", nullable=False)
        np_code = Column(String(16), default="", nullable=False)
        np_status = Column(String(500), default="", nullable=False)
        arrival_notified = Column(Boolean, default=False, nullable=False)
        pickup_notified = Column(Boolean, default=False, nullable=False)
        pickup_detected_at = Column(DateTime, nullable=True)
        picked_at = Column(DateTime, nullable=True)
        checked_at = Column(DateTime, nullable=True)
        last_error = Column(Text, default="", nullable=False)
        reminded_on = Column(Date, nullable=True)
        finished_at = Column(DateTime, nullable=True)
        updated_at = Column(DateTime, default=utc_now, nullable=False)

    class Notice(site.Base):
        __tablename__ = "transit_delivery_notices"
        id = Column(Integer, primary_key=True)
        batch_id = Column(String(64), ForeignKey("transit_shipments.batch_id"), nullable=False, index=True)
        kind = Column(String(32), nullable=False)
        event_key = Column(String(200), nullable=False, unique=True)
        payload = Column(Text, default="{}", nullable=False)
        chunks = Column(Text, default="[]", nullable=False)
        sent_chunks = Column(Integer, default=0, nullable=False)
        status = Column(String(32), default="pending", nullable=False, index=True)
        attempts = Column(Integer, default=0, nullable=False)
        next_attempt = Column(DateTime, nullable=True)
        last_error = Column(Text, default="", nullable=False)
        sent_at = Column(DateTime, nullable=True)

    MODELS = SimpleNamespace(Shipment=Shipment, Notice=Notice)

    @event.listens_for(site.SessionLocal, "after_commit")
    def wake_after_commit(db):
        if db.info.pop("transit_delivery_wakeup", False):
            WAKE.set()

    @event.listens_for(site.SessionLocal, "after_rollback")
    def discard_wakeup(db):
        db.info.pop("transit_delivery_wakeup", None)

    @site.app.post("/admin/transit/batch/<batch_id>/delivery")
    @site.admin_required
    def admin_transit_delivery(batch_id):
        expected = session.get("transit_delivery_csrf", "")
        if not expected or not secrets.compare_digest(expected, request.form.get("csrf_token", "")):
            abort(403)
        with site.SessionLocal() as db:
            if not orders_for(db, batch_id):
                abort(404)
            try:
                save_shipment(db, batch_id, {key: request.form.get(key, "").strip() for key in FORM_KEYS})
                db.commit()
                flash("Перевізника, дату та ТТН збережено.", "success")
            except DeliveryError as error:
                db.rollback()
                flash(str(error), "error")
        return redirect(url_for("admin_transit"))

    return MODELS
