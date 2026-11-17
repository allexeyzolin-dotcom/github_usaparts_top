"""Background Nova Poshta tracking for customer orders; inventory is never changed."""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo
import re
import threading

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Text, text

SITE = None
MODELS = None
LOCK = threading.Lock()
STARTED = False
INTERVAL = timedelta(minutes=5)
COLLECTED = {"9", "10", "11"}


def number(value):
    return re.sub(r"\s+", "", str(value or ""))


def local_label(value):
    return value.replace(tzinfo=timezone.utc).astimezone(ZoneInfo("Europe/Kyiv")).strftime("%d.%m %H:%M") if value else ""


def prime_cache(db, orders):
    ids = [order.id for order in orders if order.ttn]
    db.info["order_tracking_cache"] = {row.order_id: row for row in
        db.query(MODELS.Tracking).filter(MODELS.Tracking.order_id.in_(ids)).all()} if ids else {}


def tracking_for(db, order):
    cache = db.info.get("order_tracking_cache")
    return cache.get(order.id) if cache is not None else db.get(MODELS.Tracking, order.id)


def serialize(db, order):
    row = tracking_for(db, order)
    current = row is not None and row.ttn == number(order.ttn)
    code = row.status_code if current else ""
    return {"ttn": order.ttn or "", "status": order.ttn_status if current or not row else "Очікує перевірки",
            "statusCode": code, "checkedAt": local_label(row.checked_at) if current else "",
            "lastSuccessAt": local_label(row.succeeded_at) if current else "",
            "error": row.last_error if current else "",
            "received": code in COLLECTED,
            "attention": code in {"2", "102", "103", "106", "108"},
            "enabled": bool(order.ttn) and order.status not in {"done", "cancelled"}}


def refresh(db, orders, *, now=None, force=False):
    """Batch requests and match by waybill, then recheck orders after network I/O."""
    now = now or datetime.utcnow()
    due = {}
    invalid = []
    for order in orders:
        if not order.ttn or order.status == "cancelled" or (not force and order.status == "done"):
            continue
        ttn = number(order.ttn)
        row = db.get(MODELS.Tracking, order.id)
        changed = row is None or row.ttn != ttn
        if not force and not changed and (row.status_code in COLLECTED or
                (row.checked_at and row.checked_at > now - INTERVAL)):
            continue
        if not re.fullmatch(r"\d{14}", ttn):
            invalid.append((order.id, ttn))
            continue
        due.setdefault(ttn, []).append(order.id)
    for order_id, ttn in invalid:
        order, row = locked_current(db, order_id, ttn, now)
        if row is not None:
            row.checked_at, row.last_error = now, "ТТН Нової пошти повинна містити 14 цифр."
    numbers = list(due)
    for start in range(0, len(numbers), 100):
        batch = numbers[start:start + 100]
        error = ""
        try:
            snapshots, raw = SITE.np_request(db, "TrackingDocument", "getStatusDocuments",
                {"Documents": [{"DocumentNumber": ttn} for ttn in batch]})
            if not isinstance(raw, dict) or not raw.get("success", False):
                raise ValueError("carrier_error")
            snapshots = {number(item.get("Number") or item.get("DocumentNumber")): item
                         for item in snapshots if isinstance(item, dict)}
        except Exception:
            snapshots = {}
            error = "Нова пошта тимчасово недоступна. Повторна перевірка через 5 хвилин."
        for ttn in batch:
            for order_id in due[ttn]:
                # A waybill replacement or cancellation during a slow carrier
                # response must not apply that response to the current order.
                order, row = locked_current(db, order_id, ttn, now)
                if row is None:
                    continue
                row.checked_at = now
                snapshot = snapshots.get(ttn)
                status = str((snapshot or {}).get("Status") or "").strip()
                if not status:
                    row.last_error = error or "НП поки не повернула дані цієї ТТН. Перевірка повториться автоматично."
                    continue
                row.status_code = str(snapshot.get("StatusCode") or "")[:16]
                row.succeeded_at, row.last_error = now, ""
                order.ttn_status = status[:255]
    db.info.pop("order_tracking_cache", None)
    return len(numbers)


def locked_current(db, order_id, ttn, now):
    order = (db.query(SITE.Order).filter_by(id=order_id).populate_existing()
             .with_for_update().one_or_none())
    if not order or number(order.ttn) != ttn or order.status == "cancelled":
        return None, None
    # The order lock serializes row creation with manual refreshes too.
    row = db.query(MODELS.Tracking).filter_by(order_id=order_id).populate_existing().one_or_none()
    if row is None:
        row = MODELS.Tracking(order_id=order_id, ttn=ttn, status_code="", last_error="")
        db.add(row)
    elif row.checked_at and row.checked_at > now:
        return None, None
    if row.ttn != ttn:
        row.ttn, row.status_code, row.checked_at, row.succeeded_at, row.last_error = ttn, "", None, None, ""
    return order, row


def run_once(now=None):
    if not LOCK.acquire(blocking=False):
        return
    connection = None
    try:
        if SITE.engine.dialect.name == "postgresql":
            connection = SITE.engine.connect()
            if not connection.execute(text("SELECT pg_try_advisory_lock(77303274)")).scalar():
                return
        now = now or datetime.utcnow()
        with SITE.SessionLocal() as db:
            orders = db.query(SITE.Order).filter(SITE.Order.ttn != "",
                SITE.Order.status.notin_(["done", "cancelled"])).all()
            refresh(db, orders, now=now)
            SITE.set_setting(db, "order_tracking_last_run", now.isoformat())
            db.commit()
    finally:
        if connection:
            connection.execute(text("SELECT pg_advisory_unlock(77303274)"))
            connection.close()
        LOCK.release()


def start_worker():
    global STARTED
    if STARTED:
        return
    def loop():
        while True:
            try:
                run_once()
            except Exception:
                SITE.app.logger.warning("Order waybill tracking check failed")
            threading.Event().wait(30)
    threading.Thread(target=loop, name="order-tracking", daemon=True).start()
    STARTED = True


def install(site):
    global SITE, MODELS
    SITE = site

    class Tracking(site.Base):
        __tablename__ = "order_tracking"
        order_id = Column(Integer, ForeignKey("orders.id", ondelete="CASCADE"), primary_key=True)
        ttn = Column(String(64), nullable=False, default="")
        status_code = Column(String(16), nullable=False, default="")
        checked_at = Column(DateTime)
        succeeded_at = Column(DateTime)
        last_error = Column(Text, nullable=False, default="")

    MODELS = SimpleNamespace(Tracking=Tracking)
    return MODELS
