"""Marketplace cabinet and durable Prom synchronization for the existing shop."""
import base64
import hashlib
import json
import os
import secrets
import threading
import tempfile
import time
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from zoneinfo import ZoneInfo
from datetime import timezone

from cryptography.fernet import Fernet, InvalidToken
from flask import abort, flash, jsonify, redirect, render_template, request, session, url_for
from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, Numeric, String, Text, func, or_, text

from .prom_api import PromClient, PromError, money, quantity, sku_key, utc_date

SITE = None
MODELS = None
LOCK = threading.Lock()
SCHEDULER_STARTED = False
ACTIVE = {"pending": "new", "received": "processing", "paid": "processing", "delivered": "done"}


def dump(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def setting(db, name, default=""):
    row = db.query(SITE.ApiSetting).filter_by(setting_key="mp_" + name).one_or_none()
    return row.setting_value if row else default


def put(db, name, value):
    SITE.set_setting(db, "mp_" + name, str(value))


def cipher():
    secret = str(SITE.app.secret_key or "")
    if len(secret) < 20 or secret in {"dev-secret", "change-me-to-a-long-random-secret"}:
        # Older installations use the example session secret. Never derive a
        # token key from it: retain a random key on the persistent backup volume.
        path = SITE.BACKUP_DIR / ".marketplace-encryption.key"
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            if not path.exists():
                with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".mp-key-", delete=False) as handle:
                    temporary = handle.name
                    os.chmod(temporary, 0o600)
                    handle.write(Fernet.generate_key())
                    handle.flush()
                    os.fsync(handle.fileno())
                try:
                    try:
                        os.link(temporary, path)
                    except FileExistsError:
                        pass
                finally:
                    os.unlink(temporary)
            return Fernet(path.read_bytes().strip())
        except (OSError, ValueError):
            raise PromError("Недоступний постійний ключ шифрування маркетплейсів. Перевірте сховище резервних копій") from None
    key = hashlib.sha256(("marketplaces:v1:" + secret).encode()).digest()
    return Fernet(base64.urlsafe_b64encode(key))


def token(db):
    env = os.getenv("PROM_API_TOKEN", "").strip()
    if env:
        return env
    encrypted = setting(db, "prom_token")
    if not encrypted:
        return ""
    try:
        return cipher().decrypt(encrypted.encode()).decode()
    except (InvalidToken, UnicodeError):
        raise PromError("Токен не розшифровується. Збережіть його повторно після зміни ключа сервера") from None


def log(db, title, body="", *, level="info", order_id=None, event_key=None, notify=False):
    if event_key and db.query(MODELS.Event.id).filter_by(event_key=event_key).first():
        return
    db.add(MODELS.Event(title=title, body=body[:1800], level=level, order_id=order_id,
                        event_key=event_key or secrets.token_hex(20), notify=notify))


def available(part):
    return max(int(part.qty or 0), 0) if part and part.in_stock and not part.is_deleted else 0


def catalog(db, client, end, full=False):
    params = {}
    previous = setting(db, "prom_catalog_cursor")
    if previous and not full:
        params["last_modified_from"] = (utc_date(previous) - timedelta(minutes=5)).isoformat()
    rows = list(client.pages("products", **params))
    for raw in rows:
        identifier = str(raw["id"])
        link = db.query(MODELS.Link).filter_by(remote_id=identifier).one_or_none()
        if not link:
            link = MODELS.Link(remote_id=identifier)
            db.add(link)
        elif link.sku and sku_key(link.sku) != sku_key(raw.get("sku")):
            link.stock_ready = False
            link.issue = "Артикул картки Prom змінено. Перевірте відповідник на складі"
        link.external_id = str(raw.get("external_id") or "")
        link.sku = str(raw.get("sku") or "")
        link.name = str(raw.get("name") or "")[:500]
        link.currency = str(raw.get("currency") or "")
        link.price = money(raw.get("price"))
        link.presence = str(raw.get("presence") or "")
        link.remote_status = str(raw.get("status") or "")
        link.remote_qty = quantity(raw["quantity_in_stock"]) if raw.get("quantity_in_stock") is not None else None
        link.updated_at = datetime.utcnow()
    db.flush()
    candidates = defaultdict(list)
    for part in db.query(SITE.Part).filter_by(is_deleted=False).all():
        for key in {sku_key(part.part_number), sku_key(part.part_number_export)} - {""}:
            candidates[key].append(part)
    links = db.query(MODELS.Link).all()
    sku_counts = defaultdict(int)
    assigned = {link.part_id for link in links if link.part_id}
    for link in links:
        sku_counts[sku_key(link.sku)] += 1
    matched = 0
    for link in links:
        key = sku_key(link.sku)
        options = candidates.get(key, [])
        if not link.part_id and len(options) == 1 and sku_counts[key] == 1 and options[0].id not in assigned:
            part = options[0]
            link.part_id = part.id
            assigned.add(part.id)
            matched += 1
            if link.remote_qty is not None and available(part) == link.remote_qty:
                link.synced_qty = link.remote_qty
                link.stock_ready = True
                link.issue = ""
            else:
                link.issue = "Звірте початковий залишок"
    put(db, "prom_catalog_cursor", end.isoformat())
    db.commit()
    return len(rows), matched


def normalized_lines(db, raw):
    lines = []
    products = raw.get("products")
    if raw.get("status") == "canceled" and products == []:
        return []
    if not isinstance(products, list) or not products:
        raise PromError("У замовленні немає товарів")
    for product in products:
        identifier = str(product.get("id") or "")
        link = db.query(MODELS.Link).filter_by(remote_id=identifier).one_or_none()
        count = quantity(product.get("quantity"))
        if count <= 0:
            raise PromError("У замовленні є позиція з нульовою кількістю")
        lines.append({"remote_id": identifier, "part_id": link.part_id if link else None,
                      "ready": bool(link and link.stock_ready),
                      "sku": str(product.get("sku") or ""), "name": str(product.get("name") or ""),
                      "qty": count, "amount": money(product.get("price"))})
    return lines


def process_order(db, raw, *, retry=False, adopt=False, confirm_return=False):
    identifier = str(raw.get("id") or "")
    if not identifier.isdigit():
        raise PromError("Замовлення Prom не має ID")
    remote_status = str(raw.get("status") or "")
    fingerprint = hashlib.sha256(dump(raw).encode()).hexdigest()
    state = db.query(MODELS.OrderState).filter_by(remote_id=identifier).with_for_update().one_or_none()
    incoming_modified = utc_date(raw.get("date_modified"))
    if state and state.modified_at and incoming_modified and incoming_modified < state.modified_at:
        return "unchanged"
    created = state is None
    if not state:
        state = MODELS.OrderState(remote_id=identifier)
        db.add(state)
        db.flush()
    elif state.fingerprint == fingerprint and not state.issue and not retry:
        return "unchanged"
    start = utc_date(setting(db, "prom_started_at"))
    created_at = utc_date(raw.get("date_created"))
    if not created_at or not start:
        raise PromError("Не визначено початок обліку замовлень")
    if created:
        state.historical = created_at < start
    if adopt:
        state.historical = False
    order = db.query(SITE.Order).filter_by(id=state.order_id).populate_existing().with_for_update().one_or_none() if state.order_id else None
    if not order:
        order = SITE.Order(external_source="prom", external_order_id=identifier, created_at=created_at)
        db.add(order)
        db.flush()
        state.order_id = order.id
    previous_status = state.status
    previous_payment = state.payment_status
    total = money(raw.get("full_price", raw.get("price")))
    rate = state.fx_rate or Decimal(str(SITE.DEFAULT_UAH_RATE))
    state.total_uah = total
    state.fx_rate = rate
    state.payload_json = dump(raw)
    state.modified_at = utc_date(raw.get("date_modified")) or created_at
    state.status = remote_status
    state.payment_status = str((raw.get("payment_data") or {}).get("status") or "")
    order.prepayment_usd = 0 if state.payment_status == "paid" else total
    state.fingerprint = fingerprint
    state.updated_at = datetime.utcnow()
    order.customer_name = " ".join(str(raw.get(k) or "") for k in
                                   ("client_first_name", "client_second_name", "client_last_name")).strip()
    order.phone = str(raw.get("phone") or "")
    address = raw.get("delivery_address")
    order.city = str(address or "")[:255] if not isinstance(address, dict) else str(address.get("city") or "")
    order.comment = f"Prom №{identifier}. {raw.get('client_notes') or ''}".strip()
    order.total_usd = (total / Decimal(rate)).quantize(Decimal("0.01"))
    provider = raw.get("delivery_provider_data") or {}
    order.delivery_type = "nova_poshta" if provider.get("provider") == "nova_poshta" else "pickup"
    order.np_warehouse_ref = str(provider.get("recipient_warehouse_id") or "")
    order.np_warehouse_label = str(address or "")[:255]
    if provider.get("declaration_number"):
        order.ttn = str(provider["declaration_number"])
    order.external_status = remote_status
    order.updated_at = datetime.utcnow()
    state.issue = ""
    stock_changed = False
    line_change = False
    try:
        lines = normalized_lines(db, raw)
        if remote_status not in ACTIVE and remote_status != "canceled":
            raise PromError("Невідомий статус Prom. Потрібна перевірка")
        if not state.historical and remote_status != "canceled" and any(not item["part_id"] for item in lines):
            raise PromError("Зіставте всі товари замовлення зі складом")
        if not state.historical and remote_status != "canceled" and any(not item["ready"] for item in lines):
            raise PromError("Звірте початкові залишки товарів цього замовлення")
        # Part rows use the same locks as website checkout. A nested transaction
        # keeps previous reservations intact when an edited order cannot fit.
        ids = sorted({item.part_id for item in order.items if item.part_id} |
                     {item["part_id"] for item in lines if item["part_id"]})
        locked = db.query(SITE.Part).filter(SITE.Part.id.in_(ids)).order_by(SITE.Part.id).populate_existing().with_for_update(key_share=True).all()
        by_id = {part.id: part for part in locked}
        old_lines = sorted((item.part_id or 0, item.part_number, item.qty) for item in order.items)
        new_lines = sorted((item["part_id"] or 0, item["sku"], item["qty"]) for item in lines)
        line_change = old_lines != new_lines
        should_reserve = not state.historical and remote_status != "canceled"
        if order.stock_reserved and remote_status == "canceled":
            if order.status in {"done", "shipped"}:
                state.return_pending = True
            if state.return_pending and not confirm_return:
                raise PromError("Після відправки підтвердіть фізичне повернення товару")
            if confirm_return:
                state.return_pending = False
        with db.begin_nested():
            old_demand, new_demand = defaultdict(int), defaultdict(int)
            if order.stock_reserved:
                for item in order.items:
                    if not item.part_id:
                        raise PromError("У попереднього резерву немає прив'язки до складу")
                    old_demand[item.part_id] += item.qty
            if should_reserve:
                for item in lines:
                    new_demand[item["part_id"]] += item["qty"]
            deltas = {identifier: new_demand[identifier] - old_demand[identifier] for identifier in ids}
            for part_id, delta in deltas.items():
                part = by_id.get(part_id)
                if not part or (delta > 0 and (part.is_deleted or available(part) < delta)):
                    raise PromError("Недостатньо товару на складі. Резерв не змінено")
            for part_id, delta in deltas.items():
                if delta:
                    part = by_id[part_id]
                    part.qty = max(int(part.qty or 0), 0) - delta
                    part.in_stock = part.qty > 0 and not part.is_deleted
                    part.updated_at = datetime.utcnow()
                    stock_changed = True
            if old_lines != new_lines:
                order.items.clear()
                db.flush()
                for item in lines:
                    order.items.append(SITE.OrderItem(part_id=item["part_id"], part_number=item["sku"],
                        name=item["name"], qty=item["qty"],
                        price_usd=(item["amount"] / Decimal(rate)).quantize(Decimal("0.01"))))
                db.flush()
            else:
                existing = sorted(order.items, key=lambda item: (item.part_id or 0, item.part_number, item.qty))
                incoming = sorted(lines, key=lambda item: (item["part_id"] or 0, item["sku"], item["qty"]))
                for item, updated in zip(existing, incoming):
                    item.price_usd = (updated["amount"] / Decimal(rate)).quantize(Decimal("0.01"))
                    item.name = updated["name"]
            order.stock_reserved = should_reserve
        internal = ACTIVE.get(remote_status, "cancelled")
        if order.status not in {"shipped", "done"} or internal in {"done", "cancelled"}:
            order.status = internal
        order.is_processing = order.status in {"processing", "shipped"}
    except (PromError, ValueError) as error:
        state.issue = str(error)[:500]
        if not order.items:
            for product in raw.get("products", []):
                try:
                    count = quantity(product.get("quantity"))
                except PromError:
                    count = 1
                order.items.append(SITE.OrderItem(part_number=str(product.get("sku") or ""),
                    name=str(product.get("name") or ""), qty=count, price_usd=0))
        if remote_status == "canceled" and not state.return_pending:
            order.status = "cancelled"
    if not state.historical and (created or previous_status != remote_status or
                                previous_payment != state.payment_status or line_change):
        title = f"Prom №{identifier}: {raw.get('status_name') or remote_status}"
        details = f"Сума {total:.2f} грн. " + "; ".join(
            f"{p.get('sku') or p.get('name')}: {p.get('quantity')} шт." for p in raw.get("products", []))
        if state.issue:
            details += " · " + state.issue
        log(db, title, details, level="warning" if state.issue else "success", order_id=order.id,
            event_key=f"prom:{identifier}:{fingerprint}:{'issue' if state.issue else 'ok'}", notify=True)
    db.commit()
    if stock_changed:
        SITE.public_cache_clear()
    return "issue" if state.issue else ("created" if created else "updated")


def import_safely(db, raw, retry=False):
    try:
        return process_order(db, raw, retry=retry)
    except (PromError, ValueError) as error:
        db.rollback()
        identifier = str(raw.get("id") or "")
        state = db.query(MODELS.OrderState).filter_by(remote_id=identifier).one_or_none()
        if not state:
            try:
                created_at = utc_date(raw.get("date_created"))
                start = utc_date(setting(db, "prom_started_at"))
                historical = not created_at or not start or created_at < start
            except PromError:
                historical = True
            state = MODELS.OrderState(remote_id=identifier, historical=historical)
            db.add(state)
        state.payload_json = dump(raw)
        state.issue = str(error)[:500]
        state.status = str(raw.get("status") or "")
        state.updated_at = datetime.utcnow()
        log(db, f"Prom №{identifier}: потрібна перевірка", state.issue, level="warning",
            event_key="invalid:" + identifier + ":" + hashlib.sha256(dump(raw).encode()).hexdigest())
        db.commit()
        return "issue"


def sync_orders(db, client, end):
    previous = setting(db, "prom_orders_cursor")
    params = {"sort_dir": "desc"}
    if previous:
        params["last_modified_from"] = (utc_date(previous) - timedelta(minutes=5)).isoformat()
    else:
        params["date_from"] = (end - timedelta(days=30)).isoformat()
    params["last_modified_to"] = end.isoformat()
    rows = list(client.pages("orders", **params))
    counts = defaultdict(int)
    # Creation order makes simultaneous reservations deterministic.
    rows.sort(key=lambda raw: (str(raw.get("date_created") or ""), int(raw["id"])))
    seen = set()
    for raw in rows:
        counts[import_safely(db, raw)] += 1
        seen.add(str(raw["id"]))
    for state in db.query(MODELS.OrderState).filter(MODELS.OrderState.issue != "").all():
        if state.remote_id not in seen:
            counts[import_safely(db, json.loads(state.payload_json), retry=True)] += 1
    put(db, "prom_orders_cursor", end.isoformat())
    db.commit()
    return dict(counts)


def sync_stock(db, client):
    sent = 0
    for link in db.query(MODELS.Link).filter_by(stock_ready=True).order_by(MODELS.Link.id).all():
        part = db.get(SITE.Part, link.part_id) if link.part_id else None
        if not part:
            link.issue = "Товар складу видалено або не зіставлено"
            continue
        target = available(part)
        # Reconciliation never silently overwrites a manual edit on Prom.
        if target == link.synced_qty and link.remote_qty == target and link.presence == ("available" if target else "not_available") and not link.issue:
            continue
        try:
            remote = client.product(link.remote_id)
            remote_qty = quantity(remote.get("quantity_in_stock"))
            remote_presence = str(remote.get("presence") or "")
            link.remote_qty = remote_qty
            link.presence = remote_presence
            if str(remote.get("status")) in {"deleted", "deleted_by_moderator", "draft"}:
                raise PromError("Картка Prom неактивна; залишок не змінено")
            if remote_qty not in {link.synced_qty, target}:
                raise PromError("Залишок змінено на Prom. Звірте джерело кількості")
            desired_presence = "available" if target else "not_available"
            if remote_qty != target or remote_presence != desired_presence:
                client.stock(link.remote_id, target)
                # Read-back verifies both write permission and zero-stock behavior.
                check = client.product(link.remote_id)
                if quantity(check.get("quantity_in_stock")) != target or check.get("presence") != desired_presence:
                    raise PromError("Prom ще не підтвердив нову кількість. Перевірте облік залишків")
                sent += 1
                put(db, "prom_write_verified", datetime.utcnow().isoformat())
                if target == 0:
                    put(db, "prom_zero_verified", datetime.utcnow().isoformat())
                log(db, f"Залишок {link.sku} → Prom", f"Передано {target} шт.", level="success")
            link.synced_qty = target
            link.remote_qty = target
            link.presence = desired_presence
            link.last_stock_at = datetime.utcnow()
            link.issue = ""
            db.commit()
        except PromError as error:
            link.issue = str(error)
            db.commit()
            if "HTTP 401" in str(error) or "HTTP 403" in str(error) or "Ліміт" in str(error):
                raise
    db.commit()
    return sent


def verify_stock_access(db, client):
    """A write/read-back probe that preserves an already matching stock value."""
    links = db.query(MODELS.Link).filter_by(stock_ready=True).order_by(MODELS.Link.remote_qty, MODELS.Link.id).limit(20).all()
    for link in links:
        part = db.query(SITE.Part).filter_by(id=link.part_id).populate_existing().with_for_update(key_share=True).one_or_none()
        if not part or part.is_deleted:
            continue
        qty = available(part)
        expected = "available" if qty else "not_available"
        current = client.product(link.remote_id)
        if current.get("quantity_in_stock") is None or quantity(current["quantity_in_stock"]) != qty or current.get("presence") != expected:
            continue
        processed = client.stock(link.remote_id, qty)
        confirmed = client.product(link.remote_id)
        if quantity(confirmed.get("quantity_in_stock")) != qty or confirmed.get("presence") != expected:
            raise PromError("Prom не підтвердив запис кількості")
        put(db, "prom_endpoint_verified", datetime.utcnow().isoformat())
        if processed:
            put(db, "prom_write_verified", datetime.utcnow().isoformat())
        if not qty:
            put(db, "prom_zero_verified", datetime.utcnow().isoformat())
        log(db, "Перевірка залишків Prom завершена", f"{link.sku}: підтверджено попередні {qty} шт. " +
            ("Prom підтвердив обробку запису." if processed else "API прийняв запит без змін; реальну зміну кількості ще не перевірено."), level="success")
        db.commit()
        return qty
    raise PromError("Для перевірки запису спочатку зіставте й звірте хоча б один товар")


def deliver_notifications(db):
    if setting(db, "prom_notifications", "1") != "1":
        return
    configured = SITE.telegram_settings(db)
    if not configured["bot_token"] or not configured["chat_id"]:
        return
    for entry in db.query(MODELS.Event).filter_by(notify=True, notified=False).filter(
            MODELS.Event.attempts < 5).order_by(MODELS.Event.id).limit(20).all():
        try:
            SITE.send_telegram_message(db, entry.title + "\n" + entry.body)
            entry.notified = True
        except Exception:
            entry.attempts += 1
        db.commit()


def run_sync(kind="sync"):
    if not LOCK.acquire(blocking=False):
        return False
    connection = None
    db = SITE.SessionLocal()
    run = None
    client = None
    try:
        if SITE.engine.dialect.name == "postgresql":
            connection = SITE.engine.connect()
            if not connection.execute(text("SELECT pg_try_advisory_lock(77303271)")).scalar():
                return False
        client = PromClient(token(db))
        run = MODELS.Run(status="running")
        db.add(run)
        db.commit()
        run_id = run.id
        end = datetime.utcnow()
        verification = None
        if kind == "verify":
            verification = verify_stock_access(db, client)
            products, matched, sent, orders = 0, 0, 0, {}
        else:
            products, matched = catalog(db, client, end, full=kind == "catalog")
            orders = sync_orders(db, client, end)
            sent = sync_stock(db, client) if setting(db, "prom_stock", "1") == "1" else 0
            deliver_notifications(db)
        run = db.get(MODELS.Run, run_id)
        run.status = "success"
        run.summary = dump({"products": products, "matched": matched, "orders": orders, "stock": sent, "verification_qty": verification})
        run.finished_at = datetime.utcnow()
        put(db, "prom_last_success", run.finished_at.isoformat())
        put(db, "prom_failures", "0")
        put(db, "prom_last_error", "")
        db.commit()
        return True
    except Exception as error:
        db.rollback()
        message = str(error) if isinstance(error, PromError) else "Помилка обміну. Перевірте журнал сервера"
        if run:
            run = db.get(MODELS.Run, run.id)
            run.status = "error"
            run.summary = message
            run.finished_at = datetime.utcnow()
        failures = min(int(setting(db, "prom_failures", "0")) + 1, 7)
        put(db, "prom_failures", failures)
        put(db, "prom_last_error", message)
        put(db, "prom_retry_after", (datetime.utcnow() + timedelta(seconds=min(120 * 2 ** failures, 1800))).isoformat())
        log(db, "Помилка обміну Prom", message, level="error")
        db.commit()
        SITE.app.logger.warning("Prom synchronization failed (%s)", type(error).__name__)
        return False
    finally:
        if client:
            client.close()
        db.close()
        if connection:
            connection.execute(text("SELECT pg_advisory_unlock(77303271)"))
            connection.close()
        LOCK.release()


def kick(kind="sync"):
    if LOCK.locked():
        return False
    threading.Thread(target=run_sync, args=(kind,), name="prom-sync", daemon=True).start()
    return True


def scheduler_loop():
    while True:
        try:
            with SITE.SessionLocal() as db:
                enabled = setting(db, "prom_auto") == "1"
                interval = max(120, min(3600, int(setting(db, "prom_interval", "120"))))
                last = db.query(MODELS.Run).order_by(MODELS.Run.id.desc()).first()
                retry = utc_date(setting(db, "prom_retry_after")) if setting(db, "prom_failures", "0") != "0" else None
                due = not last or (datetime.utcnow() - last.started_at).total_seconds() >= interval
                if retry and retry > datetime.utcnow():
                    due = False
            if enabled and due:
                run_sync()
        except Exception:
            SITE.app.logger.warning("Marketplace scheduler check failed")
        time.sleep(15)


def start_scheduler():
    global SCHEDULER_STARTED
    if not SCHEDULER_STARTED:
        threading.Thread(target=scheduler_loop, name="marketplace-scheduler", daemon=True).start()
        SCHEDULER_STARTED = True


def install(app, site):
    global SITE, MODELS
    SITE = site
    asset_bytes = (site.BASE_DIR / "static" / "marketplaces.css").read_bytes() + (site.BASE_DIR / "static" / "marketplaces.js").read_bytes()
    app.jinja_env.globals["marketplace_asset_version"] = hashlib.sha256(asset_bytes).hexdigest()[:12]

    @app.context_processor
    def marketplace_assets():
        if request.path.startswith("/admin/marketplaces") and app.config.get("MARKETPLACE_PREVIEW"):
            contents = (site.BASE_DIR / "static" / "marketplaces.css").read_bytes() + (site.BASE_DIR / "static" / "marketplaces.js").read_bytes()
            return {"marketplace_asset_version": hashlib.sha256(contents).hexdigest()[:12]}
        return {}

    class Link(site.Base):
        __tablename__ = "prom_product_links"
        id = Column(Integer, primary_key=True)
        remote_id = Column(String(64), nullable=False, unique=True, index=True)
        external_id = Column(String(255), default="", nullable=False)
        sku = Column(String(255), default="", nullable=False, index=True)
        name = Column(String(500), default="", nullable=False)
        part_id = Column(Integer, ForeignKey("parts.id"), nullable=True, index=True)
        currency = Column(String(8), default="", nullable=False)
        price = Column(Numeric(14, 2), default=0, nullable=False)
        remote_qty = Column(Integer, nullable=True)
        presence = Column(String(32), default="", nullable=False)
        remote_status = Column(String(64), default="", nullable=False)
        synced_qty = Column(Integer, nullable=True)
        stock_ready = Column(Boolean, default=False, nullable=False)
        issue = Column(Text, default="", nullable=False)
        last_stock_at = Column(DateTime, nullable=True)
        updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    class OrderState(site.Base):
        __tablename__ = "prom_order_states"
        id = Column(Integer, primary_key=True)
        remote_id = Column(String(64), unique=True, index=True, nullable=False)
        order_id = Column(Integer, ForeignKey("orders.id"), nullable=True)
        status = Column(String(64), default="", nullable=False)
        payment_status = Column(String(64), default="", nullable=False)
        total_uah = Column(Numeric(14, 2), default=0, nullable=False)
        fx_rate = Column(Numeric(12, 6), nullable=True)
        historical = Column(Boolean, default=True, nullable=False)
        return_pending = Column(Boolean, default=False, nullable=False)
        payload_json = Column(Text, default="{}", nullable=False)
        fingerprint = Column(String(64), default="", nullable=False)
        issue = Column(Text, default="", nullable=False)
        modified_at = Column(DateTime, nullable=True)
        updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    class Event(site.Base):
        __tablename__ = "marketplace_events"
        id = Column(Integer, primary_key=True)
        title = Column(String(500), nullable=False)
        body = Column(Text, default="", nullable=False)
        level = Column(String(16), default="info", nullable=False)
        event_key = Column(String(255), unique=True, nullable=False)
        order_id = Column(Integer, ForeignKey("orders.id"), nullable=True)
        notify = Column(Boolean, default=False, nullable=False)
        notified = Column(Boolean, default=False, nullable=False)
        attempts = Column(Integer, default=0, nullable=False)
        created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    class Run(site.Base):
        __tablename__ = "marketplace_runs"
        id = Column(Integer, primary_key=True)
        status = Column(String(16), default="running", nullable=False)
        summary = Column(Text, default="", nullable=False)
        started_at = Column(DateTime, default=datetime.utcnow, nullable=False)
        finished_at = Column(DateTime, nullable=True)

    MODELS = SimpleNamespace(Link=Link, OrderState=OrderState, Event=Event, Run=Run)

    @app.template_filter("mp_time")
    def marketplace_time(value):
        if not value:
            return "Ще не було"
        parsed = utc_date(value) if isinstance(value, str) else value
        return parsed.replace(tzinfo=timezone.utc).astimezone(ZoneInfo("Europe/Kyiv")).strftime("%d.%m · %H:%M")

    def csrf():
        value = session.setdefault("marketplace_csrf", secrets.token_urlsafe(32))
        if request.method == "POST" and not secrets.compare_digest(value, request.form.get("csrf_token", "")):
            abort(403)
        return value

    @app.after_request
    def private_marketplace_responses(response):
        if request.path.startswith("/admin/marketplaces"):
            response.headers["Cache-Control"] = "no-store"
            response.headers["Referrer-Policy"] = "no-referrer"
            response.headers["X-Robots-Tag"] = "noindex, nofollow"
        return response

    @app.route("/admin/marketplaces", methods=["GET", "POST"])
    @site.admin_required
    def admin_marketplaces():
        form_token = csrf()
        tab = request.args.get("tab", "overview")
        if tab not in {"overview", "prom", "products", "orders", "journal", "avtopro"}:
            abort(404)
        db = site.SessionLocal()
        try:
            if request.method == "POST":
                action = request.form.get("action", "")
                try:
                    if action == "connect":
                        new_token = request.form.get("prom_token", "").strip()
                        if new_token:
                            encoded = cipher().encrypt(new_token.encode()).decode()
                            candidate = PromClient(new_token)
                        else:
                            encoded = None
                            candidate = PromClient(token(db))
                        try:
                            candidate.request("GET", "/products/list", params={"limit": 1})
                            candidate.request("GET", "/orders/list", params={"limit": 1})
                            existing_shop = db.query(Link).order_by(Link.id).first()
                            if existing_shop and new_token:
                                candidate.product(existing_shop.remote_id)
                        finally:
                            candidate.close()
                        if encoded:
                            put(db, "prom_token", encoded)
                        if not setting(db, "prom_started_at"):
                            put(db, "prom_started_at", datetime.utcnow().isoformat())
                        put(db, "prom_connected", "1")
                        log(db, "Prom підключено", "Товари й замовлення доступні через API", level="success")
                        db.commit()
                        kick("catalog")
                        flash("Prom підключено. Завантажуємо товари й історію замовлень", "success")
                    elif action == "preferences":
                        if not setting(db, "prom_connected"):
                            raise PromError("Спочатку перевірте підключення Prom")
                        put(db, "prom_auto", "1" if request.form.get("auto") else "0")
                        put(db, "prom_stock", "1" if request.form.get("stock") else "0")
                        put(db, "prom_notifications", "1" if request.form.get("notifications") else "0")
                        interval = int(request.form.get("interval", "120"))
                        if interval not in {120, 300, 600, 900}:
                            raise PromError("Оберіть інтервал зі списку")
                        put(db, "prom_interval", interval)
                        db.commit()
                        flash("Налаштування обміну збережено", "success")
                    elif action in {"sync", "catalog", "verify"}:
                        flash("Обмін запущено" if kick(action) else "Обмін уже виконується", "success")
                    elif action == "map":
                        link = db.get(Link, request.form.get("link_id", type=int))
                        part = db.get(site.Part, request.form.get("part_id", type=int))
                        if not link or not part or part.is_deleted:
                            raise PromError("Оберіть товар зі складу")
                        if db.query(Link.id).filter(Link.part_id == part.id, Link.id != link.id).first():
                            raise PromError("Ця складська позиція вже прив'язана до іншої картки Prom")
                        if link.part_id and db.query(site.OrderItem.id).join(site.Order).filter(
                                site.Order.external_source == "prom", site.Order.stock_reserved == True,
                                site.OrderItem.part_id == link.part_id).first():
                            raise PromError("Спочатку завершіть активні резерви цієї прив'язки")
                        link.part_id = part.id
                        link.stock_ready = available(part) == link.remote_qty
                        link.synced_qty = link.remote_qty if link.stock_ready else None
                        link.issue = "" if link.stock_ready else "Звірте початковий залишок"
                        db.commit()
                        flash("Товар зіставлено", "success")
                    elif action in {"resolve_site", "resolve_prom"}:
                        link = db.get(Link, request.form.get("link_id", type=int))
                        if not link or not link.part_id:
                            raise PromError("Спочатку зіставте товар")
                        candidate = PromClient(token(db))
                        try:
                            remote = candidate.product(link.remote_id)
                        finally:
                            candidate.close()
                        qty = quantity(remote.get("quantity_in_stock"))
                        part = db.query(site.Part).filter_by(id=link.part_id).populate_existing().with_for_update(key_share=True).one()
                        if action == "resolve_prom":
                            before = available(part)
                            part.qty = qty
                            part.in_stock = qty > 0
                            part.updated_at = datetime.utcnow()
                            site.queue_part_inventory_change(db, part, before, context_label="Звірка з Prom",
                                                             reason="Залишок підтверджено адміністратором")
                        link.synced_qty = qty
                        link.remote_qty = qty
                        link.stock_ready = True
                        link.issue = ""
                        log(db, f"Звірено {link.sku}", "Основою обрано " + ("сайт" if action == "resolve_site" else "Prom"))
                        db.commit()
                        kick()
                        flash("Залишок звірено, обмін запущено", "success")
                    elif action in {"retry_order", "adopt_order", "return_order"}:
                        state = db.get(OrderState, request.form.get("state_id", type=int))
                        if not state:
                            abort(404)
                        if not LOCK.acquire(blocking=False):
                            raise PromError("Дочекайтеся завершення обміну")
                        try:
                            process_order(db, json.loads(state.payload_json), retry=True,
                                          adopt=action == "adopt_order", confirm_return=action == "return_order")
                        finally:
                            LOCK.release()
                        flash("Замовлення оброблено. Перевірте його стан", "success")
                    elif action == "avtopro_settings":
                        put(db, "avtopro_account", request.form.get("account", "").strip()[:120])
                        put(db, "avtopro_note", request.form.get("note", "").strip()[:1000])
                        new_key = request.form.get("api_key", "").strip()
                        if new_key:
                            put(db, "avtopro_key", cipher().encrypt(new_key.encode()).decode())
                        db.commit()
                        flash("Базові налаштування Автопро збережено", "success")
                    else:
                        abort(400)
                except (PromError, ValueError) as error:
                    db.rollback()
                    flash(str(error), "error")
                return redirect(url_for("admin_marketplaces", tab=tab))
            links_count = db.query(func.count(Link.id)).scalar() or 0
            mapped_count = db.query(func.count(Link.id)).filter(Link.part_id != None).scalar() or 0
            ready_count = db.query(func.count(Link.id)).filter_by(stock_ready=True).filter(Link.issue == "").scalar() or 0
            issues = db.query(func.count(Link.id)).filter(or_(Link.issue != "", Link.part_id == None)).scalar() or 0
            order_issues = db.query(func.count(OrderState.id)).filter(OrderState.issue != "").scalar() or 0
            config = {key: setting(db, "prom_" + key, default) for key, default in
                      [("connected", ""), ("auto", "0"), ("stock", "1"), ("notifications", "1"),
                       ("interval", "120"), ("last_success", ""), ("last_error", ""), ("started_at", ""),
                       ("write_verified", ""), ("endpoint_verified", ""), ("zero_verified", "")]}
            telegram = site.telegram_settings(db)
            last_run = db.query(Run).order_by(Run.id.desc()).first()
            search = request.args.get("q", "").strip()[:100]
            page = max(1, request.args.get("page", 1, type=int))
            query = db.query(Link)
            if search:
                query = query.filter(or_(Link.sku.ilike(f"%{search}%"), Link.name.ilike(f"%{search}%")))
            if request.args.get("issues"):
                query = query.filter(or_(Link.issue != "", Link.part_id == None))
            total_filtered = query.count() if tab == "products" else 0
            product_rows = []
            if tab == "products":
                for link in query.order_by(Link.id).offset((page - 1) * 40).limit(40).all():
                    part = db.get(site.Part, link.part_id) if link.part_id else None
                    warehouse = db.get(site.Warehouse, part.warehouse_id) if part else None
                    product_rows.append({"link": link, "part": part, "warehouse": warehouse,
                                         "qty": available(part) if part else None})
            order_rows = []
            if tab in {"orders", "overview"}:
                order_query = db.query(OrderState).order_by(OrderState.modified_at.desc())
                if tab == "orders" and request.args.get("issues"):
                    order_query = order_query.filter(OrderState.issue != "")
                for state in order_query.limit(100 if tab == "orders" else 5).all():
                    raw = json.loads(state.payload_json)
                    order_rows.append({"state": state, "order": db.get(site.Order, state.order_id),
                        "items": raw.get("products", []), "label": raw.get("status_name") or state.status})
            feeds = []
            if tab == "avtopro":
                for warehouse in db.query(site.Warehouse).order_by(site.Warehouse.name).all():
                    key = db.query(site.ApiSetting).filter_by(setting_key=site.avtopro_feed_setting(warehouse.id)).one_or_none()
                    private_key = key.setting_value if key else ""
                    count, qty = db.query(func.count(site.Part.id), func.sum(site.Part.qty)).filter(
                        site.Part.warehouse_id == warehouse.id, site.Part.is_deleted == False,
                        site.Part.in_stock == True, site.Part.qty > 0).one()
                    feeds.append({"warehouse": warehouse, "positions": count, "qty": qty or 0,
                        "url": site.absolute_public_url(url_for("avtopro_price_feed", warehouse_id=warehouse.id,
                                                               token=private_key)) if private_key else ""})
            return render_template("admin_marketplaces.html", tab=tab, csrf_token=form_token, config=config,
                preview=bool(site.app.config.get("MARKETPLACE_PREVIEW")),
                telegram_configured=bool(telegram["bot_token"] and telegram["chat_id"]),
                counts={"products": links_count, "mapped": mapped_count, "ready": ready_count,
                        "issues": issues, "order_issues": order_issues,
                        "orders": db.query(func.count(OrderState.id)).scalar() or 0},
                last_run=last_run, product_rows=product_rows, order_rows=order_rows, page=page,
                total_filtered=total_filtered, search=search,
                events=db.query(Event).order_by(Event.id.desc()).limit(80).all() if tab in {"journal", "overview"} else [],
                feeds=feeds, feed_token=session.setdefault("avtopro_feed_form_token", secrets.token_urlsafe(32)),
                avtopro={"account": setting(db, "avtopro_account", "USA SHOP · 337792"),
                         "note": setting(db, "avtopro_note"), "key_set": bool(setting(db, "avtopro_key"))},
                news=[])
        finally:
            db.close()

    @app.get("/admin/marketplaces/parts")
    @site.admin_required
    def marketplace_parts_search():
        q = request.args.get("q", "").strip()[:100]
        if len(q) < 2:
            return jsonify(results=[])
        with site.SessionLocal() as db:
            parts = db.query(site.Part).filter(site.Part.is_deleted == False, or_(
                site.Part.part_number.ilike(f"%{q}%"), site.Part.name.ilike(f"%{q}%"))).limit(20).all()
            results = []
            for part in parts:
                warehouse = db.get(site.Warehouse, part.warehouse_id)
                results.append({"id": part.id, "label": f"{part.part_number} · {part.name[:60]} · "
                                f"{warehouse.name if warehouse else ''} · {available(part)} шт."})
            return jsonify(results=results)

    @app.before_request
    def protect_prom_inventory_actions():
        # Prom owns the inbound commercial state. Local fulfillment remains
        # available; cancellation and line edits must arrive through its API.
        if request.method != "POST" or not session.get("admin_auth"):
            return
        if request.endpoint in {"admin_order_cancel", "admin_order_edit", "admin_order_complete", "admin_order_send_to_packing"}:
            identifier = (request.view_args or {}).get("order_id")
            with site.SessionLocal() as db:
                order = db.get(site.Order, identifier) if identifier else None
                if order and order.external_source == "prom":
                    state = db.query(OrderState).filter_by(order_id=order.id).one_or_none()
                    commercial_edit = request.endpoint in {"admin_order_cancel", "admin_order_edit"}
                    blocked = not state or state.issue or (state.historical and not order.stock_reserved)
                    if commercial_edit or blocked:
                        flash("Змініть замовлення у Prom або спочатку перевірте його облік у розділі Маркетплейси", "error")
                        return redirect(url_for("admin_marketplaces", tab="orders"))

    return MODELS
