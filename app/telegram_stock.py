"""Authorized Telegram stock commands, durable receipts, and targeted Prom sync."""
import json
import re
import secrets
import threading
import time
from datetime import datetime, timedelta
from types import SimpleNamespace

import requests
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint, and_, or_, text

from .prom_api import sku_key

SITE = None
MODELS = None
STARTED = False
POLL_LOCK = threading.Lock()
HELP = "Команди складу:\n+АРТИКУЛ 2 — додати 2 шт.\n-АРТИКУЛ 2 — списати 2 шт.\nБез кількості змінюється 1 шт."
MAX_QTY = 2147483647


class CommandError(ValueError):
    pass


def parse_command(value):
    value = str(value or "").strip()
    if not value.startswith(("+", "-")):
        return None
    match = re.fullmatch(r"([+-])\s*([^\s+-][^\s+]*)(?:\s+([0-9]{1,10}))?", value)
    if not match or len(match[2]) > 255 or not sku_key(match[2]):
        raise CommandError("Невірна команда.\n" + HELP)
    count = int(match[3] or 1)
    if not 1 <= count <= MAX_QTY:
        raise CommandError("Кількість має бути цілим числом від 1 до 2147483647.")
    return match[2], count if match[1] == "+" else -count


def settings(db):
    values = SITE.get_api_settings_map(db)
    return {
        "enabled": values.get("telegram_stock_enabled") == "1",
        "token": values.get("telegram_bot_token", "").strip(),
        "chat_id": values.get("telegram_chat_id", "").strip(),
        "started_at": int(values.get("telegram_stock_started_at", "0") or 0),
    }


def api(token, method, payload=None, *, timeout=20):
    try:
        response = requests.post("https://api.telegram.org/bot" + token + "/" + method,
                                 json=payload or {}, timeout=timeout)
        result = response.json()
    except (requests.RequestException, ValueError):
        # requests errors contain the request URL, including the bot credential.
        raise CommandError("Telegram тимчасово недоступний") from None
    if response.status_code >= 400 or not result.get("ok"):
        code = result.get("error_code", response.status_code)
        error = CommandError("Telegram не підтвердив запит (код " + str(code) + ")")
        error.status = code
        error.retry_after = (result.get("parameters") or {}).get("retry_after", 0)
        raise error
    return result.get("result")


def remember_chat(db, message):
    chat = message.get("chat") or {}
    if chat.get("id") is None:
        return
    try:
        recent = json.loads(SITE.get_api_settings_map(db).get("telegram_stock_recent_chats", "[]"))
    except (ValueError, TypeError):
        recent = []
    identifier = str(chat["id"])
    recent = [row for row in recent if row.get("id") != identifier][:24]
    recent.insert(0, {"id": identifier, "type": chat.get("type", ""),
                      "title": str(chat.get("title") or chat.get("username") or
                                   chat.get("first_name") or "Без назви")[:255]})
    SITE.set_setting(db, "telegram_stock_recent_chats", json.dumps(recent, ensure_ascii=False))


def recent_chats(db):
    try:
        return json.loads(SITE.get_api_settings_map(db).get("telegram_stock_recent_chats", "[]"))
    except (ValueError, TypeError):
        return []


def matching_parts(db, identifier):
    key = sku_key(identifier)
    # Exact normalized OEMs only; a substring must never identify stock to deduct.
    rows = db.query(SITE.Part).filter_by(is_deleted=False).order_by(SITE.Part.id).all()
    return [part for part in rows if key in {sku_key(part.part_number), sku_key(part.part_number_export)} - {""}]


def current_text(command, db):
    part = db.get(SITE.Part, command.part_id) if command.part_id else None
    warehouse = db.get(SITE.Warehouse, part.warehouse_id) if part else None
    lines = ["✅ Залишок змінено", "", "Артикул: " + (part.part_number if part else command.identifier)]
    if part:
        lines.append("Товар: " + str(part.name or "Без назви")[:400])
    if warehouse:
        lines.append("Склад: " + warehouse.name)
    if SITE.location_line(part):
        lines.append(SITE.location_line(part))
    lines.extend([f"Було: {command.before_qty} шт.", f"Зміна: {command.delta:+d} шт.",
                  f"Після команди: {command.after_qty} шт."])
    if part:
        qty = max(int(part.qty or 0), 0)
        if qty != command.after_qty:
            lines.append(f"Поточний залишок на сайті: {qty} шт.")
        lines.append("Наявність: " + ("в наявності" if part.in_stock and qty else "немає в наявності"))
    lines.append("Prom: " + (command.prom_message or "очікує передачі"))
    return "\n".join(lines)


def apply_to_part(db, command, part_id):
    part = (db.query(SITE.Part).filter_by(id=part_id, is_deleted=False)
            .populate_existing().with_for_update().one_or_none())
    if not part or sku_key(command.identifier) not in {
            sku_key(part.part_number), sku_key(part.part_number_export)}:
        raise CommandError("Товар більше не доступний за цим артикулом. Залишок не змінено.")
    before = max(int(part.qty or 0), 0)
    after = before + command.delta
    if after < 0:
        raise CommandError(f"Недостатньо товару. На складі {before} шт., запитано {abs(command.delta)} шт. Залишок не змінено.")
    if after > MAX_QTY:
        raise CommandError("Занадто великий залишок. Залишок не змінено.")
    command.part_id, command.before_qty, command.after_qty = part.id, before, after
    command.status, command.prom_status = "applied", "pending"
    command.prom_message = "очікує передачі"
    command.reply_markup = ""
    command.reply_sent = 0
    part.qty, part.in_stock, part.updated_at = after, after > 0, datetime.utcnow()
    db.add(SITE.NewsFeed(source="telegram", title="Залишок змінено через Telegram",
                        body=f"{part.part_number}: {before} → {after} шт. Склад #{part.warehouse_id}. Команда #{command.id}.",
                        severity="info"))


def handle_update(db, update, bot_id, config):
    """Commit a command and its quantity in one transaction; delivery is separate."""
    message = update.get("message")
    callback = update.get("callback_query")
    if not config["enabled"] or not config["chat_id"]:
        return None
    if callback:
        return handle_selection(db, callback, bot_id, config)
    if not message:
        return None  # Editing or forwarding a past command cannot change stock.
    remember_chat(db, message)
    sender = message.get("from") or {}
    if (str((message.get("chat") or {}).get("id")) != config["chat_id"] or
            not sender.get("id") or sender.get("is_bot") or message.get("sender_chat") or
            message.get("forward_origin") or message.get("forward_date") or
            int(message.get("date", 0)) < config["started_at"]):
        return None
    body = str(message.get("text") or "").strip()
    if not body.startswith(("+", "-")) and body.split("@", 1)[0] not in {"/start", "/help", "/stock"}:
        return None
    identifier = int(update["update_id"])
    existing = db.query(MODELS.Command).filter_by(bot_id=str(bot_id), update_id=identifier).one_or_none()
    if existing:
        return existing.id
    command = MODELS.Command(bot_id=str(bot_id), update_id=identifier,
                             chat_id=config["chat_id"], sender_id=str(sender["id"]),
                             message_id=int(message["message_id"]), nonce=secrets.token_urlsafe(12))
    db.add(command)
    db.flush()
    try:
        parsed = parse_command(body)
        if parsed is None:
            command.status, command.reply_text = "help", HELP
            return command.id
        command.identifier, command.delta = parsed
        parts = matching_parts(db, command.identifier)
        if not parts:
            raise CommandError("Товар за артикулом " + command.identifier + " не знайдено на складах. Залишок не змінено.")
        if len(parts) > 1:
            if len(parts) > 50:
                raise CommandError("Артикул знайдено у понад 50 позиціях. Уточніть товар у кабінеті сайту; залишок не змінено.")
            command.status = "select"
            command.candidates = json.dumps([part.id for part in parts])
            buttons = []
            for part in parts:
                warehouse = db.get(SITE.Warehouse, part.warehouse_id)
                label = f"{warehouse.name if warehouse else 'Склад'} · {part.qty} шт. · #{part.id}"
                buttons.append([{"text": label[:64], "callback_data": f"stk:{command.nonce}:{part.id}"}])
            command.reply_markup = json.dumps({"inline_keyboard": buttons}, ensure_ascii=False)
            command.reply_text = "Артикул " + command.identifier + " знайдено в кількох позиціях. Обери склад для зміни " + f"{command.delta:+d} шт."
        else:
            apply_to_part(db, command, parts[0].id)
    except CommandError as error:
        command.status, command.reply_text = "error", str(error)
    return command.id


def handle_selection(db, callback, bot_id, config):
    message = callback.get("message") or {}
    sender = callback.get("from") or {}
    if (str((message.get("chat") or {}).get("id")) != config["chat_id"] or
            not sender.get("id") or sender.get("is_bot")):
        return None
    match = re.fullmatch(r"stk:([A-Za-z0-9_-]{16}):([0-9]+)", str(callback.get("data") or ""))
    if not match:
        return None
    command = (db.query(MODELS.Command).filter_by(bot_id=str(bot_id), nonce=match[1],
               chat_id=config["chat_id"], sender_id=str(sender["id"]))
               .populate_existing().with_for_update().one_or_none())
    if not command or command.status != "select":
        return None
    if command.created_at < datetime.utcnow() - timedelta(hours=1):
        command.status, command.reply_text, command.reply_sent = "error", "Вибір складу застарів. Надішли команду ще раз; залишок не змінено.", 0
        command.reply_markup = ""
        return command.id
    part_id = int(match[2])
    if part_id not in json.loads(command.candidates):
        return None
    try:
        apply_to_part(db, command, part_id)
    except CommandError as error:
        command.status, command.reply_text, command.reply_sent = "error", str(error), 0
        command.reply_markup = ""
    return command.id


def finish_command(identifier, config):
    with SITE.SessionLocal() as db:
        command = db.get(MODELS.Command, identifier)
        if not command or command.chat_id != config["chat_id"]:
            return
        if command.part_id and command.status == "applied" and command.prom_status != "confirmed":
            result = SITE.marketplaces.sync_part_stock(command.part_id, expected_before=command.before_qty)
            command.prom_status, command.prom_message = result["status"], result["message"]
            command.next_attempt = datetime.utcnow() + timedelta(seconds=30)
            if result["status"] == "confirmed" and command.reply_sent:
                # A later retry is a follow-up, never a second inventory movement.
                command.reply_sent = 0
            db.commit()
        if command.reply_sent:
            return
        message = current_text(command, db) if command.status == "applied" else command.reply_text
        payload = {"chat_id": command.chat_id, "text": message, "disable_web_page_preview": True,
                   "entities": SITE.location_entities(message),
                   "reply_parameters": {"message_id": command.message_id, "allow_sending_without_reply": True}}
        if command.reply_markup:
            payload["reply_markup"] = json.loads(command.reply_markup)
        sent = api(config["token"], "sendMessage", payload)
        command.reply_sent = 1
        command.reply_message_id = int(sent.get("message_id", 0))
        db.commit()


def consume_batch(updates, bot_id, config):
    """Persist each update before acknowledging it to Telegram with an offset."""
    for update in updates:
        with SITE.SessionLocal() as db:
            command_id = handle_update(db, update, bot_id, config)
            SITE.set_setting(db, "telegram_stock_offset:" + str(bot_id), str(int(update["update_id"]) + 1))
            db.commit()
        SITE.public_cache_clear()
        if update.get("callback_query"):
            try:
                api(config["token"], "answerCallbackQuery", {"callback_query_id": update["callback_query"]["id"]})
            except CommandError:
                pass
        if command_id:
            try:
                finish_command(command_id, config)
            except CommandError:
                pass  # The durable reply remains unsent and will be retried.


def retry_pending(bot_id, config):
    with SITE.SessionLocal() as db:
        command = MODELS.Command
        rows = (db.query(command.id).filter_by(bot_id=str(bot_id), chat_id=config["chat_id"])
                .filter(or_(command.reply_sent == 0, and_(command.status == "applied", command.part_id.isnot(None),
                    command.prom_status != "confirmed", or_(command.next_attempt.is_(None),
                    command.next_attempt <= datetime.utcnow()))))
                .order_by(command.id).limit(10).all())
        ids = [row.id for row in rows]
    for identifier in ids:
        try:
            finish_command(identifier, config)
        except CommandError:
            pass


def poll_once():
    if not POLL_LOCK.acquire(blocking=False):
        return
    connection = None
    try:
        if SITE.engine.dialect.name == "postgresql":
            connection = SITE.engine.connect()
            if not connection.execute(text("SELECT pg_try_advisory_lock(77303272)")).scalar():
                return
        with SITE.SessionLocal() as db:
            config = settings(db)
        if not config["enabled"] or not config["token"] or not config["chat_id"]:
            return
        me = api(config["token"], "getMe")
        webhook = api(config["token"], "getWebhookInfo")
        if webhook.get("url"):
            raise CommandError("Для цього бота вже налаштований webhook. Потрібно узгодити спосіб отримання команд.")
        bot_id = str(me["id"])
        with SITE.SessionLocal() as db:
            values = SITE.get_api_settings_map(db)
            offset = int(values.get("telegram_stock_offset:" + bot_id, "0") or 0)
        updates = api(config["token"], "getUpdates", {"offset": offset, "limit": 100, "timeout": 15,
                     "allowed_updates": ["message", "callback_query", "my_chat_member"]}, timeout=25) or []
        # A cabinet change while long polling is in flight must revoke the old chat.
        with SITE.SessionLocal() as db:
            current = settings(db)
        if current != config:
            return
        consume_batch(updates, bot_id, config)
        retry_pending(bot_id, config)
        with SITE.SessionLocal() as db:
            SITE.set_setting(db, "telegram_stock_last_poll", datetime.utcnow().isoformat())
            SITE.set_setting(db, "telegram_stock_error", "")
            db.commit()
    finally:
        if connection:
            connection.execute(text("SELECT pg_advisory_unlock(77303272)"))
            connection.close()
        POLL_LOCK.release()


def worker_loop():
    while True:
        try:
            poll_once()
        except Exception as error:
            message = str(error) if isinstance(error, CommandError) else "Не вдалося обробити команду. Перевірте журнал сервера."
            SITE.app.logger.warning("Telegram stock worker failed (%s)", type(error).__name__)
            try:
                with SITE.SessionLocal() as db:
                    SITE.set_setting(db, "telegram_stock_error", message)
                    db.commit()
            except Exception:
                pass
        time.sleep(2)


def start_worker():
    global STARTED
    if not STARTED:
        threading.Thread(target=worker_loop, name="telegram-stock", daemon=True).start()
        STARTED = True


def install(site):
    global SITE, MODELS
    SITE = site

    class StockCommand(site.Base):
        __tablename__ = "telegram_stock_commands"
        __table_args__ = (UniqueConstraint("bot_id", "update_id"),)
        id = Column(Integer, primary_key=True)
        bot_id = Column(String(64), nullable=False)
        update_id = Column(Integer, nullable=False)
        chat_id = Column(String(64), nullable=False)
        sender_id = Column(String(64), nullable=False)
        message_id = Column(Integer, nullable=False)
        nonce = Column(String(32), unique=True, nullable=False)
        identifier = Column(String(255), default="", nullable=False)
        delta = Column(Integer, default=0, nullable=False)
        part_id = Column(Integer, ForeignKey("parts.id"), nullable=True)
        before_qty = Column(Integer, nullable=True)
        after_qty = Column(Integer, nullable=True)
        status = Column(String(32), default="new", nullable=False)
        candidates = Column(Text, default="[]", nullable=False)
        prom_status = Column(String(32), default="", nullable=False)
        prom_message = Column(Text, default="", nullable=False)
        reply_text = Column(Text, default="", nullable=False)
        reply_markup = Column(Text, default="", nullable=False)
        reply_sent = Column(Integer, default=0, nullable=False)
        reply_message_id = Column(Integer, default=0, nullable=False)
        next_attempt = Column(DateTime, nullable=True)
        created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    MODELS = SimpleNamespace(Command=StockCommand)
    return MODELS
