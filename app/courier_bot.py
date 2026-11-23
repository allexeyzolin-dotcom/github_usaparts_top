"""Private courier bot. Polling and dispatch notices share the site's delivery queue."""
import logging
import re
import threading

from sqlalchemy import or_

from . import courier as c
from .telegram_stock import CommandError, api

LOG = logging.getLogger(__name__)
START_LOCK = threading.Lock()
STARTED = False


def app_button(order_id=None):
    page = "https://usaparts.top/courier/"
    return {"text": "Відкрити відправлення" if order_id else "Додаток кур’єра",
            "web_app": {"url": page + (f"?order={order_id}" if order_id else "")}}


def summary(data):
    lines = [f"Відправлення №{data['id']} · Нова пошта",
             f"Отримувач: {data['name'] or 'Не вказано'}", f"Телефон: {data['phone'] or 'Не вказано'}",
             f"{data['service']}: {data['address'] or 'Уточніть у менеджера'}",
             "Контроль оплати: " + data["payment"] + " грн",
             f"Комплектація · {len(data['items'])} поз., {sum(i['qty'] for i in data['items'])} шт."]
    for index, item in enumerate(data["items"], 1):
        block = (f"\n{index}. OEM {item['oem']} × {item['qty']} шт.\n{item['name']}\n"
                 f"Склад: {item['warehouse'] or 'не вказано'} · Місце: {item['location'] or 'уточніть у менеджера'}")
        if len("\n".join(lines)) + len(block) > 3250:
            lines.append("Решта позицій — у повній картці.")
            break
        lines.append(block)
    if data["ttn"]:
        lines.append("Архів · ТТН " + data["ttn"])
    return "\n".join(lines)


def keyboard(data):
    order_id = data["id"]
    buttons = [[{"text": "Товари та фото", "callback_data": f"items:{order_id}:0"}]]
    button = app_button(order_id)
    if not data["ttn"]:
        button["text"] = "Додати ТТН · сканер"
        button["web_app"]["url"] += "&scan=1"
    buttons.append([button])
    return {"inline_keyboard": buttons}


def send_list(db, token, chat, archive=False, page=0):
    rows = c.queue(db, archive).offset(page * 8).limit(9).all()
    buttons = [[{"text": f"№{o.id} · {o.customer_name or o.city or 'Відправлення'}"[:80],
                 "callback_data": f"order:{o.id}"}] for o, _ in rows[:8]]
    tab = "archive" if archive else "active"
    nav = []
    if page:
        nav.append({"text": "← Назад", "callback_data": f"{tab}:{page-1}"})
    if len(rows) > 8:
        nav.append({"text": "Далі →", "callback_data": f"{tab}:{page+1}"})
    if nav:
        buttons.append(nav)
    buttons.extend([[{"text": "До відправки" if archive else "Архів",
                      "callback_data": "active:0" if archive else "archive:0"}], [app_button()]])
    title = "Архів відправлень" if archive else "До відправки"
    api(token, "sendMessage", {"chat_id": chat, "text": title if rows else title + " · порожньо",
                              "reply_markup": {"inline_keyboard": buttons}, "protect_content": True})


def process(db, cfg, token, update):
    cb = update.get("callback_query") or {}
    message = cb.get("message") or update.get("message") or {}
    chat = message.get("chat") or {}
    user = (cb.get("from") or message.get("from") or {}).get("id")
    if chat.get("type") != "private" or not user or chat.get("id") != user:
        return
    identifier = str(user)
    cmd = str(message.get("text") or "")
    if not cb and cmd.startswith("/start ") and c.pair(cfg, cmd[7:].strip(), "tg"):
        cfg.chat_id = identifier
        db.flush()
    if cb:
        api(token, "answerCallbackQuery", {"callback_query_id": cb["id"]})
    if cfg.chat_id != identifier:
        if cmd.startswith("/start"):
            api(token, "sendMessage", {"chat_id": user, "text": "Отримайте у менеджера особисте посилання для підключення кур’єра."})
        return
    action = cb.get("data", "")
    nav = re.fullmatch(r"(active|archive):([0-9]{1,6})", action)
    if not cb or nav:
        send_list(db, token, user, nav[1] == "archive" if nav else cmd == "/archive", int(nav[2]) if nav else 0)
        return
    match = re.fullmatch(r"(items|order):([0-9]{1,10})(?::([0-9]{1,6}))?", action)
    if not match:
        return
    order_id = int(match[2])
    dispatch = db.get(c.MODELS.Dispatch, order_id)
    order = db.get(c.SITE.Order, order_id) if dispatch else None
    if not order or (not order.ttn and c.is_closed(order)):
        api(token, "sendMessage", {"chat_id": user, "text": f"Відправлення №{order_id} більше не активне."})
        return
    data = c.card(db, order, dispatch)
    if match[1] == "order":
        api(token, "sendMessage", {"chat_id": user, "text": summary(data), "reply_markup": keyboard(data), "protect_content": True})
        return
    index = int(match[3] or 0)
    if index >= len(data["items"]):
        return
    item = data["items"][index]
    caption = (f"№{order_id} · {data['name']} · позиція {index+1} з {len(data['items'])}\n"
               f"OEM {item['oem']} · {item['qty']} шт.\n{item['name']}\n"
               f"Склад: {item['warehouse'] or 'не вказано'}\nМісце: {item['location'] or 'уточніть у менеджера'}\n"
               + (item["description"] or ""))[:1000]
    buttons = []
    if index + 1 < len(data["items"]):
        buttons.append([{"text": "Наступний товар →", "callback_data": f"items:{order_id}:{index+1}"}])
    buttons.append([app_button(order_id)])
    payload = {"chat_id": user, "reply_markup": {"inline_keyboard": buttons}, "protect_content": True}
    if item["photo"]:
        try:
            api(token, "sendPhoto", {**payload, "photo": c.SITE.absolute_public_url(item["photo"]), "caption": caption})
            return
        except CommandError:
            pass
    api(token, "sendMessage", {**payload, "text": caption + "\nФото недоступне."})


def notices(db, cfg, token):
    O, D = c.SITE.Order, c.MODELS.Dispatch
    rows = (db.query(O, D).join(D, D.order_id == O.id)
            .filter(or_(D.notice_version.is_(None), D.notice_version != "closed"))
            .order_by(D.created_at).all())
    recipient = cfg.chat_id
    for order, dispatch in rows:
        cfg = c.config(db, True)
        if not cfg.chat_id or cfg.chat_id != recipient or token != c.bot_token(cfg):
            return
        closed = c.is_closed(order)
        same_chat = dispatch.notified_chat == cfg.chat_id and dispatch.notice_id
        if closed and not same_chat:
            dispatch.notice_version = "closed"
            db.commit()
            continue
        data = c.card(db, order, dispatch)
        state = "closed" if closed else data["version"]
        if same_chat and dispatch.notice_version == state:
            continue
        content = (f"Замовлення №{order.id} знято з доставки." if closed and not order.ttn else summary(data))
        buttons = {"inline_keyboard": []} if closed and not order.ttn else keyboard(data)
        payload = {"chat_id": cfg.chat_id, "text": content, "reply_markup": buttons}
        if same_chat:
            try:
                api(token, "editMessageText", {**payload, "message_id": dispatch.notice_id})
            except CommandError as exc:
                if "400" not in str(exc):
                    raise
                # A deleted or no-longer-editable card needs a fresh status message.
                same_chat = False
        if not same_chat:
            msg = api(token, "sendMessage", {**payload, "protect_content": True})
            dispatch.notice_id = msg["message_id"]
            dispatch.notified_chat = cfg.chat_id
        dispatch.notice_version = state
        db.commit()


def cycle():
    with c.SITE.SessionLocal() as db:
        cfg = c.config(db)
        db.commit()
        if not cfg.bot_token:
            return
        token = c.bot_token(cfg)
        updates = api(token, "getUpdates", {"offset": cfg.bot_offset or 0, "limit": 25,
                      "allowed_updates": ["message", "callback_query"], "timeout": 0})
        for update in updates:
            cfg = c.config(db, True)
            if token != c.bot_token(cfg):
                return
            if update["update_id"] >= (cfg.bot_offset or 0):
                try:
                    process(db, cfg, token, update)
                except CommandError as exc:
                    # Expired callback queries and users who blocked the bot must
                    # not prevent delivery of later updates.
                    if not any(code in str(exc) for code in ("400", "403")):
                        raise
                cfg.bot_offset = update["update_id"] + 1
            db.commit()
        if cfg.chat_id:
            notices(db, cfg, token)
        if cfg.bot_error:
            cfg.bot_error = ""
            db.commit()


def worker():
    wait = threading.Event()
    while True:
        try:
            cycle()
            seconds = 8
        except Exception as exc:
            # Transport exceptions can contain credentials. Only sanitized API errors reach the cabinet.
            message = str(exc) if isinstance(exc, CommandError) else "Помилка обробки Telegram. Перевірте підключення."
            LOG.warning("Courier bot cycle failed (%s)", type(exc).__name__)
            try:
                with c.SITE.SessionLocal.begin() as db:
                    c.config(db).bot_error = message
            except Exception:
                pass
            seconds = 30
        wait.wait(seconds)


def start():
    global STARTED
    with START_LOCK:
        if not STARTED:
            threading.Thread(target=worker, daemon=True, name="courier-bot").start()
            STARTED = True
