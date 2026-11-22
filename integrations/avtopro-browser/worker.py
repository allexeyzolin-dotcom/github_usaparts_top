"""One browser, one persistent seller session, one sequential polling loop."""

import fcntl
import html
import http.client
import json
import logging
import os
import re
import secrets
import signal
import threading
import time
from collections import Counter
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlsplit

from playwright.sync_api import Error as BrowserError
from playwright.sync_api import TimeoutError as BrowserTimeout
from playwright.sync_api import sync_playwright

from avtopro_status import is_new_sale


ORDERS = "https://avtopro.ua/orders/"
HOSTS = {"avtopro.ua", "www.avtopro.ua", "avto.pro", "www.avto.pro"}
ORDER_PATH = re.compile(r"/orders/([0-9]{3,20})/?")
KEY_FILE = Path("/run/secrets/site-key")
CAPTURE = Path(__file__).with_name("capture.js")
DATA = Path("/data")
LOGIN_ORIGIN = "https://avtopro.ua"
SALES_TAB = re.compile(r"^\s*(?:Продаж|Продажи)(?:\s|$)", re.I)
RESULTS = {"created", "updated", "unchanged", "cancelled", "return_review"}
LOG = logging.getLogger("avtopro")
LIST_SELECTOR = '.number a[href*="/orders/"]'
LIST_ROWS = """
selector => [...document.querySelectorAll(selector)].filter(a => a.getClientRects().length)
    .map(a => [a.href, a.closest(".order-list-order--mt-x2")?.querySelector(".status")?.innerText
                    || a.parentElement?.parentElement?.querySelector(".status")?.innerText || ""])
"""


class Pause(Exception):
    """An operator must check the session or the destination."""


class NeedsLogin(Pause):
    """The seller's browser session has expired."""


class RetryLater(Exception):
    """A temporary failure. Retry on the next interval."""


class UnreadableOrder(Exception):
    """A single card could not be read or accepted."""


def bounded_env(name, default, minimum, maximum):
    value = int(os.getenv(name, default))
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


def order_id(url):
    parsed = urlsplit(url)
    match = ORDER_PATH.fullmatch(parsed.path)
    if parsed.scheme == "https" and parsed.netloc.lower() in HOSTS and match:
        return match[1]
    return None


def credentials():
    try:
        values = json.loads((DATA / "credentials.json").read_text(encoding="utf-8"))
        if isinstance(values, dict):
            return values
    except (OSError, ValueError):
        pass
    return {}


class SiteClient:
    def __init__(self):
        origin = urlsplit(os.getenv("USAPARTS_ORIGIN", "https://usaparts.top"))
        if (origin.scheme != "https" or not origin.hostname or origin.username or origin.password
                or origin.path not in ("", "/") or origin.query or origin.fragment):
            raise ValueError("USAPARTS_ORIGIN must be an HTTPS origin")
        self.host = origin.hostname
        self.port = origin.port
        self.reported_session = None

    def request(self, method, path, payload=None):
        key = credentials().get("site_key", "")
        if not key and KEY_FILE.is_file():
            key = KEY_FILE.read_text().strip()
        if not isinstance(key, str) or not re.fullmatch(r"[A-Za-z0-9_-]{43}", key):
            raise Pause("Встановіть чинний ключ імпорту USAparts.top")
        connection = http.client.HTTPSConnection(self.host, self.port, timeout=15)
        try:
            body = json.dumps(payload, ensure_ascii=False).encode() if payload is not None else None
            connection.request(method, "/api/integrations/avtopro/" + path, body=body, headers={
                "Authorization": f"Bearer {key}", "Content-Type": "application/json", "Accept": "application/json",
            })
            response = connection.getresponse()
            if response.status in {401, 403}:
                raise Pause("USAparts.top відхилив ключ імпорту")
            if response.status == 404 or 300 <= response.status < 400:
                raise Pause("На USAparts.top потрібна актуальна версія імпорту Автопро")
            if response.status in {400, 422} and method == "POST":
                raise UnreadableOrder("USAparts.top відхилив дані картки")
            if response.status not in {200, 201}:
                raise RetryLater(f"USAparts.top повернув HTTP {response.status}")
            raw = response.read(256 * 1024 + 1)
            if len(raw) > 256 * 1024:
                raise Pause("Завелика відповідь USAparts.top")
            result = json.loads(raw)
            if not isinstance(result, dict):
                raise Pause("Невідомий формат відповіді USAparts.top")
            return result
        except (OSError, http.client.HTTPException, ValueError):
            raise RetryLater("Помилка зв’язку з USAparts.top") from None
        finally:
            connection.close()

    def watch(self, cursor, visible):
        query = urlencode({"after": cursor, "seen": ",".join(visible)})
        result = self.request("GET", f"orders?{query}")
        for field in ("ids", "known"):
            values = result.get(field)
            if not isinstance(values, list) or any(not isinstance(i, str) or not re.fullmatch(r"[0-9]{3,20}", i) for i in values):
                raise Pause("Невідомий формат списку замовлень USAparts.top")
        return result["ids"], set(result["known"])

    def submit(self, payload, *, existing_only):
        path = "order?sync_draft=1"
        if existing_only:
            path += "&existing_only=1"
        result = self.request("POST", path, payload).get("result")
        if result not in RESULTS:
            raise Pause("Невідомий результат імпорту USAparts.top")
        return result

    def report_session(self, mode):
        if mode == self.reported_session:
            return
        try:
            self.request("POST", "session", {"state": mode})
        except (Pause, RetryLater, UnreadableOrder):
            LOG.warning("Не вдалося оновити стан сесії в кабінеті USAparts.top")
        else:
            self.reported_session = mode


class Console:
    def __init__(self, data, interval=300):
        self.file = data / "mode"
        self.lock = threading.Lock()
        self.wake = threading.Event()
        self.stop = threading.Event()
        self.csrf = secrets.token_urlsafe(32)
        self.mode = "sync" if self.file.is_file() and self.file.read_text().strip() == "sync" else "paused"
        self.version = 0
        self.message = "Очікує ручного входу або перевірки"
        self.last_check = None
        self.counts = {}
        self.interval = interval

    def set_mode(self, mode, message, expected=None):
        with self.lock:
            if expected is not None and expected != self.version:
                return
            temporary = self.file.with_suffix(".tmp")
            temporary.write_text("sync\n" if mode == "sync" else "paused\n")
            temporary.replace(self.file)
            self.version += 1
            self.mode, self.message = mode, message
            self.wake.set()
        LOG.info("%s", message)

    def status(self):
        with self.lock:
            return {"mode": self.mode, "message": self.message, "lastCheck": self.last_check,
                    "intervalSeconds": self.interval, "counts": self.counts}

    def update(self, version, message, counts=None):
        with self.lock:
            if self.version != version:
                return
            self.message = message
            if counts is not None:
                self.counts = dict(counts)
                self.last_check = datetime.now(timezone.utc).isoformat(timespec="seconds")
        LOG.info("%s%s", message, f" {dict(counts)}" if counts is not None else "")

    def current(self, version):
        return not self.stop.is_set() and self.version == version


def console_handler(console):
    actions = {
        "login": "Відкрити Avto.pro для ручного входу",
        "preview": "Перевірити зчитування без передачі",
        "sync": "Увімкнути синхронізацію",
        "paused": "Призупинити",
    }

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def send_content(self, mime, body):
            content = body.encode()
            self.send_response(200)
            self.send_header("Content-Type", mime + "; charset=utf-8")
            self.send_header("Content-Length", str(len(content)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(content)

        def do_GET(self):
            if self.path == "/health":
                self.send_content("text/plain", "ok")
            elif self.path == "/status":
                self.send_content("application/json", json.dumps(console.status(), ensure_ascii=False))
            elif self.path == "/":
                buttons = "".join(f'<button name="mode" value="{mode}">{label}</button>' for mode, label in actions.items())
                status = html.escape(json.dumps(console.status(), ensure_ascii=False, indent=2))
                self.send_content("text/html", f"""<!DOCTYPE html><html lang="uk"><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Avto.pro · USAparts.top</title>
<style>body{{font:1rem system-ui;color:#173448;margin:7vh auto;padding:0 20px;max-width:740px}}
button{{padding:10px;margin:4px;border:1px solid #bcc9d0;border-radius:6px;font:inherit;cursor:pointer}}
pre{{white-space:pre-wrap;background:#edf3f6;padding:16px;border-radius:8px}} a{{color:#17567a}}</style>
<h1>Замовлення Avto.pro</h1><pre>{status}</pre>
<p><a href="/vnc/vnc.html?autoconnect=1&amp;resize=scale&amp;path=vnc/websockify"
target="_blank" rel="noopener">Відкрити захищений браузер</a> · <a href="/">Оновити стан</a></p>
<form method="post" action="/mode"><input type="hidden" name="csrf" value="{console.csrf}">{buttons}</form></html>""")
            else:
                self.send_error(404)

        def do_POST(self):
            if self.path != "/mode":
                self.send_error(404)
                return
            size = self.headers.get("Content-Length", "")
            if not size.isascii() or not size.isdecimal() or not 0 < int(size) <= 1024:
                self.send_error(400)
                return
            form = parse_qs(self.rfile.read(int(size)).decode("ascii", errors="replace"))
            mode = form.get("mode", [""])[0]
            if mode not in actions or not secrets.compare_digest(form.get("csrf", [""])[0].encode(), console.csrf.encode()):
                self.send_error(403)
                return
            console.set_mode(mode, actions[mode])
            self.send_response(303)
            self.send_header("Location", "/")
            self.end_headers()

    return Handler


def read_only(route):
    request = route.request
    url = urlsplit(request.url)
    if request.is_navigation_request() and (url.scheme != "https" or url.netloc.lower() not in HOSTS):
        route.abort("blockedbyclient")
    elif request.method not in {"GET", "HEAD"} or request.resource_type in {"image", "media", "font"}:
        route.abort()
    else:
        route.fallback()


def needs_login(page):
    return (re.search(r"/(?:login|signin|auth|captcha|unauthorized)(?:/|$)", urlsplit(page.url).path, re.I)
            or page.locator('input[type="password"]:visible, iframe[src*="captcha"]:visible, '
                            'iframe[src*="challenges.cloudflare.com"]:visible').count())


def login_challenge(page):
    return page.locator('input[autocomplete="one-time-code"]:visible, iframe[src*="challenges.cloudflare.com"]:visible, '
                        'iframe[title*="captcha" i]:visible, input[name*="captcha" i]:visible').count()


def automatic_login(browser):
    values = credentials()
    email, password = values.get("email"), values.get("password")
    if not isinstance(email, str) or not email or not isinstance(password, str) or not password:
        raise Pause("Налаштуйте дані входу Avto.pro в консолі контейнера")
    page = browser.new_page()
    permitted_submit = False

    def allow_login_request(route):
        request = route.request
        url = urlsplit(request.url)
        same_origin = f"{url.scheme}://{url.netloc.lower()}" == LOGIN_ORIGIN
        query = {name.casefold() for name in parse_qs(url.query, keep_blank_values=True)}
        if (request.method not in {"GET", "HEAD"} and not (permitted_submit and same_origin and request.method == "POST")
                or request.is_navigation_request() and request.frame == page.main_frame and not same_origin
                or query.intersection({"login", "password", "email", "passwd"})):
            route.abort()
        else:
            route.fallback()

    page.route("**/*", allow_login_request)
    page.route_web_socket("**/*", lambda socket: socket.close())
    try:
        response = page.goto(ORDERS, wait_until="domcontentloaded", timeout=20_000)
        if response and response.status in {429, 500, 502, 503, 504}:
            raise RetryLater(f"Вхід Avto.pro тимчасово недоступний (HTTP {response.status})")
        if login_challenge(page) or (response and response.status == 403):
            raise Pause("Avto.pro запитав додаткове підтвердження входу")
        if page.locator(".pro-navigation__link--active").filter(has_text=SALES_TAB).first.is_visible():
            return
        parsed = urlsplit(page.url)
        if f"{parsed.scheme}://{parsed.netloc.lower()}" != LOGIN_ORIGIN:
            raise Pause("Avto.pro перенаправив із дозволеної сторінки входу")
        user_field = page.locator('input[name="Login"]:visible')
        password_field = page.locator('input[name="Password"][type="password"]:visible')
        form = page.locator("form").filter(has=user_field).filter(has=password_field)
        submit = form.get_by_role("button", name=re.compile(r"^(?:Увійти|Войти)$", re.I))
        if form.count() != 1 or submit.count() != 1 or not submit.is_visible():
            raise Pause("Штатну форму входу Avto.pro не розпізнано")
        user_field.fill(email)
        password_field.fill(password)
        permitted_submit = True
        submit.click(timeout=8_000)
        await_sales = page.locator(".pro-navigation__link--active").filter(has_text=SALES_TAB).first
        try:
            await_sales.wait_for(state="visible", timeout=12_000)
        except BrowserTimeout:
            if login_challenge(page):
                raise Pause("Avto.pro запитав CAPTCHA або одноразовий код") from None
            # Some sign-in forms close after success without restoring the requested page.
            if password_field.is_visible():
                raise Pause("Avto.pro відхилив вхід або запросив додаткове підтвердження") from None
        permitted_submit = False
        sales_rows(page)
    except BrowserError:
        raise Pause("Штатний вхід Avto.pro не підтверджено") from None
    finally:
        page.close()


def open_page(page, url, *, card=False):
    try:
        response = page.goto(url, wait_until="domcontentloaded", timeout=20_000)
    except BrowserError as exc:
        if "ERR_BLOCKED_BY_CLIENT" in str(exc) or needs_login(page):
            raise NeedsLogin("Потрібен вхід або ручне підтвердження на Avto.pro") from None
        raise
    if not response:
        raise RetryLater("Avto.pro не повернув сторінку")
    if response.status in {401, 403} or needs_login(page):
        raise NeedsLogin("Потрібен вхід або ручне підтвердження на Avto.pro")
    if response.status == 429:
        raise Pause("Avto.pro обмежив запити (HTTP 429). Перевірте кабінет перед повторним запуском.")
    if card and response.status in {404, 410}:
        raise UnreadableOrder("Картка недоступна")
    if response.status >= 400:
        raise RetryLater(f"Avto.pro повернув HTTP {response.status}")
    if urlsplit(page.url).netloc.lower() not in HOSTS:
        raise NeedsLogin("Avto.pro перенаправив на сторінку підтвердження")
    return response


def sales_rows(page):
    open_page(page, ORDERS)
    try:
        page.locator(".pro-navigation__link--active").filter(has_text=SALES_TAB) \
            .first.wait_for(state="visible", timeout=10_000)
    except BrowserTimeout:
        if needs_login(page):
            raise NeedsLogin("Потрібен вхід або ручне підтвердження на Avto.pro") from None
        raise Pause("Не відкрився розділ «Продаж». Перевірте вхід і активну вкладку Avto.pro.") from None
    if urlsplit(page.url).path.rstrip("/") != "/orders":
        raise Pause("Avto.pro змінив адресу розділу «Продаж»")
    empty = page.get_by_text(re.compile(r"^(?:Немає замовлень|У вас (?:поки )?немає замовлень|Нет заказов)[.!]?$", re.I))
    try:
        page.locator(LIST_SELECTOR).filter(visible=True).first.or_(empty.filter(visible=True).first).first.wait_for(timeout=10_000)
    except BrowserTimeout:
        raise Pause("Не знайдено список продажів Avto.pro. Перевірте відкриту сторінку.") from None
    rows = [(ident, url, status.strip()) for url, status in page.evaluate(LIST_ROWS, LIST_SELECTOR)
            if (ident := order_id(url))]
    if not any(status for _, _, status in rows) and not empty.filter(visible=True).count():
        raise Pause("Не знайдено статуси продажів Avto.pro")
    return rows


def read_card(page, url, capture, *, tracking_only):
    open_page(page, url, card=True)
    try:
        page.locator("main .order-progressbar-status").first.wait_for(state="visible", timeout=6_000)
    except BrowserTimeout:
        if needs_login(page):
            raise NeedsLogin("Потрібне ручне підтвердження Avto.pro") from None
        raise UnreadableOrder("Не знайдено статус картки") from None
    payload = page.evaluate(f"(avtoproTrackingOnly) => ({capture.strip().removesuffix(';')})", tracking_only)
    if payload.get("orderId") != order_id(url) or not payload.get("status"):
        raise UnreadableOrder("Поля картки Avto.pro не розпізнано")
    return payload


def cycle(page, client, capture, batch, watch_cursor, sales_offset, preview, current):
    counts = Counter()
    rows = sales_rows(page)
    if not current():
        return watch_cursor, sales_offset, counts
    candidates = [(ident, url) for ident, url, status in rows if is_new_sale(status)]
    watched, known = ([], set()) if preview else client.watch(watch_cursor, [i for i, _ in candidates][:100])
    if not preview and current():
        client.report_session("active")
    # Keep older, already imported orders in rotation even when absent from the sales list.
    watched = watched[:batch]
    if not preview:
        watch_cursor = watched[-1] if len(watched) == batch else ""
    by_id = {ident: url for ident, url, _ in rows}
    tasks = [(ident, by_id.get(ident, f"{ORDERS}{ident}/"), True) for ident in watched]
    active = [(ident, url, False) for ident, url in candidates if ident not in known]
    if active:
        start = 0 if preview else sales_offset % len(active)
        tasks.extend((active[start:] + active[:start])[:batch])
        if not preview:
            sales_offset = (start + batch) % len(active)
    seen = set()
    for ident, url, existing in tasks:
        if not current():
            break
        if ident in seen:
            continue
        seen.add(ident)
        try:
            payload = read_card(page, url, capture, tracking_only=existing)
            if not current():
                break
            if not existing and (payload.get("statusOnly") or not is_new_sale(payload["status"])):
                counts["skipped"] += 1
            elif preview:
                counts["read"] += 1
            else:
                counts[client.submit(payload, existing_only=existing)] += 1
        except UnreadableOrder as exc:
            counts["review"] += 1
            LOG.warning("Замовлення %s. %s", ident, exc)
        if current():
            time.sleep(1)
    return watch_cursor, sales_offset, counts


def run(browser, console, client, capture, interval, batch):
    watch_cursor, sales_offset = "", 0
    while not console.stop.is_set():
        console.wake.clear()
        with console.lock:
            mode, version = console.mode, console.version
        if mode == "login":
            try:
                page = browser.pages[0] if browser.pages else browser.new_page()
                page.bring_to_front()
                page.goto(ORDERS, wait_until="domcontentloaded", timeout=20_000)
                message = "Вхід вручну через захищений браузер"
            except BrowserError:
                message = "Відкрийте захищений браузер і перевірте сторінку Avto.pro"
            console.set_mode("paused", message, expected=version)
        elif mode in {"preview", "sync"}:
            page = browser.new_page()
            page.route("**/*", read_only)
            page.route_web_socket("**/*", lambda socket: socket.close())
            try:
                watch_cursor, sales_offset, counts = cycle(page, client, capture, batch, watch_cursor,
                    sales_offset, mode == "preview", lambda: console.current(version))
                console.update(version, "Перевірку завершено", counts)
                if mode == "preview":
                    console.set_mode("paused", "Попередню перевірку завершено", expected=version)
            except NeedsLogin:
                if console.current(version):
                    try:
                        automatic_login(browser)
                    except Pause as exc:
                        console.set_mode("paused", str(exc), expected=version)
                        if mode == "sync":
                            client.report_session("login_required")
                    except RetryLater as exc:
                        if mode == "preview":
                            console.set_mode("paused", str(exc), expected=version)
                        else:
                            console.update(version, str(exc))
                    else:
                        console.update(version, "Сесію Avto.pro відновлено. Продовжую опитування")
                        console.wake.set()
            except Pause as exc:
                console.set_mode("paused", str(exc), expected=version)
            except (RetryLater, BrowserError) as exc:
                message = str(exc) if isinstance(exc, RetryLater) else "Навігація Avto.pro перервалася"
                if mode == "preview":
                    console.set_mode("paused", message, expected=version)
                else:
                    console.update(version, message)
            finally:
                page.close()
        console.wake.wait(interval if mode == "sync" else None)


def launch_browser(playwright, profile):
    return playwright.chromium.launch_persistent_context(
        profile, executable_path="/usr/bin/chromium", headless=os.getenv("AVTOPRO_HEADLESS") == "1",
        chromium_sandbox=True, args=["--disable-setuid-sandbox", "--no-first-run", "--noerrdialogs"],
        handle_sigint=False, handle_sigterm=False, handle_sighup=False,
        service_workers="block", accept_downloads=False, no_viewport=True,
    )


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    interval = bounded_env("AVTOPRO_INTERVAL_SECONDS", 300, 60, 86400)
    batch = bounded_env("AVTOPRO_BATCH_SIZE", 10, 1, 20)
    capture = CAPTURE.read_text(encoding="utf-8")
    client = SiteClient()
    with (DATA / "worker.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        console = Console(DATA, interval)

        def terminate(*_):
            console.stop.set()
            console.wake.set()

        signal.signal(signal.SIGTERM, terminate)
        signal.signal(signal.SIGINT, terminate)
        with sync_playwright() as playwright:
            browser = launch_browser(playwright, DATA / "chromium")
            server = ThreadingHTTPServer(("127.0.0.1", 8090), console_handler(console))
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                run(browser, console, client, capture, interval, batch)
            finally:
                server.shutdown()
                server.server_close()
                browser.close()


if __name__ == "__main__":
    main()
