"""Container smoke test. Every HTTPS page is served by a local Playwright fixture."""

import sys
import threading
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import parse_qs, urlsplit
from unittest.mock import patch

from playwright.sync_api import sync_playwright

from worker import CAPTURE, Console, Pause, automatic_login, cycle, launch_browser, read_only, run, sales_rows


LIST = """
<nav><a class="pro-navigation__link--active">Продаж 2</a></nav>
<div class="order-list-order--mt-x2">
  <span class="number"><a href="/orders/10002/">10002</a></span>
  <span class="status">Замовлення формується</span>
</div>
<div class="order-list-order--mt-x2">
  <span class="number"><a href="/orders/10003/">10003</a></span>
  <span class="status">Завершено</span>
</div>
<div class="order-list-order--mt-x2">
  <span class="number"><a href="/orders/10004/">10004</a></span>
  <span class="status">Чекає отримання</span>
</div>
<div class="order-list-order--mt-x2">
  <span class="number"><a href="/orders/10005/">10005</a></span>
  <span class="status">Очікує на підтвердження продавцем</span>
</div>
"""
CANCELLED = """
<main><h1>Замовлення 10001</h1>
<div class="order-progressbar-status">Замовлення 10001 скасовано покупцем</div>
<dl><dt>ПІБ покупця</dt><dd>Тест</dd></dl></main>
"""
CARD = """
<main><h1>Замовлення 10002</h1>
<div class="order-progressbar-status">Замовлення формується</div>
<dl><dt>ПІБ покупця</dt><dd><div class="wrapper-name"><div><span>Тест</span></div></div></dd>
<dt>Сума замовлення покупця</dt><dd><span class="nowrap">1 200,00 грн</span></dd>
<dt>Умови доставки</dt><dd><span class="text-delivery">Нова пошта, відділення 12</span></dd>
<dt>Спосіб оплати</dt><dd>При отриманні</dd></dl>
<div class="item">
  <span class="product"><a class="brand-and-code"><span class="brand">VAG</span> TEST-123</a></span>
  <span class="product__info-name">Тестова деталь</span>
  <span class="info__quantity">2 шт.</span><span class="total-sum-per-one">600 грн</span>
</div></main>
"""
TRACKING = """
<main><h1>Замовлення 10005</h1>
<div class="order-progressbar-status">Замовлення очікує доставки або отримання</div>
<dl><dt>ПІБ покупця</dt><dd>Тест</dd></dl>
<div class="thread__post">
  <span class="thread__post__avatar pro-image--robot">Автопро</span>
  <div class="thread__post__body"><div>ТТН 20400000000001</div></div>
</div>
<div class="thread__post"><span class="thread__post__avatar">Покупець</span>
  <div class="thread__post__body">ТТН 20400000000002</div>
</div>
</main>
"""
LOGIN = """
<form>
  <input name="Login" placeholder="Email">
  <input type="password" name="Password" placeholder="Пароль">
  <button type="submit">Увійти</button>
</form>
<script>
document.querySelector("form").onsubmit = async (event) => {
  event.preventDefault();
  const response = await fetch("/fixture-auth/", {
    method: "POST", body: new URLSearchParams(new FormData(event.target))
  });
  if (response.ok) location.reload();
}
</script>
"""


class TestDestination:
    def __init__(self):
        self.transfers = []
        self.session_reports = []
        self.stop = None

    def watch(self, cursor, candidates):
        assert cursor == ""
        assert candidates == ["10002", "10005"], candidates
        return ["10001", "10005"], {"10005"}

    def report_session(self, state):
        self.session_reports.append(state)
        if state == "login_required" and self.stop:
            self.stop()

    def submit(self, payload, *, existing_only):
        self.transfers.append((payload, existing_only))
        if payload["orderId"] == "10001":
            return "cancelled"
        return "updated" if existing_only else "created"


def fixture(route):
    request = route.request
    url = urlsplit(request.url)
    if url.hostname != "avtopro.ua":
        route.abort()
        return
    if url.path == "/fixture-auth/" and request.method == "POST":
        assert parse_qs(request.post_data) == {"Login": ["seller@example.invalid"], "Password": ["fixture-only"]}
        route.fulfill(headers={"Set-Cookie": "fixture=yes; Path=/; HttpOnly; Secure; Max-Age=86400"})
        return
    if request.method != "GET":
        route.abort()
        return
    if url.path == "/fixture-login/":
        route.fulfill(body="Signed in", headers={"Set-Cookie": "fixture=yes; Path=/; HttpOnly; Secure; Max-Age=86400"})
        return
    if "fixture=yes" not in request.all_headers().get("cookie", ""):
        route.fulfill(status=401, body=LOGIN, content_type="text/html; charset=utf-8")
        return
    assert url.path not in {"/orders/10003/", "/orders/10004/"}, "Untracked past sale was opened"
    pages = {"/orders/": LIST, "/orders/10001/": CANCELLED, "/orders/10002/": CARD, "/orders/10005/": TRACKING}
    route.fulfill(body=pages.get(url.path, "missing"), status=200 if url.path in pages else 404, content_type="text/html; charset=utf-8")


def main(phase):
    with sync_playwright() as playwright:
        browser = launch_browser(playwright, Path("/data/fixture"))
        try:
            browser.route("**/*", fixture)
            page = browser.pages[0]
            if phase == "login":
                page.goto("https://avtopro.ua/fixture-login/")
                assert sales_rows(page)[0][0] == "10002"
            elif phase == "verify":
                page.route("**/*", read_only)
                page.route_web_socket("**/*", lambda socket: socket.close())
                destination = TestDestination()
                capture = CAPTURE.read_text(encoding="utf-8")
                _, _, preview = cycle(page, destination, capture, 1, "", 0, True, lambda: True)
                assert preview == {"read": 1} and not destination.transfers
                _, _, sync = cycle(page, destination, capture, 10, "", 0, False, lambda: True)
                assert sync == {"created": 1, "cancelled": 1, "updated": 1}, sync
                (cancellation, existing), (tracking, watched), (card, new) = destination.transfers
                assert existing and cancellation["statusOnly"] and cancellation["orderId"] == "10001"
                assert watched and tracking["statusOnly"] and tracking["waybill"] == "20400000000001"
                assert "items" not in tracking and "customer" not in tracking
                assert not new and card["orderId"] == "10002" and card["totalUah"] == "1200.00"
                assert card["delivery"] == "Нова пошта, відділення 12"
                assert card["items"] == [{"partNumber": "TEST-123", "brand": "VAG", "name": "Тестова деталь",
                                         "qty": 2, "unitUah": "600"}], card["items"]
                assert page.evaluate("fetch(location.href, {method:'POST'}).then(() => false, () => true)")
                browser.clear_cookies()
                with patch("worker.credentials", return_value={"email": "seller@example.invalid", "password": "fixture-only"}):
                    automatic_login(browser)
                    assert sales_rows(page)[0][0] == "10002"
                    browser.clear_cookies()
                    challenge_url = "**/orders/"
                    browser.route(challenge_url, lambda route: route.fulfill(
                        status=403, body='<input autocomplete="one-time-code">', content_type="text/html"))
                    try:
                        automatic_login(browser)
                    except Pause as exc:
                        assert "підтвердження" in str(exc)
                    else:
                        raise AssertionError("Login challenge was not stopped")
                    finally:
                        browser.unroute(challenge_url)
                browser.clear_cookies()
                with TemporaryDirectory() as temporary:
                    console = Console(Path(temporary))
                    console.set_mode("sync", "test")

                    def stop():
                        console.stop.set()
                        console.wake.set()

                    destination.stop = stop
                    timeout = threading.Timer(25, stop)
                    timeout.start()
                    try:
                        run(browser, console, destination, capture, 60, 10)
                    finally:
                        timeout.cancel()
                    assert destination.session_reports == ["active", "login_required"], destination.session_reports
                    assert Console(Path(temporary)).mode == "paused"
                    assert len(destination.transfers) == 3
            print(f"Fixture {phase} OK", flush=True)
        finally:
            browser.close()


if __name__ == "__main__":
    main(sys.argv[1])
