"""Prom contracts and transport. No credentials or customer data in errors."""
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import re
import requests


class PromError(ValueError):
    pass


def money(value):
    text = re.sub(r"[\s\u00a0\u202f]", "", str(value or "0")).replace(",", ".")
    text = re.sub(r"(?:грн\.?|UAH|₴)$", "", text, flags=re.I)
    try:
        result = Decimal(text)
    except InvalidOperation:
        raise PromError("Некоректна сума у відповіді Prom") from None
    if not result.is_finite() or result < 0:
        raise PromError("Некоректна сума у відповіді Prom")
    return result.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def quantity(value):
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError):
        raise PromError("Prom повернув некоректну кількість") from None
    if not result.is_finite() or result < 0 or result != result.to_integral_value():
        raise PromError("Для запчастин потрібна ціла невід'ємна кількість")
    return int(result)


def utc_date(value):
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc).replace(tzinfo=None) if parsed.tzinfo else parsed
    except (TypeError, ValueError):
        raise PromError("Некоректна дата у відповіді Prom") from None


def sku_key(value):
    return re.sub(r"[\s\-]", "", str(value or "")).casefold()


class PromClient:
    BASE_URL = "https://my.prom.ua/api/v1"

    def __init__(self, token, transport=None):
        if not token or not re.fullmatch(r"[A-Za-z0-9._\-]{20,512}", token):
            raise PromError("Збережіть чинний API-токен Prom")
        self.session = transport or requests.Session()
        self.session.headers.update({"Authorization": "Bearer " + token,
                                     "Accept": "application/json", "X-LANGUAGE": "uk"})

    def close(self):
        self.session.close()

    def request(self, method, path, *, params=None, payload=None):
        if not path.startswith("/") or "://" in path or ".." in path:
            raise PromError("Некоректна адреса методу Prom")
        try:
            response = self.session.request(method, self.BASE_URL + path,
                                            params=params, json=payload, timeout=(6, 25),
                                            allow_redirects=False)
        except requests.RequestException:
            raise PromError("Prom не відповідає. Обмін буде повторено автоматично") from None
        if response.status_code == 429:
            raise PromError("Ліміт запитів Prom. Наступну спробу відкладено")
        if response.status_code in (401, 403):
            raise PromError(f"Prom: HTTP {response.status_code}. Перевірте токен і права доступу")
        if not 200 <= response.status_code < 300:
            raise PromError(f"Prom: HTTP {response.status_code}. Дані не підтверджені")
        try:
            data = response.json()
        except ValueError:
            raise PromError("Prom повернув відповідь невідомого формату") from None
        if not isinstance(data, dict):
            raise PromError("Prom повернув відповідь невідомого формату")
        return data

    def pages(self, resource, **params):
        if resource not in ("orders", "products"):
            raise PromError("Непідтримуваний список Prom")
        cursor = None
        seen = set()
        for _ in range(200):
            query = {**params, "limit": 100}
            if cursor is not None:
                query["last_id"] = cursor
            data = self.request("GET", f"/{resource}/list", params=query)
            rows = data.get(resource)
            if not isinstance(rows, list):
                raise PromError(f"Prom не повернув список {resource}")
            identifiers = []
            for row in rows:
                if not isinstance(row, dict) or not str(row.get("id", "")).isdigit():
                    raise PromError("У запису Prom немає числового ID")
                identifier = int(row["id"])
                identifiers.append(identifier)
                if identifier not in seen:
                    seen.add(identifier)
                    yield row
            if len(rows) < 100:
                return
            next_cursor = min(identifiers) - 1
            if cursor is not None and next_cursor >= cursor:
                raise PromError("Prom не просуває сторінки списку; курсор не збережено")
            cursor = next_cursor
        raise PromError("Забагато сторінок Prom за один обмін. Зменшіть період імпорту")

    def product(self, identifier):
        data = self.request("GET", f"/products/{int(identifier)}")
        if not isinstance(data.get("product"), dict):
            raise PromError("Не отримано картку товару Prom")
        return data["product"]

    def stock(self, identifier, qty):
        qty = quantity(qty)
        data = self.request("POST", "/products/edit", payload=[{
            "id": int(identifier), "quantity_in_stock": qty,
            "presence": "available" if qty else "not_available"}])
        identifiers = data.get("processed_ids")
        if not isinstance(identifiers, list):
            raise PromError("Prom не повернув результат обробки залишку")
        processed = {str(value) for value in identifiers}
        if data.get("errors") or (processed and str(identifier) not in processed):
            raise PromError("Prom не підтвердив зміну залишку. Перевірте права запису й різновиди")
        # Real API returns [] with no errors for an unchanged product. Callers
        # must read back stock even when the API reports no processed IDs.
        return str(identifier) in processed
