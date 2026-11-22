"""Avto.pro statuses eligible for first import by the seller browser."""

import re


NEW_STATUS = re.compile(
    r"(?:(?:нове|новий|новое|новый)(?: (?:замовлення|заказ))?|замовлення формується|заказ формируется"
    r"|(?:замовлення )?очікує (?:на )?підтвердження(?: продавцем)?"
    r"|(?:заказ )?ожидает подтверждения(?: продавцом)?)[.!]?",
    re.I,
)


def is_new_sale(status):
    return bool(NEW_STATUS.fullmatch(" ".join(status.split())))
