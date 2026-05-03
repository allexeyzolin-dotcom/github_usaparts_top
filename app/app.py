import csv
import hashlib
import io
import json
import os
import re
import requests
import random
import shutil
import subprocess
import threading
import time
import zipfile
import xml.etree.ElementTree as ET
import qrcode
from decimal import Decimal
from urllib.parse import parse_qs, quote, urlsplit
from xml.sax.saxutils import escape as xml_escape
from PIL import Image, ImageOps, UnidentifiedImageError
from qrcode.image.svg import SvgPathImage
from uuid import uuid4
from datetime import datetime, timedelta
from functools import wraps
from pathlib import Path
from tempfile import TemporaryDirectory

from flask import Flask, render_template, request, redirect, url_for, flash, send_file, jsonify, session, Response
from sqlalchemy import create_engine, Column, Integer, String, Numeric, Boolean, DateTime, ForeignKey, Text, desc, func, event
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
UPLOAD_DIR = BASE_DIR / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "dev-secret")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://usa:usa123@localhost:5433/usa_auto_parts")
engine = create_engine(DATABASE_URL, future=True)
SessionLocal = sessionmaker(bind=engine, future=True)
Base = declarative_base()

DEFAULT_UAH_RATE = 41.50
ADMIN_EMAIL = "admin@usashop.local"
ADMIN_PASSWORD = "admin123"
EXPORT_PHOTO_SIZE = (360, 480)
W8_TRACKING_TIMEOUT_MS = 45000
W8_TRACKING_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/135.0.0.0 Safari/537.36"
)
W8_TRACKING_STEALTH_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'platform', {get: () => 'Win32'});
Object.defineProperty(navigator, 'language', {get: () => 'en-US'});
Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
window.chrome = { runtime: {} };
"""
W8_TRACKING_BROWSER_CANDIDATES = (
    os.getenv("PLAYWRIGHT_CHROMIUM_PATH"),
    "/usr/bin/chromium",
    "/usr/bin/chromium-browser",
    "/usr/bin/google-chrome",
    "/usr/bin/google-chrome-stable",
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
)
BACKUP_DIR = Path(os.getenv("BACKUP_DIR", str(PROJECT_ROOT / "backups"))).resolve()
BACKUP_LOCK = threading.Lock()
BACKUP_SCHEDULER_STARTED = False
BACKUP_SCHEDULER_INTERVAL_SECONDS = max(int(os.getenv("BACKUP_SCHEDULER_INTERVAL_SECONDS", "900") or 900), 60)


class Warehouse(Base):
    __tablename__ = "warehouses"
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False, unique=True)
    markup_percent = Column(Numeric(8, 2), nullable=False, default=0)
    revision_status = Column(String(32), nullable=False, default="not_started")
    revision_percent = Column(Integer, nullable=False, default=0)
    revision_date = Column(DateTime, nullable=True)
    revision_current_index = Column(Integer, nullable=False, default=0)
    revision_started_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    parts = relationship("Part", back_populates="warehouse", cascade="all, delete-orphan")


class Part(Base):
    __tablename__ = "parts"
    id = Column(Integer, primary_key=True)
    warehouse_id = Column(Integer, ForeignKey("warehouses.id"), nullable=False)
    part_number = Column(String(255), nullable=False, index=True)
    barcode = Column(String(8), nullable=False, default="", index=True)
    brand = Column(String(255), nullable=False, default="")
    producer_type = Column(String(32), nullable=False, default="OEM")
    name = Column(String(500), nullable=False, default="")
    description = Column(Text, nullable=False, default="")
    price_usd = Column(Numeric(12, 2), nullable=False, default=0)
    qty = Column(Integer, nullable=False, default=0)
    in_stock = Column(Boolean, nullable=False, default=False)
    photo_urls = Column(Text, nullable=False, default="")
    showcase_photo_urls = Column(Text, nullable=False, default="[]")
    youtube_url = Column(String(500), nullable=False, default="")
    has_photo = Column(Boolean, nullable=False, default=False)
    has_description = Column(Boolean, nullable=False, default=False)
    views_24h = Column(Integer, nullable=False, default=0)
    views_168h = Column(Integer, nullable=False, default=0)
    brand_export = Column(String(255), nullable=False, default="")
    part_number_export = Column(String(255), nullable=False, default="")
    avtopro_flag_1 = Column(String(32), nullable=False, default="")
    avtopro_flag_2 = Column(String(32), nullable=False, default="")
    avtopro_flag_3 = Column(String(32), nullable=False, default="")
    avtopro_flag_4 = Column(String(32), nullable=False, default="")
    raw_import_row = Column(Text, nullable=False, default="")
    stock_checked_at = Column(DateTime, nullable=True)
    stock_check_status = Column(String(32), nullable=False, default="unchecked")
    stock_check_note = Column(String(255), nullable=False, default="")
    is_deleted = Column(Boolean, nullable=False, default=False)
    deleted_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    warehouse = relationship("Warehouse", back_populates="parts")


class PartTemplate(Base):
    __tablename__ = "part_templates"
    id = Column(Integer, primary_key=True)
    part_number = Column(String(255), nullable=False, unique=True, index=True)
    cross_numbers_json = Column(Text, nullable=False, default="[]")
    barcode = Column(String(8), nullable=False, default="", index=True)
    brand = Column(String(255), nullable=False, default="")
    producer_type = Column(String(32), nullable=False, default="OEM")
    name = Column(String(500), nullable=False, default="")
    description = Column(Text, nullable=False, default="")
    price_usd = Column(Numeric(12, 2), nullable=False, default=0)
    unassigned_qty = Column(Integer, nullable=False, default=0)
    photo_urls = Column(Text, nullable=False, default="")
    showcase_photo_urls = Column(Text, nullable=False, default="[]")
    youtube_url = Column(String(500), nullable=False, default="")
    has_photo = Column(Boolean, nullable=False, default=False)
    has_description = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class TransitOrder(Base):
    __tablename__ = "transit_orders"
    id = Column(Integer, primary_key=True)
    batch_id = Column(String(64), nullable=False, default="")
    part_template_id = Column(Integer, ForeignKey("part_templates.id"), nullable=True)
    linked_part_id = Column(Integer, ForeignKey("parts.id"), nullable=True)
    part_number = Column(String(255), nullable=False, index=True)
    barcode = Column(String(8), nullable=False, default="", index=True)
    title = Column(String(500), nullable=False, default="")
    service_info = Column(Text, nullable=False, default="")
    short_description = Column(String(255), nullable=False, default="")
    full_description = Column(Text, nullable=False, default="")
    qty = Column(Integer, nullable=False, default=1)
    accepted_qty = Column(Integer, nullable=False, default=0)
    arrival_notified_qty = Column(Integer, nullable=False, default=0)
    price_usd = Column(Numeric(12, 2), nullable=False, default=0)
    photo_urls = Column(Text, nullable=False, default="")
    has_photo = Column(Boolean, nullable=False, default=False)
    status = Column(String(32), nullable=False, default="in_transit")
    labels_printed_at = Column(DateTime, nullable=True)
    archived_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    template = relationship("PartTemplate")
    part = relationship("Part")


class Car(Base):
    __tablename__ = "cars"
    id = Column(Integer, primary_key=True)
    vin = Column(String(64), nullable=False, default="")
    brand = Column(String(255), nullable=False, default="")
    model = Column(String(255), nullable=False, default="")
    year = Column(Integer, nullable=True)
    mileage = Column(Integer, nullable=True)
    status = Column(String(32), nullable=False, default="in_stock")
    price_usd = Column(Numeric(12, 2), nullable=False, default=0)
    description = Column(Text, nullable=False, default="")
    image_urls = Column(Text, nullable=False, default="")
    youtube_url = Column(String(500), nullable=False, default="")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class ApiSetting(Base):
    __tablename__ = "api_settings"
    id = Column(Integer, primary_key=True)
    setting_key = Column(String(255), nullable=False, unique=True)
    setting_value = Column(Text, nullable=False, default="")
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class NewsFeed(Base):
    __tablename__ = "news_feed"
    id = Column(Integer, primary_key=True)
    source = Column(String(255), nullable=False, default="")
    title = Column(String(255), nullable=False, default="")
    body = Column(Text, nullable=False, default="")
    severity = Column(String(32), nullable=False, default="info")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class WarehousePrintMark(Base):
    __tablename__ = "warehouse_print_marks"
    id = Column(Integer, primary_key=True)
    warehouse_id = Column(Integer, ForeignKey("warehouses.id"), nullable=True)
    part_number = Column(String(255), nullable=False, index=True)
    printed_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class ImportSession(Base):
    __tablename__ = "import_sessions"
    id = Column(Integer, primary_key=True)
    warehouse_id = Column(Integer, ForeignKey("warehouses.id"), nullable=False)
    file_name = Column(String(255), nullable=False)
    status = Column(String(32), nullable=False, default="preview")
    total_rows = Column(Integer, nullable=False, default=0)
    new_rows = Column(Integer, nullable=False, default=0)
    changed_rows = Column(Integer, nullable=False, default=0)
    same_rows = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class ImportChange(Base):
    __tablename__ = "import_changes"
    id = Column(Integer, primary_key=True)
    import_session_id = Column(Integer, ForeignKey("import_sessions.id"), nullable=False)
    part_number = Column(String(255), nullable=False)
    change_type = Column(String(32), nullable=False, default="same")
    before_price = Column(Numeric(12, 2), nullable=True)
    after_price = Column(Numeric(12, 2), nullable=True)
    before_qty = Column(Integer, nullable=True)
    after_qty = Column(Integer, nullable=True)
    before_stock = Column(Boolean, nullable=True)
    after_stock = Column(Boolean, nullable=True)
    apply_change = Column(Boolean, nullable=False, default=False)
    payload_json = Column(Text, nullable=False, default="{}")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class Order(Base):
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True)
    customer_name = Column(String(255), nullable=False, default="")
    phone = Column(String(64), nullable=False, default="")
    city = Column(String(255), nullable=False, default="")
    delivery_type = Column(String(32), nullable=False, default="pickup")
    np_service_type = Column(String(32), nullable=False, default="")
    np_city_ref = Column(String(64), nullable=False, default="")
    np_warehouse_ref = Column(String(64), nullable=False, default="")
    np_warehouse_label = Column(String(255), nullable=False, default="")
    np_street_ref = Column(String(64), nullable=False, default="")
    np_street_name = Column(String(255), nullable=False, default="")
    np_house = Column(String(64), nullable=False, default="")
    comment = Column(Text, nullable=False, default="")
    total_usd = Column(Numeric(12, 2), nullable=False, default=0)
    status = Column(String(32), nullable=False, default="new")
    is_processing = Column(Boolean, nullable=False, default=False)
    prepayment_usd = Column(Numeric(12, 2), nullable=False, default=0)
    ttn = Column(String(64), nullable=False, default="")
    ttn_status = Column(String(255), nullable=False, default="")
    cancel_reason = Column(Text, nullable=False, default="")
    stock_reserved = Column(Boolean, nullable=False, default=False)
    external_source = Column(String(64), nullable=False, default="")
    external_order_id = Column(String(255), nullable=False, default="")
    external_status = Column(String(255), nullable=False, default="")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    items = relationship("OrderItem", back_populates="order", cascade="all, delete-orphan")


class OrderItem(Base):
    __tablename__ = "order_items"
    id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey("orders.id"), nullable=False)
    part_id = Column(Integer, ForeignKey("parts.id"), nullable=True)
    part_number = Column(String(255), nullable=False)
    name = Column(String(500), nullable=False)
    qty = Column(Integer, nullable=False, default=1)
    price_usd = Column(Numeric(12, 2), nullable=False, default=0)
    order = relationship("Order", back_populates="items")


class StatsEvent(Base):
    __tablename__ = "stats_events"
    id = Column(Integer, primary_key=True)
    event_type = Column(String(64), nullable=False, index=True)
    visitor_id = Column(String(64), nullable=False, default="", index=True)
    part_id = Column(Integer, nullable=True)
    part_number = Column(String(255), nullable=False, default="", index=True)
    part_name = Column(String(500), nullable=False, default="")
    query_text = Column(String(500), nullable=False, default="", index=True)
    quantity = Column(Integer, nullable=False, default=1)
    order_id = Column(Integer, nullable=True)
    meta_json = Column(Text, nullable=False, default="{}")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)


class ReceivingDraftItem(Base):
    __tablename__ = "receiving_draft_items"
    id = Column(Integer, primary_key=True)
    warehouse_id = Column(Integer, ForeignKey("warehouses.id"), nullable=True)
    part_number = Column(String(255), nullable=False, index=True)
    barcode = Column(String(8), nullable=False, default="", index=True)
    title = Column(String(500), nullable=False, default="")
    qty = Column(Integer, nullable=False, default=1)
    price_usd = Column(Numeric(12, 2), nullable=False, default=0)
    description = Column(Text, nullable=False, default="")
    photo_urls = Column(Text, nullable=False, default="")
    has_photo = Column(Boolean, nullable=False, default=False)
    existing_stocks_json = Column(Text, nullable=False, default="[]")
    source = Column(String(32), nullable=False, default="mobile")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class AppNotification(Base):
    __tablename__ = "app_notifications"
    id = Column(Integer, primary_key=True)
    part_id = Column(Integer, ForeignKey("parts.id"), nullable=False)
    warehouse_id = Column(Integer, ForeignKey("warehouses.id"), nullable=False)
    barcode = Column(String(8), nullable=False, default="", index=True)
    part_number = Column(String(255), nullable=False, default="")
    title = Column(String(500), nullable=False, default="")
    reason = Column(Text, nullable=False, default="")
    current_qty = Column(Integer, nullable=False, default=0)
    entered_qty = Column(Integer, nullable=False, default=0)
    status = Column(String(32), nullable=False, default="open")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class AvailabilityRequest(Base):
    __tablename__ = "availability_requests"
    id = Column(Integer, primary_key=True)
    warehouse_id = Column(Integer, ForeignKey("warehouses.id"), nullable=False)
    title = Column(String(255), nullable=False, default="")
    status = Column(String(32), nullable=False, default="open")
    progress_percent = Column(Integer, nullable=False, default=0)
    total_items = Column(Integer, nullable=False, default=0)
    checked_items = Column(Integer, nullable=False, default=0)
    completed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    items = relationship("AvailabilityRequestItem", back_populates="request", cascade="all, delete-orphan")


class AvailabilityRequestItem(Base):
    __tablename__ = "availability_request_items"
    id = Column(Integer, primary_key=True)
    request_id = Column(Integer, ForeignKey("availability_requests.id"), nullable=False)
    part_id = Column(Integer, ForeignKey("parts.id"), nullable=False)
    part_number = Column(String(255), nullable=False, default="")
    title = Column(String(500), nullable=False, default="")
    expected_qty = Column(Integer, nullable=False, default=0)
    checked_qty = Column(Integer, nullable=True)
    status = Column(String(32), nullable=False, default="pending")
    note = Column(Text, nullable=False, default="")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    request = relationship("AvailabilityRequest", back_populates="items")


class PackingRequest(Base):
    __tablename__ = "packing_requests"
    id = Column(Integer, primary_key=True)
    source_type = Column(String(32), nullable=False, default="manual")
    source_order_id = Column(Integer, nullable=True)
    delivery_type = Column(String(32), nullable=False, default="pickup")
    np_service_type = Column(String(32), nullable=False, default="warehouse")
    np_city_ref = Column(String(64), nullable=False, default="")
    np_warehouse_ref = Column(String(64), nullable=False, default="")
    np_warehouse_label = Column(String(255), nullable=False, default="")
    np_street_ref = Column(String(64), nullable=False, default="")
    np_street_name = Column(String(255), nullable=False, default="")
    np_house = Column(String(64), nullable=False, default="")
    status = Column(String(32), nullable=False, default="open")
    customer_name = Column(String(255), nullable=False, default="")
    phone = Column(String(64), nullable=False, default="")
    city = Column(String(255), nullable=False, default="")
    comment = Column(Text, nullable=False, default="")
    control_payment_uah = Column(Numeric(12, 2), nullable=False, default=0)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    items = relationship("PackingRequestItem", back_populates="request", cascade="all, delete-orphan")


class PackingRequestItem(Base):
    __tablename__ = "packing_request_items"
    id = Column(Integer, primary_key=True)
    request_id = Column(Integer, ForeignKey("packing_requests.id"), nullable=False)
    part_id = Column(Integer, ForeignKey("parts.id"), nullable=True)
    part_number = Column(String(255), nullable=False, default="")
    title = Column(String(500), nullable=False, default="")
    expected_qty = Column(Integer, nullable=False, default=0)
    found_qty = Column(Integer, nullable=False, default=0)
    missing_qty = Column(Integer, nullable=False, default=0)
    status = Column(String(32), nullable=False, default="pending")
    photos_json = Column(Text, nullable=False, default="[]")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    request = relationship("PackingRequest", back_populates="items")


def now():
    return datetime.utcnow()


def admin_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("admin_auth"):
            return redirect(url_for("admin_login"))
        return fn(*args, **kwargs)
    return wrapper


@app.after_request
def apply_crawl_headers(response):
    private_paths = ("/admin", "/api", "/cart", "/checkout")
    if request.path == "/cart" or request.path == "/checkout" or request.path.startswith(private_paths):
        response.headers.setdefault("X-Robots-Tag", "noindex, nofollow")
    return response


def flash_news(db, source: str, title: str, body: str, severity: str = "info"):
    clean_source = normalize_text(source)
    clean_title = normalize_text(title)
    clean_body = normalize_text(body)
    clean_body = re.sub(r"^Склад\s+(Склад\b[^:]*:)", r"\1", clean_body)
    db.add(
        NewsFeed(
            source=clean_source,
            title=clean_title,
            body=clean_body,
            severity=severity,
            created_at=now(),
        )
    )


def get_api_settings_map(db):
    rows = db.query(ApiSetting).all()
    return {r.setting_key: r.setting_value for r in rows}


def telegram_settings(db):
    settings = get_api_settings_map(db)
    return {
        "bot_token": (settings.get("telegram_bot_token") or "").strip(),
        "chat_id": (settings.get("telegram_chat_id") or "").strip(),
    }


def normalize_ua_phone(phone_raw: str) -> str | None:
    digits = re.sub(r"\D", "", phone_raw or "")
    if digits.startswith("380") and len(digits) == 12:
        core = digits[3:]
    elif digits.startswith("0") and len(digits) == 10:
        core = digits[1:]
    elif len(digits) == 9:
        core = digits
    else:
        return None
    return f"+380 {core[:2]} {core[2:5]} {core[5:7]} {core[7:9]}"


def send_telegram_message(db, text: str):
    info = telegram_settings(db)
    if not info["bot_token"] or not info["chat_id"]:
        raise Exception("У вкладці API не заповнені Telegram bot token або chat ID.")
    return send_telegram_message_raw(info["bot_token"], info["chat_id"], text)


def send_telegram_message_raw(bot_token: str, chat_id: str, text: str):
    response = requests.post(
        f"https://api.telegram.org/bot{bot_token}/sendMessage",
        json={
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": True,
        },
        timeout=20,
    )
    if response.status_code >= 400:
        description = ""
        try:
            description = response.json().get("description", "")
        except Exception:
            description = response.text[:300]
        if "chat not found" in description.lower():
            raise Exception("Telegram не знаходить цей chat ID. Для групи вкажіть ID групи, зазвичай він починається з -100.")
        raise Exception(f"Telegram API error: {response.status_code} {description}")
    payload = response.json()
    if not payload.get("ok"):
        raise Exception(payload.get("description") or "Telegram не підтвердив відправлення повідомлення.")
    return payload


def queue_telegram_message(db, text: str):
    info = telegram_settings(db)
    if not info["bot_token"] or not info["chat_id"]:
        return False
    db.info.setdefault("telegram_queue", []).append(
        {
            "bot_token": info["bot_token"],
            "chat_id": info["chat_id"],
            "text": text,
        }
    )
    return True


def inventory_change_icon(delta_qty: int, new_qty: int) -> str:
    if new_qty <= 0:
        return "❌"
    if delta_qty > 0:
        return "🟢⬆️"
    if delta_qty < 0:
        return "🔴⬇️"
    return "ℹ️"


def build_inventory_telegram_message(
    *,
    part_number: str,
    title: str,
    before_qty: int,
    after_qty: int,
    context_label: str = "",
    reason: str = "",
):
    delta_qty = int(after_qty or 0) - int(before_qty or 0)
    if delta_qty == 0:
        return ""
    icon = inventory_change_icon(delta_qty, after_qty)
    is_increase = delta_qty > 0
    change_label = "Додано" if is_increase else "Списано"
    headline = "Додавання товару" if is_increase else "Зменшення наявності"
    description = normalize_text(title or "").strip() or "Без опису"
    lines = [
        f"{icon} {headline}",
        "",
        f"Номер запчастини: {normalize_text(part_number or '').strip() or '-'}",
        f"Опис: {description}",
        f"Кількість на складі: {int(before_qty or 0)}",
        f"{change_label}: {abs(int(delta_qty))}",
        f"Новий залишок: {int(after_qty or 0)}",
    ]
    if context_label:
        lines.append(f"Контекст: {normalize_text(context_label).strip()}")
    if reason:
        lines.append(f"Причина: {normalize_text(reason).strip()}")
    return "\n".join(lines)


def queue_inventory_change(
    db,
    *,
    part_number: str,
    title: str,
    before_qty: int,
    after_qty: int,
    context_label: str = "",
    reason: str = "",
):
    text = build_inventory_telegram_message(
        part_number=part_number,
        title=title,
        before_qty=before_qty,
        after_qty=after_qty,
        context_label=context_label,
        reason=reason,
    )
    if text:
        queue_telegram_message(db, text)


def queue_part_inventory_change(db, part: "Part", before_qty: int, context_label: str = "", reason: str = ""):
    if not part:
        return
    queue_inventory_change(
        db,
        part_number=part.part_number,
        title=part.name or part.description or "",
        before_qty=before_qty,
        after_qty=int(part.qty or 0),
        context_label=context_label,
        reason=reason,
    )


def queue_template_inventory_change(db, template: "PartTemplate", before_qty: int, context_label: str = "", reason: str = ""):
    if not template:
        return
    queue_inventory_change(
        db,
        part_number=template.part_number,
        title=template.name or template.description or "",
        before_qty=before_qty,
        after_qty=template_unassigned_qty(template),
        context_label=context_label,
        reason=reason,
    )


def build_inventory_assignment_telegram_message(
    *,
    part_number: str,
    title: str,
    qty: int,
    from_label: str,
    to_label: str,
    warehouse_qty: int,
    unassigned_qty: int,
    context_label: str = "",
    reason: str = "",
) -> str:
    qty = max(int(qty or 0), 0)
    if qty <= 0:
        return ""
    description = normalize_text(title or "").strip() or "Без опису"
    direction_to_warehouse = normalize_text(to_label or "").strip().casefold() != "всі товари"
    headline = "Присвоєння складу" if direction_to_warehouse else "Повернення у Всі товари"
    lines = [
        f"🔁 {headline}",
        "",
        f"Номер запчастини: {normalize_text(part_number or '').strip() or '-'}",
        f"Опис: {description}",
        f"Переміщено: {qty}",
        f"Звідки: {normalize_text(from_label or '').strip() or '-'}",
        f"Куди: {normalize_text(to_label or '').strip() or '-'}",
        f"Залишок у складі: {max(int(warehouse_qty or 0), 0)}",
        f"Залишок у Всі товари: {max(int(unassigned_qty or 0), 0)}",
    ]
    if context_label:
        lines.append(f"Контекст: {normalize_text(context_label).strip()}")
    if reason:
        lines.append(f"Причина: {normalize_text(reason).strip()}")
    return "\n".join(lines)


def queue_inventory_assignment_change(
    db,
    *,
    template: "PartTemplate",
    warehouse_name: str,
    qty: int,
    to_warehouse: bool,
    warehouse_qty: int,
    unassigned_qty: int,
    context_label: str = "",
    reason: str = "",
):
    if not template:
        return
    warehouse_label = normalize_text(warehouse_name or "").strip() or "Склад"
    text = build_inventory_assignment_telegram_message(
        part_number=template.part_number,
        title=template.name or template.description or "",
        qty=qty,
        from_label="Всі товари" if to_warehouse else warehouse_label,
        to_label=warehouse_label if to_warehouse else "Всі товари",
        warehouse_qty=warehouse_qty,
        unassigned_qty=unassigned_qty,
        context_label=context_label,
        reason=reason,
    )
    if text:
        queue_telegram_message(db, text)


def build_order_telegram_message(order: Order, items: list[dict], city_name: str, address: str, comment: str) -> str:
    delivery_comment = normalize_text(order.comment or "").strip()
    lines = [
        "🟢➕ Нове замовлення",
        "",
        f"Замовлення: #{order.id}",
        f"Клієнт: {normalize_text(order.customer_name or '').strip() or 'Без імені'}",
        f"Телефон: {normalize_text(order.phone or '').strip() or 'Не вказано'}",
        f"Місто: {city_name or 'Не вказано'}",
        f"Адреса / доставка: {address or delivery_comment or 'Не вказано'}",
        "",
        "Позиції:",
    ]
    for item in items:
        part = item.get("part")
        if not part:
            continue
        name = normalize_text(part.name or "").strip() or "Без опису"
        lines.append(f"- {part.part_number} • {name} • {int(item.get('qty') or 0)} шт.")
    lines.extend(
        [
            "",
            f"Сума: ${float(order.total_usd or 0):.2f}",
            f"Коментар: {comment or 'Без коментаря'}",
            f"Час: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
        ]
    )
    return "\n".join(lines)


def build_order_ttn_telegram_message(order: Order, ttn: str) -> str:
    order_comment = normalize_text(order.comment or "").strip()
    lines = [
        "📦 ТТН додано до замовлення",
        "",
        f"Замовлення: #{order.id}",
        f"Клієнт: {normalize_text(order.customer_name or '').strip() or 'Без імені'}",
        f"Телефон: {normalize_text(order.phone or '').strip() or 'Не вказано'}",
        f"ТТН: {normalize_text(ttn or '').strip() or 'Не вказано'}",
    ]
    if order.city:
        lines.append(f"Місто: {normalize_text(order.city or '').strip()}")
    if order_comment:
        lines.append(f"Коментар / доставка: {order_comment}")
    lines.extend(
        [
            f"Сума: ${float(order.total_usd or 0):.2f}",
            f"Час: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
        ]
    )
    return "\n".join(lines)


def telegram_api_request(db, method_name: str, params=None):
    info = telegram_settings(db)
    if not info["bot_token"]:
        raise Exception("У вкладці API не заповнений Telegram bot token.")
    response = requests.get(
        f"https://api.telegram.org/bot{info['bot_token']}/{method_name}",
        params=params or {},
        timeout=20,
    )
    payload = response.json()
    if response.status_code >= 400 or not payload.get("ok"):
        raise Exception(payload.get("description") or f"Telegram API error: {response.status_code}")
    return payload.get("result")


def telegram_connection_status(db):
    info = telegram_settings(db)
    status = {
        "configured": bool(info["bot_token"]),
        "chat_id": info["chat_id"],
        "bot_username": "",
        "chat_ok": False,
        "chat_type": "",
        "chat_title": "",
        "error": "",
    }
    if not info["bot_token"]:
        status["error"] = "Не заповнений Telegram bot token."
        return status
    try:
        me = telegram_api_request(db, "getMe")
        status["bot_username"] = me.get("username", "")
    except Exception as exc:
        status["error"] = str(exc)
        return status
    if not info["chat_id"]:
        status["error"] = "Не заповнений chat ID."
        return status
    try:
        chat = telegram_api_request(db, "getChat", {"chat_id": info["chat_id"]})
        status["chat_ok"] = True
        status["chat_type"] = chat.get("type", "")
        status["chat_title"] = chat.get("title") or chat.get("username") or chat.get("first_name") or ""
    except Exception as exc:
        status["error"] = str(exc)
    return status


@event.listens_for(SessionLocal, "after_commit")
def send_queued_telegram_messages(session):
    messages = session.info.pop("telegram_queue", [])
    for message in messages:
        try:
            send_telegram_message_raw(message["bot_token"], message["chat_id"], message["text"])
        except Exception:
            continue


@event.listens_for(SessionLocal, "after_rollback")
def clear_queued_telegram_messages(session):
    session.info.pop("telegram_queue", None)


def telegram_recent_chats(db):
    try:
        updates = telegram_api_request(
            db,
            "getUpdates",
            {"limit": 25, "allowed_updates": json.dumps(["message", "edited_message", "my_chat_member", "chat_member"])},
        ) or []
    except Exception:
        return []
    found = {}
    for update in updates:
        chat = None
        if update.get("message"):
            chat = update["message"].get("chat")
        elif update.get("edited_message"):
            chat = update["edited_message"].get("chat")
        elif update.get("my_chat_member"):
            chat = update["my_chat_member"].get("chat")
        elif update.get("chat_member"):
            chat = update["chat_member"].get("chat")
        if not chat:
            continue
        chat_id = str(chat.get("id", "")).strip()
        if not chat_id or chat_id in found:
            continue
        found[chat_id] = {
            "id": chat_id,
            "type": chat.get("type", ""),
            "title": chat.get("title") or chat.get("username") or chat.get("first_name") or "Без назви",
        }
    chats = list(found.values())
    chats.sort(key=lambda item: (0 if item["id"].startswith("-100") else 1, item["title"].lower()))
    return chats


def json_loads_safe(value, default):
    if not value:
        return default
    try:
        return json.loads(value)
    except Exception:
        return default


def _text_quality_score(text: str) -> int:
    if not text:
        return 0
    good_letters = sum(1 for ch in text if ("А" <= ch <= "я") or ch in "ІіЇїЄєҐґ")
    suspicious = sum(text.count(token) for token in ("РЎ", "РІ", "Рє", "Р»", "СЃ", "С‚", "СЏ", "вЂ", "Ð", "Ñ", "ЎР", "�"))
    return (good_letters * 2) - (suspicious * 5)


def _repair_text_once(text: str, source_encoding: str) -> str:
    try:
        return text.encode(source_encoding).decode("utf-8")
    except Exception:
        return text


def _repair_common_mojibake_segments(text: str) -> str:
    replacements = {
        "РРЎРєР»Р°Рґ": "Склад",
        "РЎРєР»Р°Рґ": "Склад",
        "СЃРєР»Р°Рґ": "склад",
        "Р†РјРїРѕСЂС‚РѕРІР°РЅРѕ": "Імпортовано",
        "РѕРЅРѕРІР»РµРЅРѕ": "оновлено",
        "Р·Р°СЃС‚РѕСЃРѕРІР°РЅРѕ": "застосовано",
        "Р·РјС–РЅРµРЅРёС…": "змінених",
        "Р·РјС–РЅ": "змін",
        "РЅРѕРІРёС…": "нових",
        "РЎС‚РІРѕСЂРµРЅРѕ": "Створено",
        "РџСЂРёР№РЅСЏС‚Рѕ": "Прийнято",
        "РџСЂРёР№РѕРј": "Прийом",
        "РЎРёСЃС‚РµРјР°": "Система",
    }
    repaired = text
    for broken, fixed in replacements.items():
        repaired = repaired.replace(broken, fixed)
    repaired = re.sub(r"\bСѓ\b", "у", repaired)
    return repaired


def normalize_text(value):
    if value is None:
        return ""
    if not isinstance(value, str):
        return value
    text = value
    if text.startswith("ЎР"):
        text = "Р" + text
    elif text.startswith("ўр"):
        text = "р" + text
    best = text
    best_score = _text_quality_score(best)

    for _ in range(3):
        candidates = {best}
        candidates.add(_repair_text_once(best, "cp1251"))
        candidates.add(_repair_text_once(best, "latin1"))
        candidates.add(_repair_text_once(best, "cp866"))
        improved = max(candidates, key=_text_quality_score)
        improved_score = _text_quality_score(improved)
        if improved_score <= best_score or improved == best:
            break
        best = improved
        best_score = improved_score

    best = _repair_common_mojibake_segments(best)
    return best


def ean8_check_digit(payload7: str) -> str:
    digits = [int(x) for x in payload7]
    odd_sum = digits[0] + digits[2] + digits[4] + digits[6]
    even_sum = digits[1] + digits[3] + digits[5]
    total = odd_sum * 3 + even_sum
    return str((10 - (total % 10)) % 10)


def barcode_from_numeric_id(identifier: int) -> str:
    payload = f"{max(int(identifier or 0), 0):07d}"[-7:]
    return payload + ean8_check_digit(payload)


def ensure_part_barcode(db, part: Part):
    if part and not (part.barcode or "").strip():
        if not part.id:
            db.flush()
        part.barcode = barcode_from_numeric_id(part.id)


def ensure_draft_barcode(db, item: ReceivingDraftItem):
    if item and not (item.barcode or "").strip():
        if not item.id:
            db.flush()
        item.barcode = barcode_from_numeric_id(item.id)


def ensure_template_barcode(db, template: PartTemplate):
    if template and not (template.barcode or "").strip():
        if not template.id:
            db.flush()
        template.barcode = barcode_from_numeric_id(5_000_000 + int(template.id or 0))


def find_generated_barcode_match(db, normalized_barcode: str):
    if len(normalized_barcode or "") != 8 or not normalized_barcode.isdigit():
        return None, None
    payload7 = normalized_barcode[:7]

    try:
        raw_identifier = int(payload7)
    except Exception:
        return None, None

    template_candidate = None
    if raw_identifier >= 5_000_000:
        template_id = raw_identifier - 5_000_000
        if template_id > 0:
            template_candidate = db.get(PartTemplate, template_id)
            if template_candidate:
                ensure_template_barcode(db, template_candidate)
                if normalize_text(template_candidate.barcode or "").strip() == normalized_barcode:
                    return template_candidate, None

    part_candidate = db.get(Part, raw_identifier)
    if part_candidate and not bool(part_candidate.is_deleted):
        ensure_part_barcode(db, part_candidate)
        if normalize_text(part_candidate.barcode or "").strip() == normalized_barcode:
            return None, part_candidate

    return None, None


def get_admin_password_hash(db):
    settings = get_api_settings_map(db)
    return (settings.get("admin_password_hash") or "").strip()


def verify_admin_password(db, password: str) -> bool:
    password_hash = get_admin_password_hash(db)
    if password_hash:
        return check_password_hash(password_hash, password)
    return password == ADMIN_PASSWORD


def set_admin_password(db, password: str):
    set_setting(db, "admin_password_hash", generate_password_hash(password))


def format_dt(value):
    if not value:
        return ""
    return value.strftime("%d.%m.%Y %H:%M")


def stock_status_label(status: str) -> str:
    mapping = {
        "unchecked": "Не перевірено",
        "checked_ok": "Збігається",
        "updated": "Оновлено",
        "needs_clarification": "Потребує уточнення",
    }
    return mapping.get((status or "").strip(), "Не перевірено")


def is_active_part(part: Part | None) -> bool:
    return bool(part and not bool(part.is_deleted))


def deleted_note(part: Part | None) -> str:
    if not part or not part.is_deleted:
        return ""
    return "Видалено"


def template_unassigned_qty(template: PartTemplate | None) -> int:
    try:
        return max(int(template.unassigned_qty or 0), 0) if template else 0
    except Exception:
        return 0


def template_note(template: PartTemplate | None) -> str:
    if not template:
        return ""
    qty = template_unassigned_qty(template)
    if qty > 0:
        return f"Не додано до складу • {qty} шт. у базі"
    return "Не додано до складу"


def producer_type_label(value: str | None) -> str:
    text = normalize_text(value or "").strip()
    upper = text.upper()
    lowered = text.casefold()
    if "OEM" in upper:
        return "OEM"
    if any(token in lowered for token in ["замін", "замен", "аналог", "aftermarket"]):
        return "Замінник"
    return text or "OEM"


def apply_template_to_parts(db, template: PartTemplate, only_parts=None):
    if not template:
        return []
    parts = only_parts
    if parts is None:
        parts = db.query(Part).filter(Part.part_number == template.part_number).all()
    for sibling in parts:
        sibling.brand = normalize_text(template.brand or sibling.brand or "").strip()
        sibling.producer_type = producer_type_label(template.producer_type or sibling.producer_type)
        sibling.name = normalize_text(template.name or sibling.name or "").strip()
        sibling.description = normalize_text(template.description or sibling.description or "").strip()
        sibling.price_usd = float(template.price_usd or sibling.price_usd or 0)
        sibling.photo_urls = template.photo_urls or sibling.photo_urls or ""
        sibling.showcase_photo_urls = (
            template.showcase_photo_urls
            or sibling.showcase_photo_urls
            or dump_media_urls([template.photo_urls] if template.photo_urls else [])
        )
        sibling.youtube_url = normalize_text(template.youtube_url or sibling.youtube_url or "").strip()
        sibling.brand_export = normalize_text(template.brand or sibling.brand_export or "").strip()
        sibling.part_number_export = sibling.part_number or sibling.part_number_export or ""
        sibling.has_photo = bool(primary_part_photo(sibling))
        sibling.has_description = bool((sibling.description or "").strip())
        if template.barcode and not sibling.barcode:
            sibling.barcode = template.barcode
    return parts


def merge_master_value(current_value, candidate_value, *, allow_zero: bool = False):
    current_text = normalize_text("" if current_value is None else str(current_value)).strip()
    candidate_text = normalize_text("" if candidate_value is None else str(candidate_value)).strip()
    if current_text:
        return current_value
    if candidate_text:
        return candidate_value
    if allow_zero and candidate_value not in (None, ""):
        return candidate_value
    return current_value


def rebalance_template_assignment_qty(template: PartTemplate | None, before_qty: int, after_qty: int) -> int:
    if not template:
        return 0
    before_qty = max(int(before_qty or 0), 0)
    after_qty = max(int(after_qty or 0), 0)
    current_unassigned = template_unassigned_qty(template)
    if after_qty > before_qty:
        transfer_qty = min(after_qty - before_qty, current_unassigned)
        if transfer_qty:
            template.unassigned_qty = current_unassigned - transfer_qty
    elif after_qty < before_qty:
        template.unassigned_qty = current_unassigned + (before_qty - after_qty)
    template.updated_at = now()
    return template_unassigned_qty(template)


def sync_template_from_part(db, part: Part):
    if not part or not (part.part_number or "").strip():
        return None

    existing_template = find_part_template(db, part.part_number)
    merged_gallery = parse_media_urls(
        template_gallery_urls(existing_template) + part_gallery_urls(part)
    )
    export_photo = safe_photo(part.photo_urls)
    if not export_photo and existing_template:
        export_photo = safe_photo(existing_template.photo_urls)
    if not export_photo and merged_gallery:
        export_photo = merged_gallery[0]

    payload = {
        "brand": normalize_text(part.brand or "").strip(),
        "producer_type": producer_type_label(part.producer_type or "OEM"),
        "name": normalize_text(part.name or "").strip(),
        "description": normalize_text(part.description or "").strip(),
        "price_usd": float(part.price_usd or 0),
        "photo_urls": export_photo,
        "showcase_photo_urls": merged_gallery,
        "youtube_url": normalize_text(part.youtube_url or "").strip(),
    }

    if existing_template:
        payload = {
            "brand": merge_master_value(existing_template.brand, payload["brand"]),
            "producer_type": producer_type_label(existing_template.producer_type or payload["producer_type"] or "OEM"),
            "name": merge_master_value(existing_template.name, payload["name"]),
            "description": merge_master_value(existing_template.description, payload["description"]),
            "price_usd": merge_master_value(existing_template.price_usd, payload["price_usd"], allow_zero=True),
            "photo_urls": merge_master_value(existing_template.photo_urls, payload["photo_urls"]),
            "showcase_photo_urls": merged_gallery or template_gallery_urls(existing_template),
            "youtube_url": merge_master_value(existing_template.youtube_url, payload["youtube_url"]),
        }

    template, _ = upsert_part_template(db, part.part_number, payload)
    if part.barcode and not template.barcode:
        template.barcode = part.barcode
    ensure_template_barcode(db, template)
    apply_template_to_parts(db, template)
    return template


def build_all_goods_cards(db, query_text: str = ""):
    templates = db.query(PartTemplate).order_by(desc(PartTemplate.updated_at), PartTemplate.part_number.asc()).all()
    parts = db.query(Part).order_by(desc(Part.updated_at), Part.part_number.asc()).all()
    template_map = {}
    parts_map: dict[str, list[Part]] = {}

    for template in templates:
        ensure_template_barcode(db, template)
        key = normalize_text(template.part_number or "").strip().upper()
        if key:
            template_map[key] = template

    for part in parts:
        ensure_part_barcode(db, part)
        key = normalize_text(part.part_number or "").strip().upper()
        if not key:
            continue
        parts_map.setdefault(key, []).append(part)

    needle = normalize_text(query_text or "").strip().casefold()
    cards = []
    for part_number in sorted(set(template_map.keys()) | set(parts_map.keys())):
        template = template_map.get(part_number)
        related_parts = parts_map.get(part_number, [])
        active_parts = [part for part in related_parts if not part.is_deleted]
        active_parts.sort(
            key=lambda part: (
                0 if part.in_stock else 1,
                -(int(part.qty or 0)),
                -(part.updated_at.timestamp() if part.updated_at else 0),
                part.id,
            )
        )
        related_parts.sort(
            key=lambda part: (
                0 if not part.is_deleted else 1,
                -(part.updated_at.timestamp() if part.updated_at else 0),
                part.id,
            )
        )
        exemplar = active_parts[0] if active_parts else (related_parts[0] if related_parts else None)

        name = normalize_text(
            (template.name if template else exemplar.name if exemplar else "")
        ).strip()
        description = normalize_text(
            (template.description if template else exemplar.description if exemplar else "")
        ).strip()
        brand = normalize_text(
            (template.brand if template else exemplar.brand if exemplar else "")
        ).strip()
        barcode = normalize_text(
            (template.barcode if template else exemplar.barcode if exemplar else "")
        ).strip()
        cross_numbers = template_cross_numbers(template)
        photo_url = primary_template_photo(template) if template else ""
        if not photo_url and exemplar:
            photo_url = primary_part_photo(exemplar)
        gallery = template_gallery_urls(template) if template else []
        if not gallery and exemplar:
            gallery = part_gallery_urls(exemplar)
        youtube_url = normalize_text(
            (template.youtube_url if template else exemplar.youtube_url if exemplar else "")
        ).strip()
        price_usd = float((template.price_usd if template else exemplar.price_usd if exemplar else 0) or 0)
        template_qty = template_unassigned_qty(template)
        total_qty = template_qty + sum(int(part.qty or 0) for part in active_parts)
        warehouse_names = []
        assigned_warehouses = []
        for part in active_parts:
            warehouse = part.warehouse if hasattr(part, "warehouse") else None
            if warehouse and warehouse.name and warehouse.name not in warehouse_names:
                warehouse_names.append(warehouse.name)
            assigned_warehouses.append(
                {
                    "partId": part.id,
                    "warehouseId": part.warehouse_id,
                    "warehouseName": warehouse.name if warehouse else "",
                    "qty": int(part.qty or 0),
                    "isDeleted": bool(part.is_deleted),
                }
            )

        haystack = " ".join(
            [
                part_number,
                name,
                description,
                brand,
                barcode,
                " ".join(cross_numbers),
                " ".join(warehouse_names),
            ]
        ).casefold()
        if needle and needle not in haystack:
            continue

        updated_at = (
            format_dt(template.updated_at)
            if template and template.updated_at
            else format_dt(exemplar.updated_at) if exemplar and exemplar.updated_at else ""
        )
        warehouse_summary_parts = []
        if warehouse_names:
            warehouse_summary_parts.append(", ".join(warehouse_names))
        else:
            warehouse_summary_parts.append("Не додано до складу")
        if template_qty > 0:
            warehouse_summary_parts.append(f"Без складу: {template_qty} шт.")

        changed_records = []
        if template and template.updated_at and template.created_at and template.updated_at > (template.created_at + timedelta(seconds=1)):
            changed_records.append(template.updated_at)
        for part in related_parts:
            if part.updated_at and part.created_at and part.updated_at > (part.created_at + timedelta(seconds=1)):
                changed_records.append(part.updated_at)
        changed_at = max(changed_records) if changed_records else None

        cards.append(
            {
                "partNumber": part_number,
                "templateId": template.id if template else None,
                "name": name or part_number,
                "description": description,
                "brand": brand,
                "barcode": barcode,
                "crossNumbers": cross_numbers,
                "photoUrl": photo_url,
                "galleryCount": len(gallery),
                "youtubeUrl": youtube_url,
                "priceUsd": price_usd,
                "producerTypeLabel": producer_type_label(
                    template.producer_type if template else exemplar.producer_type if exemplar else "OEM"
                ),
                "producerType": producer_type_label(
                    template.producer_type if template else exemplar.producer_type if exemplar else "OEM"
                ),
                "templateQty": template_qty,
                "totalQty": total_qty,
                "assignedCount": len(active_parts),
                "warehouseNames": warehouse_names,
                "assignedWarehouses": assigned_warehouses,
                "primaryWarehouseId": str(active_parts[0].warehouse_id) if active_parts else "all",
                "primaryQty": int(active_parts[0].qty or 0) if active_parts else template_qty,
                "primaryPartId": active_parts[0].id if active_parts else None,
                "multipleWarehouses": len(active_parts) > 1,
                "warehouseSummary": " • ".join(part for part in warehouse_summary_parts if part),
                "isTemplateOnly": not active_parts,
                "hasChanges": bool(changed_at),
                "changedAtTs": changed_at.timestamp() if changed_at else 0,
                "updatedAt": updated_at,
            }
        )

    cards.sort(
        key=lambda item: (
            0 if item["hasChanges"] else 1,
            -(item["changedAtTs"] or 0),
            0 if not item["isTemplateOnly"] else 1,
            item["name"].casefold(),
            item["partNumber"].casefold(),
        )
    )
    return cards


def warehouse_print_marks_map(db, warehouse_id: int | None):
    query = db.query(WarehousePrintMark)
    if warehouse_id is None:
        query = query.filter(WarehousePrintMark.warehouse_id.is_(None))
    else:
        query = query.filter(WarehousePrintMark.warehouse_id == warehouse_id)
    return {
        normalize_text(mark.part_number or "").strip().upper(): mark
        for mark in query.order_by(desc(WarehousePrintMark.updated_at), desc(WarehousePrintMark.id)).all()
    }


def warehouse_print_scope_parts(db, warehouse_id: int, query_text: str = ""):
    query = db.query(Part).filter(Part.warehouse_id == warehouse_id, Part.is_deleted == False)
    needle = normalize_text(query_text or "").strip()
    if needle:
        like = f"%{needle}%"
        query = query.filter(
            (Part.part_number.ilike(like))
            | (Part.name.ilike(like))
            | (Part.description.ilike(like))
            | (Part.barcode.ilike(like))
        )
    return query.order_by(Part.part_number.asc(), Part.id.asc()).all()


def warehouse_print_row_description(primary_text: str, secondary_text: str = "") -> str:
    primary = normalize_text(primary_text or "").strip()
    secondary = normalize_text(secondary_text or "").strip()
    if secondary and secondary.casefold() != primary.casefold():
        return " / ".join(part for part in [primary, secondary] if part)
    return primary or secondary or "—"


def build_warehouse_print_picker_rows(db, scope: str, query_text: str = "", selected_numbers=None):
    selected_set = {
        normalize_text(value or "").strip().upper()
        for value in (selected_numbers or [])
        if normalize_text(value or "").strip()
    }
    rows = []
    if scope == "all":
        marks = warehouse_print_marks_map(db, None)
        for card in build_all_goods_cards(db, query_text):
            part_number = normalize_text(card.get("partNumber") or "").strip().upper()
            if not part_number:
                continue
            if selected_set and part_number not in selected_set:
                continue
            mark = marks.get(part_number)
            rows.append(
                {
                    "part_number": part_number,
                    "description": warehouse_print_row_description(card.get("name"), card.get("description")),
                    "qty": max(int(card.get("totalQty") or 0), 0),
                    "printed_at": mark.printed_at if mark else None,
                    "printed_at_label": format_dt(mark.printed_at) if mark else "",
                    "is_printed": bool(mark),
                }
            )
        return rows

    warehouse_id = int(scope or 0)
    marks = warehouse_print_marks_map(db, warehouse_id)
    for part in warehouse_print_scope_parts(db, warehouse_id, query_text):
        part_number = normalize_text(part.part_number or "").strip().upper()
        if not part_number:
            continue
        if selected_set and part_number not in selected_set:
            continue
        template = find_part_template(db, part_number)
        mark = marks.get(part_number)
        rows.append(
            {
                "part_number": part_number,
                "description": warehouse_print_row_description(
                    template.name if template and template.name else part.name,
                    template.description if template and template.description else part.description,
                ),
                "qty": max(int(part.qty or 0), 0),
                "printed_at": mark.printed_at if mark else None,
                "printed_at_label": format_dt(mark.printed_at) if mark else "",
                "is_printed": bool(mark),
            }
        )
    return rows


def touch_warehouse_print_marks(db, scope: str, part_numbers, printed_at: datetime | None = None):
    stamp = printed_at or now()
    warehouse_id = None if scope == "all" else int(scope or 0)
    normalized_numbers = [
        normalize_text(part_number or "").strip().upper()
        for part_number in part_numbers
        if normalize_text(part_number or "").strip()
    ]
    for part_number in normalized_numbers:
        query = db.query(WarehousePrintMark).filter(WarehousePrintMark.part_number == part_number)
        if warehouse_id is None:
            mark = query.filter(WarehousePrintMark.warehouse_id.is_(None)).order_by(WarehousePrintMark.id.asc()).first()
        else:
            mark = query.filter(WarehousePrintMark.warehouse_id == warehouse_id).order_by(WarehousePrintMark.id.asc()).first()
        if mark:
            mark.printed_at = stamp
            mark.updated_at = stamp
            continue
        db.add(
            WarehousePrintMark(
                warehouse_id=warehouse_id,
                part_number=part_number,
                printed_at=stamp,
                created_at=stamp,
                updated_at=stamp,
            )
        )


def warehouse_print_scope_label(db, scope: str) -> str:
    if scope == "all":
        return "Всі товари"
    warehouse = db.get(Warehouse, int(scope or 0))
    return warehouse.name if warehouse else "Склад"


def build_showcase_parts(parts, query_text: str = "", limit: int = 12, cross_map: dict[str, list[str]] | None = None):
    needle = normalize_text(query_text or "").strip().casefold()
    cross_map = cross_map or {}
    best_by_number = {}
    for part in parts:
        if not part or part.is_deleted or not part.in_stock:
            continue
        if needle and not public_part_matches_query(part, needle, cross_map):
            continue
        photo_url = primary_part_photo(part)
        if not photo_url and not needle:
            continue
        key = normalize_text(part.part_number or "").strip().upper()
        current = best_by_number.get(key)
        if not current:
            best_by_number[key] = part
            continue
        current_score = (
            int(current.qty or 0),
            1 if current.has_photo else 0,
            current.updated_at.timestamp() if current.updated_at else 0,
        )
        new_score = (
            int(part.qty or 0),
            1 if part.has_photo else 0,
            part.updated_at.timestamp() if part.updated_at else 0,
        )
        if new_score > current_score:
            best_by_number[key] = part

    ordered = sorted(
        best_by_number.values(),
        key=lambda part: (
            hashlib.md5(normalize_text(part.part_number or "").encode("utf-8", errors="ignore")).hexdigest(),
            normalize_text(part.name or "").casefold(),
        ),
    )
    return ordered[:limit], len(ordered)


def public_part_matches_query(part: Part, needle: str, cross_map: dict[str, list[str]] | None = None) -> bool:
    if not part:
        return False
    normalized = normalize_text(needle or "").strip().casefold()
    if not normalized:
        return True
    key = normalize_text(part.part_number or "").strip().upper()
    cross_numbers = (cross_map or {}).get(key, [])
    haystack = " ".join(
        [
            normalize_text(part.part_number or ""),
            " ".join(cross_numbers),
            normalize_text(part.name or ""),
            normalize_text(part.brand or ""),
            normalize_text(part.description or ""),
        ]
    ).casefold()
    return normalized in haystack


def serialize_part_card(db, part: Part):
    warehouse = db.get(Warehouse, part.warehouse_id)
    return {
        "partId": part.id,
        "warehouseId": part.warehouse_id,
        "warehouseName": warehouse.name if warehouse else "",
        "barcode": part.barcode or "",
        "partNumber": part.part_number or "",
        "title": part.name or "",
        "description": part.description or "",
        "qty": int(part.qty or 0),
        "photoUrl": primary_part_photo(part),
        "stockCheckedAt": part.stock_checked_at.isoformat() if part.stock_checked_at else "",
        "stockCheckedLabel": format_dt(part.stock_checked_at),
        "stockCheckStatus": part.stock_check_status or "unchecked",
        "stockCheckStatusLabel": stock_status_label(part.stock_check_status),
        "stockCheckNote": normalize_text(part.stock_check_note or ""),
    }


def clear_part_notifications(db, part_id: int):
    rows = db.query(AppNotification).filter(AppNotification.part_id == part_id, AppNotification.status == "open").all()
    for row in rows:
        db.delete(row)


def create_app_notification(db, part: Part, entered_qty: int, reason: str):
    clear_part_notifications(db, part.id)
    db.add(
        AppNotification(
            part_id=part.id,
            warehouse_id=part.warehouse_id,
            barcode=part.barcode or "",
            part_number=part.part_number or "",
            title=part.name or "",
            reason=normalize_text(reason),
            current_qty=int(part.qty or 0),
            entered_qty=int(entered_qty or 0),
            status="open",
            created_at=now(),
            updated_at=now(),
        )
    )


def serialize_part_picker_card(db, part: Part):
    warehouse = db.get(Warehouse, part.warehouse_id)
    ensure_part_barcode(db, part)
    return {
        "partId": part.id,
        "partNumber": part.part_number or "",
        "title": part.name or "",
        "brand": part.brand or "",
        "barcode": part.barcode or "",
        "qty": int(part.qty or 0),
        "warehouseId": part.warehouse_id,
        "warehouseName": warehouse.name if warehouse else "",
        "photoUrl": primary_part_photo(part),
        "priceUsd": float(part.price_usd or 0),
        "isDeleted": bool(part.is_deleted),
        "note": deleted_note(part),
    }


def availability_status_label(status: str) -> str:
    mapping = {
        "open": "Новий",
        "in_progress": "В роботі",
        "ok": "Підтверджено",
        "issue": "Є розбіжності",
        "applied": "Зміни внесено",
        "deleted": "Видалено",
    }
    return mapping.get((status or "").strip(), "Новий")


def packing_status_label(status: str) -> str:
    mapping = {
        "open": "Новий",
        "in_progress": "В роботі",
        "ready": "Чекає підтвердження",
        "packed": "Готово до пакування",
        "issue": "Є розбіжності",
        "awaiting_shipment": "Чекає відправки",
        "shipped": "Відправлено",
        "applied": "Зміни внесено",
        "deleted": "Видалено",
    }
    return mapping.get((status or "").strip(), "Новий")


def transit_status_label(status: str) -> str:
    mapping = {
        "in_transit": "В дорозі",
        "in_stock": "На складі",
        "received": "Отримано",
        "cancelled": "Скасовано",
    }
    return mapping.get((status or "").strip(), "В дорозі")


def recalc_availability_request(request_obj: AvailabilityRequest):
    items = list(request_obj.items or [])
    total = len(items)
    checked = sum(1 for item in items if item.status != "pending")
    has_issue = any(item.status in {"mismatch", "missing"} for item in items)
    request_obj.total_items = total
    request_obj.checked_items = checked
    request_obj.progress_percent = int((checked / total) * 100) if total else 0
    if total == 0:
        request_obj.status = "deleted"
        request_obj.completed_at = now()
    elif checked < total:
        request_obj.status = "in_progress" if checked else "open"
        request_obj.completed_at = None
    else:
        request_obj.status = "issue" if has_issue else "ok"
        request_obj.completed_at = now()
    request_obj.updated_at = now()


def serialize_availability_item(db, item: AvailabilityRequestItem):
    part = db.get(Part, item.part_id)
    photo_url = primary_part_photo(part) if part else ""
    barcode = part.barcode if part else ""
    return {
        "id": item.id,
        "partId": item.part_id,
        "partNumber": item.part_number or "",
        "title": item.title or "",
        "expectedQty": int(item.expected_qty or 0),
        "checkedQty": None if item.checked_qty is None else int(item.checked_qty or 0),
        "status": item.status or "pending",
        "statusLabel": {
            "pending": "Очікує",
            "found": "Підтверджено",
            "mismatch": "Розбіжність",
            "missing": "Не знайдено",
        }.get(item.status or "pending", "Очікує"),
        "note": normalize_text(item.note or ""),
        "barcode": barcode or "",
        "photoUrl": photo_url,
    }


def serialize_availability_request(db, request_obj: AvailabilityRequest):
    warehouse = db.get(Warehouse, request_obj.warehouse_id)
    items = [serialize_availability_item(db, item) for item in request_obj.items]
    issue_count = sum(1 for item in items if item["status"] in {"mismatch", "missing"})
    return {
        "id": request_obj.id,
        "title": request_obj.title or f"Запит #{request_obj.id}",
        "warehouseName": warehouse.name if warehouse else "",
        "status": request_obj.status or "open",
        "statusLabel": availability_status_label(request_obj.status),
        "progressPercent": int(request_obj.progress_percent or 0),
        "totalItems": int(request_obj.total_items or 0),
        "checkedItems": int(request_obj.checked_items or 0),
        "issueCount": issue_count,
        "createdAt": format_dt(request_obj.created_at),
        "completedAt": format_dt(request_obj.completed_at),
        "items": items,
    }


def current_availability_item(request_obj: AvailabilityRequest):
    for item in request_obj.items:
        if item.status == "pending":
            return item
    return None


def availability_mobile_payload(db, request_obj: AvailabilityRequest):
    recalc_availability_request(request_obj)
    warehouse = db.get(Warehouse, request_obj.warehouse_id)
    items = list(request_obj.items or [])
    current = current_availability_item(request_obj)
    current_index = 0
    if current:
        try:
            current_index = items.index(current) + 1
        except ValueError:
            current_index = 0
    return {
        "requestId": request_obj.id,
        "title": request_obj.title or f"Запит #{request_obj.id}",
        "warehouseId": request_obj.warehouse_id,
        "warehouseName": warehouse.name if warehouse else "",
        "status": request_obj.status or "open",
        "statusLabel": availability_status_label(request_obj.status),
        "progressPercent": int(request_obj.progress_percent or 0),
        "currentIndex": current_index,
        "total": len(items),
        "items": [serialize_availability_item(db, item) for item in items],
        "currentItem": serialize_availability_item(db, current) if current else None,
        "canRestart": request_obj.status in {"ok", "issue"},
        "completedMessage": (
            "Дякую, усі позиції підтверджено."
            if request_obj.status == "ok"
            else "Запит завершено з розбіжностями. Очікуйте рішення на сайті."
            if request_obj.status == "issue"
            else ""
        ),
    }


def recalc_packing_request(request_obj: PackingRequest):
    items = list(request_obj.items or [])
    total = len(items)
    processed = sum(1 for item in items if item.status != "pending")
    has_issue = any(item.status in {"missing", "partial"} for item in items)
    if request_obj.status in {"packed", "awaiting_shipment", "shipped", "applied", "deleted"}:
        return
    if total == 0:
        request_obj.status = "deleted"
    elif processed < total:
        request_obj.status = "in_progress" if processed else "open"
    else:
        request_obj.status = "issue" if has_issue else "ready"
        request_obj.updated_at = now()


def packing_item_barcode(db, item: PackingRequestItem) -> str:
    part = db.get(Part, item.part_id) if item.part_id else None
    if part:
        ensure_part_barcode(db, part)
        if normalize_text(part.barcode or "").strip():
            return normalize_text(part.barcode or "").strip()
    template = find_part_template(db, item.part_number or "")
    if template:
        ensure_template_barcode(db, template)
        return normalize_text(template.barcode or "").strip()
    return ""


def serialize_packing_item(db, item: PackingRequestItem):
    part = db.get(Part, item.part_id) if item.part_id else None
    template = find_part_template(db, item.part_number or "")
    photos = parse_media_urls(item.photos_json)
    barcode = packing_item_barcode(db, item)
    photo_url = primary_part_photo(part) if part else ""
    if not photo_url and template:
        photo_url = primary_template_photo(template)
    available_qty = manual_issue_available_qty(db, template)
    if available_qty <= 0 and part:
        available_qty = max(int(part.qty or 0), 0)
    return {
        "id": item.id,
        "partId": item.part_id,
        "partNumber": item.part_number or "",
        "title": item.title or "",
        "expectedQty": int(item.expected_qty or 0),
        "foundQty": int(item.found_qty or 0),
        "missingQty": int(item.missing_qty or 0),
        "status": item.status or "pending",
        "statusLabel": {
            "pending": "Очікує",
            "found": "Знайдено",
            "partial": "Частково",
            "missing": "Не знайдено",
        }.get(item.status or "pending", "Очікує"),
        "photoUrl": photo_url,
        "packedPhotos": photos,
        "barcode": barcode or "",
        "availableQty": available_qty,
    }


def serialize_packing_request(db, request_obj: PackingRequest):
    items = [serialize_packing_item(db, item) for item in request_obj.items]
    issue_count = sum(1 for item in items if item["status"] in {"partial", "missing"})
    total_qty = sum(max(int(item.get("expectedQty") or 0), 0) for item in items)
    found_qty = sum(min(max(int(item.get("foundQty") or 0), 0), max(int(item.get("expectedQty") or 0), 0)) for item in items)
    source_order = db.get(Order, request_obj.source_order_id) if request_obj.source_order_id else None
    delivery_summary = packing_request_delivery_summary(request_obj)
    return {
        "id": request_obj.id,
        "title": request_obj.comment or f"Збірка #{request_obj.id}",
        "deliveryType": request_obj.delivery_type or "pickup",
        "deliveryLabel": "Нова пошта" if request_obj.delivery_type == "nova_poshta" else "Самовивіз",
        "deliverySummary": delivery_summary,
        "status": request_obj.status or "open",
        "statusLabel": packing_status_label(request_obj.status),
        "createdAt": format_dt(request_obj.created_at),
        "updatedAt": format_dt(request_obj.updated_at),
        "customerName": request_obj.customer_name or "",
        "phone": request_obj.phone or "",
        "city": request_obj.city or "",
        "controlPaymentUah": float(request_obj.control_payment_uah or 0),
        "npServiceType": normalized_np_service_type(request_obj.np_service_type or "warehouse"),
        "npCityRef": request_obj.np_city_ref or "",
        "npWarehouseRef": request_obj.np_warehouse_ref or "",
        "npWarehouseLabel": request_obj.np_warehouse_label or "",
        "npStreetRef": request_obj.np_street_ref or "",
        "npStreetName": request_obj.np_street_name or "",
        "npHouse": request_obj.np_house or "",
        "sourceType": request_obj.source_type or "manual",
        "sourceOrderId": request_obj.source_order_id,
        "issueCount": issue_count,
        "totalQty": total_qty,
        "foundQty": found_qty,
        "items": items,
        "sourceOrderStatus": source_order.status if source_order else "",
    }


def order_group_label(group: str) -> str:
    mapping = {
        "new": "Нові",
        "active": "Активні",
        "history": "Виконані / скасовані",
    }
    return mapping.get(group or "new", "Нові")


def order_group_for_status(status: str) -> str:
    if status in {"done", "cancelled"}:
        return "history"
    if status in {"processing", "awaiting_shipment", "shipped"}:
        return "active"
    return "new"


def latest_packing_request_for_order(db, order_id: int):
    return (
        db.query(PackingRequest)
        .filter(
            PackingRequest.source_order_id == order_id,
            PackingRequest.status != "deleted",
        )
        .order_by(desc(PackingRequest.updated_at), desc(PackingRequest.id))
        .first()
    )


def infer_order_delivery_type(order: Order | None) -> str:
    if not order:
        return "pickup"
    stored_delivery_type = normalize_text(getattr(order, "delivery_type", "") or "").strip().lower()
    if stored_delivery_type == "nova_poshta":
        return "nova_poshta"
    comment = normalize_text(order.comment or "").strip().lower()
    if not comment:
        return "pickup"
    if "нова пошта" in comment:
        return "nova_poshta"
    prefix = comment.split(":", 1)[0].strip()
    if prefix in {"warehouse", "postomat", "address"}:
        return "nova_poshta"
    return "pickup"


def effective_packing_delivery_type(request_obj: PackingRequest | None, order: Order | None = None) -> str:
    if not request_obj:
        return infer_order_delivery_type(order)
    current = normalize_text(request_obj.delivery_type or "").strip().lower()
    if current == "nova_poshta":
        return "nova_poshta"
    return infer_order_delivery_type(order)


def packing_button_view(request_obj: PackingRequest | None, order: Order | None = None):
    if not request_obj:
        return {
            "tone": "violet",
            "label": "Поставити на видачу",
            "disabled": False,
            "action": "send_to_packing",
        }
    if request_obj.status == "issue":
        return {
            "tone": "danger",
            "label": "Пересорт(",
            "disabled": True,
            "action": "",
        }
    if request_obj.status == "packed":
        if effective_packing_delivery_type(request_obj, order) == "nova_poshta":
            return {
                "tone": "success",
                "label": "Відправити замовлення",
                "disabled": False,
                "action": "send_to_order",
            }
        return {
            "tone": "success",
            "label": "Готово до пакування",
            "disabled": True,
            "action": "",
        }
    if request_obj.status == "awaiting_shipment":
        return {
            "tone": "warn",
            "label": "Чекає відправки",
            "disabled": True,
            "action": "",
        }
    if request_obj.status == "shipped":
        return {
            "tone": "success",
            "label": "Відправлено",
            "disabled": True,
            "action": "",
        }
    return {
        "tone": "warn",
        "label": "Чекає підтвердження",
        "disabled": True,
        "action": "",
    }


def admin_order_capabilities(order: Order, packing_request: PackingRequest | None):
    delivery_type = effective_packing_delivery_type(packing_request, order) if packing_request else infer_order_delivery_type(order)
    request_status = packing_request.status if packing_request else ""
    locked_packing_statuses = {"packed", "awaiting_shipment", "shipped"}
    has_locked_packing = bool(packing_request and request_status in locked_packing_statuses)
    can_edit_items = order.status not in {"done", "cancelled"} and not has_locked_packing

    if packing_request:
        can_direct_ttn = delivery_type == "nova_poshta" and request_status in {"awaiting_shipment", "shipped"}
        can_complete = (
            delivery_type == "nova_poshta" and order.status == "shipped"
        ) or (
            delivery_type != "nova_poshta" and request_status in {"awaiting_shipment", "shipped"}
        )
    else:
        can_direct_ttn = delivery_type == "nova_poshta" and order.status in {"awaiting_shipment", "shipped"}
        can_complete = delivery_type == "nova_poshta" and order.status == "shipped"

    return {
        "deliveryType": delivery_type,
        "canEditItems": can_edit_items,
        "canDirectTtnEdit": can_direct_ttn,
        "canComplete": can_complete and order.status not in {"done", "cancelled"},
    }


def serialize_admin_order(db, order: Order):
    packing_request = latest_packing_request_for_order(db, order.id)
    packing_items = {}
    if packing_request:
        for item in packing_request.items:
            key = item.part_id or item.part_number
            packing_items[key] = item

    items = []
    has_issue = False
    for item in order.items:
        key = item.part_id or item.part_number
        packing_item = packing_items.get(key)
        item_status = packing_item.status if packing_item else ""
        issue = item_status in {"missing", "partial"}
        if issue:
            has_issue = True
        items.append({
            "id": item.id,
            "partId": item.part_id,
            "partNumber": item.part_number or "",
            "name": item.name or "",
            "qty": int(item.qty or 0),
            "priceUsd": float(item.price_usd or 0),
            "status": item_status or "",
            "statusLabel": {
                "missing": "Не знайдено",
                "partial": "Не вистачає",
                "found": "Знайдено",
            }.get(item_status or "", ""),
            "hasPackingState": bool(packing_item),
            "issue": issue,
            "foundQty": int(packing_item.found_qty or 0) if packing_item else 0,
            "expectedQty": int(packing_item.expected_qty or item.qty or 0) if packing_item else int(item.qty or 0),
        })

    packing_button = packing_button_view(packing_request, order)
    capabilities = admin_order_capabilities(order, packing_request)
    packing_control_payment_uah = (
        float(packing_request.control_payment_uah or 0)
        if packing_request
        else float(order.prepayment_usd or 0)
    )
    return {
        "id": order.id,
        "customerName": normalize_text(order.customer_name or ""),
        "phone": normalize_text(order.phone or ""),
        "city": normalize_text(order.city or ""),
        "comment": normalize_text(order.comment or ""),
        "status": order.status or "new",
        "statusLabel": {
            "new": "Нове",
            "processing": "В роботі",
            "awaiting_shipment": "Чекає відправки",
            "shipped": "Відправлено",
            "done": "Виконано",
            "cancelled": "Скасовано",
        }.get(order.status or "new", order.status or "new"),
        "group": order_group_for_status(order.status),
        "groupLabel": order_group_label(order_group_for_status(order.status)),
        "createdAt": format_dt(order.created_at),
        "updatedAt": format_dt(order.updated_at),
        "totalUsd": float(order.total_usd or 0),
        "prepaymentUsd": float(order.prepayment_usd or 0),
        "ttn": normalize_text(order.ttn or ""),
        "ttnStatus": normalize_text(order.ttn_status or ""),
        "cancelReason": normalize_text(order.cancel_reason or ""),
        "externalSource": normalize_text(order.external_source or ""),
        "externalStatus": normalize_text(order.external_status or ""),
        "isProcessing": bool(order.is_processing),
        "packingRequestId": packing_request.id if packing_request else None,
        "packingStatus": packing_request.status if packing_request else "",
        "packingStatusLabel": packing_status_label(packing_request.status) if packing_request else "",
        "packingDeliveryType": capabilities["deliveryType"] if packing_request else capabilities["deliveryType"],
        "packingControlPaymentUah": packing_control_payment_uah,
        "packingButton": packing_button,
        "hasIssue": has_issue,
        "canEditItems": capabilities["canEditItems"],
        "canDirectTtnEdit": capabilities["canDirectTtnEdit"],
        "canComplete": capabilities["canComplete"],
        "items": items,
    }


def serialize_transit_order(db, order: TransitOrder):
    linked_part = db.get(Part, order.linked_part_id) if order.linked_part_id else None
    template = db.get(PartTemplate, order.part_template_id) if order.part_template_id else None
    warehouse = db.get(Warehouse, linked_part.warehouse_id) if linked_part and linked_part.warehouse_id else None

    photo_url = safe_photo(order.photo_urls)
    if not photo_url and linked_part:
        photo_url = primary_part_photo(linked_part)
    if not photo_url and template:
        photo_url = primary_template_photo(template)

    source_note = ""
    if linked_part and not linked_part.is_deleted:
        source_note = f"Є в базі складів: {warehouse.name}" if warehouse else "Є в базі складів"
    elif template:
        source_note = template_note(template)
    elif linked_part and linked_part.is_deleted:
        source_note = "Є як шаблон, але не додано до складу"

    return {
        "id": order.id,
        "batchId": normalize_text(order.batch_id or "").strip(),
        "partNumber": normalize_text(order.part_number or ""),
        "barcode": normalize_text(order.barcode or ""),
        "title": normalize_text(order.title or ""),
        "serviceInfo": normalize_text(order.service_info or ""),
        "shortDescription": normalize_text(order.short_description or ""),
        "fullDescription": normalize_text(order.full_description or ""),
        "qty": int(order.qty or 0),
        "acceptedQty": int(order.accepted_qty or 0),
        "priceUsd": float(order.price_usd or 0),
        "photoUrl": photo_url,
        "hasPhoto": bool(photo_url),
        "status": order.status or "in_transit",
        "statusLabel": transit_status_label(order.status),
        "sourceNote": source_note,
        "isTemplateOnly": bool(template and not linked_part),
        "labelsPrinted": bool(order.labels_printed_at),
        "labelsPrintedAt": format_dt(order.labels_printed_at),
        "archivedAt": format_dt(order.archived_at),
        "createdDayLabel": order.created_at.strftime("%d.%m.%Y") if order.created_at else "",
        "createdAt": format_dt(order.created_at),
        "updatedAt": format_dt(order.updated_at),
    }


def normalize_transit_order_progress(db, order: TransitOrder) -> bool:
    changed = False
    ensured_barcode = ensure_transit_order_barcode(db, order)
    if ensured_barcode and order.barcode != ensured_barcode:
        order.barcode = ensured_barcode
        changed = True

    qty = max(int(order.qty or 0), 0)
    accepted_qty = max(int(order.accepted_qty or 0), 0)
    clamped_qty = min(accepted_qty, qty)
    if int(order.accepted_qty or 0) != clamped_qty:
        order.accepted_qty = clamped_qty
        changed = True

    expected_status = "in_stock" if qty > 0 and clamped_qty >= qty else "in_transit"
    if (order.status or "").strip() != expected_status:
        order.status = expected_status
        changed = True

    if changed:
        order.updated_at = now()
    return changed


def mobile_transit_orders(db):
    orders = (
        db.query(TransitOrder)
        .filter(TransitOrder.archived_at.is_(None))
        .order_by(TransitOrder.created_at.asc(), TransitOrder.id.asc())
        .all()
    )
    changed = False
    for order in orders:
        changed = normalize_transit_order_progress(db, order) or changed
    if changed:
        db.flush()
    return orders


def mobile_transit_payload(db):
    items = [serialize_transit_order(db, order) for order in mobile_transit_orders(db)]
    return {
        "ok": True,
        "hasTasks": bool(items),
        "total": len(items),
        "items": items,
    }


def build_transit_arrival_telegram_message(orders: list[TransitOrder]) -> str:
    accepted_orders = [order for order in orders if int(order.accepted_qty or 0) > int(order.arrival_notified_qty or 0)]
    positions = len(accepted_orders)
    total_delta = sum(max(int(order.accepted_qty or 0) - int(order.arrival_notified_qty or 0), 0) for order in accepted_orders)
    lines = [
        "❗ Товар прибув, додайте в склад в кабінеті!!!",
        "",
        f"Позицій: {positions}",
        f"Прийнято шт.: {total_delta}",
    ]
    for order in accepted_orders[:8]:
        delta = max(int(order.accepted_qty or 0) - int(order.arrival_notified_qty or 0), 0)
        lines.append(f"- {normalize_text(order.part_number or '').strip() or '-'} • {delta} шт.")
    if len(accepted_orders) > 8:
        lines.append(f"... ще {len(accepted_orders) - 8} поз.")
    lines.append(f"Час: {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    return "\n".join(lines)


def maybe_queue_transit_arrival_message(db, orders: list[TransitOrder]):
    active_orders = [order for order in orders if max(int(order.qty or 0), 0) > 0]
    if not active_orders:
        return False
    completed_orders = [
        order
        for order in active_orders
        if max(int(order.accepted_qty or 0), 0) >= max(int(order.qty or 0), 0)
        and max(int(order.arrival_notified_qty or 0), 0) < max(int(order.accepted_qty or 0), 0)
    ]
    if not completed_orders:
        return False
    queue_telegram_message(db, build_transit_arrival_telegram_message(completed_orders))
    for order in completed_orders:
        order.arrival_notified_qty = max(int(order.accepted_qty or 0), 0)
        order.updated_at = now()
    return True


def group_transit_orders(items: list[dict]):
    groups = []
    current_label = None
    current_items = []
    for item in items:
        label = item.get("createdDayLabel") or "Без дати"
        if label != current_label:
            if current_items:
                groups.append({"label": current_label, "items": current_items})
            current_label = label
            current_items = [item]
        else:
            current_items.append(item)
    if current_items:
        groups.append({"label": current_label, "items": current_items})
    return groups


def transit_batch_status(items: list[dict]) -> str:
    statuses = {(item.get("status") or "in_transit").strip() for item in items if item}
    if not statuses:
        return "in_transit"
    if statuses == {"in_stock"}:
        return "in_stock"
    if "in_transit" in statuses:
        return "in_transit"
    if "received" in statuses:
        return "received"
    if statuses == {"cancelled"}:
        return "cancelled"
    return next(iter(statuses))


def backfill_transit_batch_ids(db):
    updated = False
    orders = (
        db.query(TransitOrder)
        .filter((TransitOrder.batch_id == "") | (TransitOrder.batch_id.is_(None)))
        .order_by(TransitOrder.created_at.asc(), TransitOrder.id.asc())
        .all()
    )
    for order in orders:
        seed = "|".join(
            [
                order.created_at.strftime("%Y-%m-%d %H:%M") if order.created_at else "",
                normalize_text(order.short_description or "").strip(),
                normalize_text(order.full_description or "").strip(),
            ]
        )
        order.batch_id = f"legacy-{hashlib.sha1(seed.encode('utf-8', errors='ignore')).hexdigest()[:24]}"
        updated = True
    return updated


def group_transit_batches(items: list[dict]):
    batches_map = {}
    batches = []
    for item in items:
        batch_id = normalize_text(item.get("batchId") or "").strip()
        if not batch_id:
            legacy_seed = "|".join(
                [
                    normalize_text(item.get("createdAt") or "").strip(),
                    normalize_text(item.get("shortDescription") or "").strip(),
                    normalize_text(item.get("fullDescription") or "").strip(),
                ]
            )
            batch_id = f"legacy-{hashlib.sha1(legacy_seed.encode('utf-8', errors='ignore')).hexdigest()[:12]}"
        batch = batches_map.get(batch_id)
        if not batch:
            batch = {
                "batchId": batch_id,
                "shortDescription": item.get("shortDescription") or item.get("title") or "Замовлення в дорозі",
                "fullDescription": item.get("fullDescription") or "",
                "createdAt": item.get("createdAt") or "",
                "createdDayLabel": item.get("createdDayLabel") or "Без дати",
                "archivedAt": item.get("archivedAt") or "",
                "labelsPrinted": True,
                "items": [],
                "itemCount": 0,
                "totalQty": 0,
            }
            batches_map[batch_id] = batch
            batches.append(batch)
        batch["items"].append(item)
        batch["itemCount"] += 1
        batch["totalQty"] += int(item.get("qty") or 0)
        batch["labelsPrinted"] = bool(batch["labelsPrinted"] and item.get("labelsPrinted"))
        if not batch["fullDescription"] and item.get("fullDescription"):
            batch["fullDescription"] = item.get("fullDescription")
        if not batch["archivedAt"] and item.get("archivedAt"):
            batch["archivedAt"] = item.get("archivedAt")

    for batch in batches:
        batch_status = transit_batch_status(batch["items"])
        batch["status"] = batch_status
        batch["statusLabel"] = transit_status_label(batch_status)
    return group_transit_orders(batches)


def ensure_transit_order_barcode(db, order: TransitOrder) -> str:
    barcode = normalize_text(order.barcode or "").strip()
    if not barcode and order.linked_part_id:
        linked_part = db.get(Part, order.linked_part_id)
        if linked_part:
            ensure_part_barcode(db, linked_part)
            barcode = linked_part.barcode or ""
    if not barcode and order.part_template_id:
        template = db.get(PartTemplate, order.part_template_id)
        if template:
            ensure_template_barcode(db, template)
            barcode = template.barcode or ""
    if not barcode:
        barcode = barcode_from_numeric_id(7_000_000 + int(order.id or 0))
    order.barcode = barcode
    return barcode


def get_transit_draft_meta():
    raw = session.get("admin_transit_draft_meta", {})
    if not isinstance(raw, dict):
        raw = {}
    return {
        "shortDescription": normalize_text(raw.get("short_description") or "").strip(),
        "fullDescription": normalize_text(raw.get("full_description") or "").strip(),
    }


def get_transit_draft_items():
    raw_items = session.get("admin_transit_draft_items", [])
    if not isinstance(raw_items, list):
        raw_items = []
    items = []
    for raw in raw_items:
        if not isinstance(raw, dict):
            continue
        draft_id = str(raw.get("draftId") or "").strip()
        part_number = normalize_text(raw.get("partNumber") or "").strip().upper()
        title = normalize_text(raw.get("title") or "").strip()
        qty = max(int(raw.get("qty") or 0), 0)
        if not draft_id or not part_number or not title or qty <= 0:
            continue
        items.append(
            {
                "draftId": draft_id,
                "partNumber": part_number,
                "title": title,
                "serviceInfo": normalize_text(raw.get("serviceInfo") or "").strip(),
                "priceUsd": float(raw.get("priceUsd") or 0),
                "qty": qty,
                "photoUrl": normalize_text(raw.get("photoUrl") or "").strip(),
                "barcode": normalize_text(raw.get("barcode") or "").strip(),
                "sourceNote": normalize_text(raw.get("sourceNote") or "").strip(),
            }
        )
    return items


def save_transit_draft(items: list[dict], short_description: str = "", full_description: str = ""):
    session["admin_transit_draft_items"] = items
    session["admin_transit_draft_meta"] = {
        "short_description": normalize_text(short_description).strip(),
        "full_description": normalize_text(full_description).strip(),
    }
    session.modified = True


def clear_transit_draft():
    session.pop("admin_transit_draft_items", None)
    session.pop("admin_transit_draft_meta", None)
    session.modified = True


def build_transit_form_state():
    meta = get_transit_draft_meta()
    return {
        "shortDescription": meta["shortDescription"],
        "fullDescription": meta["fullDescription"],
        "partNumber": "",
        "title": "",
        "serviceInfo": "",
        "priceUsd": "0.00",
        "qty": 1,
        "existingPhotoUrl": "",
    }


def ensure_transit_template_card(
    db,
    *,
    part_number: str,
    title: str,
    service_info: str = "",
    price_usd: float = 0,
    photo_url: str = "",
    barcode: str = "",
):
    normalized = normalize_text(part_number or "").strip().upper()
    title = normalize_text(title or "").strip()
    if not normalized or not title:
        return None, False
    template, created = upsert_part_template(
        db,
        normalized,
        {
            "name": title,
            "description": normalize_text(service_info or "").strip(),
            "price_usd": float(price_usd or 0),
            "photo_urls": normalize_text(photo_url or "").strip(),
            "showcase_photo_urls": [normalize_text(photo_url or "").strip()] if normalize_text(photo_url or "").strip() else [],
        },
    )
    if barcode and not (template.barcode or "").strip():
        template.barcode = normalize_text(barcode).strip()
    ensure_template_barcode(db, template)
    return template, created


def prepare_transit_item_input(
    db,
    *,
    part_number_value,
    title_value,
    service_info_value,
    price_value,
    qty_value,
    existing_photo_url="",
    photo_file=None,
    upload_suffix="new",
):
    part_number = normalize_text(part_number_value or "").strip().upper()
    title = normalize_text(title_value or "").strip()
    service_info = normalize_text(service_info_value or "").strip()
    price_usd = float(price_value or 0)
    qty = max(int(qty_value or 0), 0)
    existing_photo_url = normalize_text(existing_photo_url or "").strip()
    uploaded_photo_url = save_upload(photo_file, f"transit_{upload_suffix or part_number or 'new'}") if photo_file else ""
    source_part, template = resolve_transit_source(db, part_number)

    if source_part:
        if not title:
            title = normalize_text(source_part.name or "").strip()
        if not service_info:
            service_info = normalize_text(source_part.description or "").strip()
        if not price_usd:
            price_usd = float(source_part.price_usd or 0)
    elif template:
        if not title:
            title = normalize_text(template.name or "").strip()
        if not service_info:
            service_info = normalize_text(template.description or "").strip()
        if not price_usd:
            price_usd = float(template.price_usd or 0)

    photo_url = uploaded_photo_url or existing_photo_url
    if not photo_url and source_part:
        photo_url = primary_part_photo(source_part)
    if not photo_url and template:
        photo_url = primary_template_photo(template)

    if not part_number or not title or qty <= 0:
        raise ValueError("invalid_transit_item")

    if not source_part and not template:
        template, _ = ensure_transit_template_card(
            db,
            part_number=part_number,
            title=title,
            service_info=service_info,
            price_usd=price_usd,
            photo_url=photo_url,
        )

    source_note = ""
    if source_part:
        warehouse = db.get(Warehouse, source_part.warehouse_id) if source_part.warehouse_id else None
        source_note = f"Склад: {warehouse.name}" if warehouse else "Знайдено в складі"
    elif template:
        source_note = template_note(template)

    return {
        "partNumber": part_number,
        "title": title,
        "serviceInfo": service_info,
        "priceUsd": float(price_usd or 0),
        "qty": qty,
        "photoUrl": photo_url or "",
        "barcode": (source_part.barcode if source_part else template.barcode if template else "") or "",
        "sourceNote": source_note,
    }


def persist_transit_order(db, item: dict, short_description: str, full_description: str, batch_id: str):
    part_number = normalize_text(item.get("partNumber") or "").strip().upper()
    title = normalize_text(item.get("title") or "").strip()
    service_info = normalize_text(item.get("serviceInfo") or "").strip()
    price_usd = float(item.get("priceUsd") or 0)
    qty = max(int(item.get("qty") or 0), 0)
    photo_url = normalize_text(item.get("photoUrl") or "").strip()
    if not short_description:
        short_description = " / ".join(part for part in [part_number, title] if part)
    source_part, template = resolve_transit_source(db, part_number)
    linked_part_id = source_part.id if source_part else None
    template_id = None
    barcode = source_part.barcode if source_part else ""

    if not source_part:
        template, created = upsert_part_template(
            db,
            part_number,
            {
                "name": title,
                "description": service_info,
                "price_usd": price_usd,
                "photo_urls": photo_url,
                "showcase_photo_urls": [photo_url] if photo_url else [],
            },
        )
        template_id = template.id if template else None
        barcode = template.barcode if template else ""
        flash_news(
            db,
            "transit",
            "Оновлено шаблон товару",
            (
                f"Створено шаблон {part_number}, який ще не додано до складу."
                if created
                else f"Шаблон {part_number} оновлено через вкладку товару в дорозі."
            ),
            "info",
        )
    elif not photo_url:
        photo_url = primary_part_photo(source_part)

    order = TransitOrder(
        batch_id=batch_id,
        part_template_id=template_id,
        linked_part_id=linked_part_id,
        part_number=part_number,
        barcode=barcode or "",
        title=title,
        service_info=service_info,
        short_description=short_description,
        full_description=full_description,
        qty=qty,
        price_usd=price_usd,
        photo_urls=photo_url or "",
        has_photo=bool(photo_url),
        status="in_transit",
        created_at=now(),
        updated_at=now(),
    )
    db.add(order)
    return order


def apply_transit_arrival_to_part(db, order: TransitOrder):
    accepted_qty = max(int(order.accepted_qty or 0), 0)
    part_number = normalize_text(order.part_number or "").strip().upper()
    if accepted_qty <= 0 or not part_number:
        return None, 0

    target_part = db.get(Part, order.linked_part_id) if order.linked_part_id else None
    if target_part and normalize_text(target_part.part_number or "").strip().upper() != part_number:
        target_part = None
    if not target_part:
        target_part = find_part_prefill(db, part_number, None)
    order_photo = normalize_text(order.photo_urls or "").strip()
    if target_part:
        base_qty = 0 if target_part.is_deleted else max(int(target_part.qty or 0), 0)
        target_part.is_deleted = False
        target_part.deleted_at = None
        target_part.qty = base_qty + accepted_qty
        target_part.in_stock = target_part.qty > 0
        if not normalize_text(target_part.name or "").strip():
            target_part.name = normalize_text(order.title or "").strip() or part_number
        if not normalize_text(target_part.description or "").strip():
            target_part.description = normalize_text(order.service_info or "").strip()
        if not float(target_part.price_usd or 0):
            target_part.price_usd = float(order.price_usd or 0)
        if order_photo and not safe_photo(target_part.photo_urls):
            target_part.photo_urls = order_photo
        if order_photo and not parse_media_urls(target_part.showcase_photo_urls):
            target_part.showcase_photo_urls = dump_media_urls([order_photo])
        if normalize_text(order.barcode or "").strip() and not normalize_text(target_part.barcode or "").strip():
            target_part.barcode = normalize_text(order.barcode).strip()
        target_part.has_photo = bool(primary_part_photo(target_part))
        target_part.has_description = bool((target_part.description or "").strip())
        target_part.updated_at = now()
        ensure_part_barcode(db, target_part)
        sync_template_from_part(db, target_part)
        order.linked_part_id = target_part.id
        queue_part_inventory_change(
            db,
            target_part,
            base_qty,
            context_label="Товар в дорозі → Товар прибув",
            reason=f"Прийнято {accepted_qty} шт.",
        )
        return target_part, accepted_qty

    target_template = db.get(PartTemplate, order.part_template_id) if order.part_template_id else None
    if target_template and normalize_text(target_template.part_number or "").strip().upper() != part_number:
        target_template = None
    if not target_template:
        target_template = find_part_template(db, part_number)
    if not target_template:
        target_template, _ = ensure_transit_template_card(
            db,
            part_number=part_number,
            title=normalize_text(order.title or "").strip() or part_number,
            service_info=normalize_text(order.service_info or "").strip(),
            price_usd=float(order.price_usd or 0),
            photo_url=order_photo,
            barcode=normalize_text(order.barcode or "").strip(),
        )
    if not target_template:
        return None, 0

    before_template_qty = template_unassigned_qty(target_template)
    target_template.unassigned_qty = before_template_qty + accepted_qty
    if not normalize_text(target_template.name or "").strip():
        target_template.name = normalize_text(order.title or "").strip() or part_number
    if not normalize_text(target_template.description or "").strip():
        target_template.description = normalize_text(order.service_info or "").strip()
    if not float(target_template.price_usd or 0):
        target_template.price_usd = float(order.price_usd or 0)
    if order_photo and not safe_photo(target_template.photo_urls):
        target_template.photo_urls = order_photo
    if order_photo and not parse_media_urls(target_template.showcase_photo_urls):
        target_template.showcase_photo_urls = dump_media_urls([order_photo])
    if normalize_text(order.barcode or "").strip() and not normalize_text(target_template.barcode or "").strip():
        target_template.barcode = normalize_text(order.barcode).strip()
    target_template.has_photo = bool(primary_template_photo(target_template))
    target_template.has_description = bool((target_template.description or "").strip())
    target_template.updated_at = now()
    ensure_template_barcode(db, target_template)
    order.part_template_id = target_template.id
    queue_template_inventory_change(
        db,
        target_template,
        before_template_qty,
        context_label="Товар в дорозі → Товар прибув",
        reason=f"Прийнято {accepted_qty} шт.",
    )
    return target_template, accepted_qty


def current_packing_item(request_obj: PackingRequest):
    for item in request_obj.items:
        if item.status == "pending":
            return item
    return None


def packing_mobile_payload(db, request_obj: PackingRequest):
    recalc_packing_request(request_obj)
    items = list(request_obj.items or [])
    current = current_packing_item(request_obj)
    current_index = 0
    if current:
        try:
            current_index = items.index(current) + 1
        except ValueError:
            current_index = 0
    issue_count = sum(1 for item in items if item.status in {"missing", "partial"})
    return {
        "requestId": request_obj.id,
        "title": request_obj.comment or f"Збірка #{request_obj.id}",
        "status": request_obj.status or "open",
        "statusLabel": packing_status_label(request_obj.status),
        "deliveryType": request_obj.delivery_type or "pickup",
        "deliveryLabel": "Нова пошта" if request_obj.delivery_type == "nova_poshta" else "Самовивіз",
        "currentIndex": current_index,
        "total": len(items),
        "issueCount": issue_count,
        "items": [serialize_packing_item(db, item) for item in items],
        "currentItem": serialize_packing_item(db, current) if current else None,
        "canMarkPacked": request_obj.status == "ready",
        "message": (
            "Усі позиції знайдено. Натисніть «Видати»."
            if request_obj.status == "ready"
            else "Є розбіжності. Очікуйте рішення в кабінеті."
            if request_obj.status == "issue"
            else ""
        ),
    }


def issue_note_label(request_obj: PackingRequest) -> str:
    return "ВИДАЧА" if (request_obj.source_type or "manual") == "manual" else "НП"


def issue_note_hint(request_obj: PackingRequest) -> str:
    return "Стоїть на видачі" if (request_obj.source_type or "manual") == "manual" else "Список замовлень"


def serialize_issue_request(db, request_obj: PackingRequest):
    payload = packing_mobile_payload(db, request_obj)
    items = payload.get("items") or []
    total_qty = sum(max(int(item.get("expectedQty") or 0), 0) for item in items)
    found_qty = sum(min(max(int(item.get("foundQty") or 0), 0), max(int(item.get("expectedQty") or 0), 0)) for item in items)
    return {
        "id": request_obj.id,
        "title": payload.get("title") or f"Збірка #{request_obj.id}",
        "noteLabel": issue_note_label(request_obj),
        "noteHint": issue_note_hint(request_obj),
        "status": payload.get("status") or "open",
        "statusLabel": payload.get("statusLabel") or packing_status_label(request_obj.status),
        "deliveryType": payload.get("deliveryType") or (request_obj.delivery_type or "pickup"),
        "deliveryLabel": payload.get("deliveryLabel") or ("Нова пошта" if request_obj.delivery_type == "nova_poshta" else "Самовивіз"),
        "createdAt": format_dt(request_obj.created_at),
        "updatedAt": format_dt(request_obj.updated_at),
        "customerName": request_obj.customer_name or "",
        "phone": request_obj.phone or "",
        "city": request_obj.city or "",
        "sourceType": request_obj.source_type or "manual",
        "totalQty": total_qty,
        "foundQty": found_qty,
        "itemsCount": len(items),
        "issueCount": payload.get("issueCount") or 0,
        "items": items,
        "currentItem": payload.get("currentItem"),
        "message": payload.get("message") or "",
    }


def mobile_issue_requests(db):
    requests = (
        db.query(PackingRequest)
        .filter(PackingRequest.status != "deleted")
        .order_by(PackingRequest.created_at.asc(), PackingRequest.id.asc())
        .all()
    )
    changed = False
    visible_requests = []
    for request_obj in requests:
        before_status = request_obj.status
        before_updated = request_obj.updated_at
        recalc_packing_request(request_obj)
        if request_obj.status != before_status or request_obj.updated_at != before_updated:
            changed = True
        if request_obj.status in {"deleted", "packed", "awaiting_shipment", "shipped", "applied"}:
            continue
        if (request_obj.source_type or "manual") == "order":
            source_order = db.get(Order, request_obj.source_order_id) if request_obj.source_order_id else None
            if not source_order or order_group_for_status(source_order.status) == "history":
                continue
        visible_requests.append(request_obj)
    visible_requests.sort(
        key=lambda request_obj: (
            {"open": 0, "in_progress": 1, "ready": 2, "issue": 3}.get(request_obj.status or "open", 9),
            -int((request_obj.updated_at or request_obj.created_at or now()).timestamp()),
            -int(request_obj.id or 0),
        )
    )
    if changed:
        db.flush()
    return visible_requests


def mobile_issue_payload(db):
    requests = [serialize_issue_request(db, request_obj) for request_obj in mobile_issue_requests(db)]
    return {
        "ok": True,
        "hasTasks": bool(requests),
        "total": len(requests),
        "requests": requests,
    }


def parse_control_payment_amount(value):
    raw_value = normalize_text(value or "").strip().replace(" ", "").replace(",", ".")
    if not raw_value:
        return None
    try:
        amount = round(float(raw_value), 2)
    except (TypeError, ValueError):
        return None
    if amount < 0:
        return None
    return amount


def serialize_shipment_request(db, request_obj: PackingRequest):
    order = db.get(Order, request_obj.source_order_id) if request_obj.source_order_id else None
    items = [serialize_packing_item(db, item) for item in request_obj.items or []]
    total_qty = sum(max(int(item.get("expectedQty") or 0), 0) for item in items)
    control_payment_uah = float(order.prepayment_usd or 0) if order else float(request_obj.control_payment_uah or 0)
    order_np = order_np_payload(order) if order else empty_np_payload()
    city_name = normalize_text(request_obj.city or (order.city if order else "")).strip()
    np_service_type = normalized_np_service_type(request_obj.np_service_type or order_np.get("service_type", "warehouse"))
    np_warehouse_label = normalize_text(request_obj.np_warehouse_label or order_np.get("warehouse_label", "")).strip()
    np_street_name = normalize_text(request_obj.np_street_name or order_np.get("street_name", "")).strip()
    np_house = normalize_text(request_obj.np_house or order_np.get("house", "")).strip()
    if np_service_type == "address":
        np_target = ", ".join(item for item in [np_street_name, np_house] if item)
    else:
        np_target = np_warehouse_label
    delivery_summary = packing_request_delivery_summary(request_obj) or " • ".join(item for item in [city_name, np_target] if item)
    return {
        "id": request_obj.id,
        "orderId": order.id if order else 0,
        "title": request_obj.comment or f"Замовлення #{request_obj.id}",
        "status": request_obj.status or "awaiting_shipment",
        "statusLabel": packing_status_label(request_obj.status),
        "createdAt": format_dt(request_obj.created_at),
        "updatedAt": format_dt(request_obj.updated_at),
        "customerName": normalize_text(request_obj.customer_name or (order.customer_name if order else "")).strip(),
        "phone": normalize_text(request_obj.phone or (order.phone if order else "")).strip(),
        "city": city_name,
        "deliveryLabel": "Нова пошта",
        "deliverySummary": delivery_summary,
        "npServiceType": np_service_type,
        "npBranchLabel": np_branch_label_from_values(np_service_type, np_warehouse_label),
        "npWarehouseLabel": np_warehouse_label,
        "npStreetName": np_street_name,
        "npHouse": np_house,
        "controlPaymentUah": control_payment_uah,
        "ttn": normalize_text(order.ttn if order else "").strip(),
        "ttnStatus": normalize_text(order.ttn_status if order else "").strip(),
        "totalQty": total_qty,
        "items": items,
    }


def mobile_shipment_requests(db):
    requests = (
        db.query(PackingRequest)
        .filter(
            PackingRequest.status == "awaiting_shipment",
            PackingRequest.delivery_type == "nova_poshta",
        )
        .order_by(desc(PackingRequest.updated_at), desc(PackingRequest.id))
        .all()
    )
    visible_requests = []
    for request_obj in requests:
        source_order = db.get(Order, request_obj.source_order_id) if request_obj.source_order_id else None
        if source_order and order_group_for_status(source_order.status) == "history":
            continue
        visible_requests.append(request_obj)
    return visible_requests


def mobile_shipment_payload(db):
    requests = [serialize_shipment_request(db, request_obj) for request_obj in mobile_shipment_requests(db)]
    return {
        "ok": True,
        "hasTasks": bool(requests),
        "total": len(requests),
        "requests": requests,
    }


def find_template_for_manual_issue(db, barcode: str):
    normalized_barcode = re.sub(r"\D", "", barcode or "")
    if len(normalized_barcode) != 8:
        return None

    template = (
        db.query(PartTemplate)
        .filter(PartTemplate.barcode == normalized_barcode)
        .order_by(desc(PartTemplate.updated_at), PartTemplate.id.asc())
        .first()
    )
    if template:
        ensure_template_barcode(db, template)
        return template

    generated_template, generated_part = find_generated_barcode_match(db, normalized_barcode)
    if generated_template:
        return generated_template
    if generated_part:
        template = sync_template_from_part(db, generated_part) or find_part_template(db, generated_part.part_number or "")
        if template:
            ensure_template_barcode(db, template)
            return template

    part = (
        db.query(Part)
        .filter(Part.barcode == normalized_barcode, Part.is_deleted == False)
        .order_by(desc(Part.updated_at), Part.id.asc())
        .first()
    )
    if not part:
        return None

    ensure_part_barcode(db, part)
    template = sync_template_from_part(db, part) or find_part_template(db, part.part_number or "")
    if template and not normalize_text(template.barcode or "").strip():
        template.barcode = normalized_barcode
        ensure_template_barcode(db, template)
    return template


def find_manual_issue_template_by_code(db, raw_code: str):
    normalized_code = normalize_text(raw_code or "").strip().upper()
    if not normalized_code:
        return None

    def add_candidate(bucket, value):
        clean_value = normalize_text(value or "").strip().upper()
        if clean_value and clean_value not in bucket:
            bucket.append(clean_value)

    candidates = []
    compact_code = re.sub(r"[\s\r\n\t\u001d]+", "", normalized_code)
    add_candidate(candidates, normalized_code)
    add_candidate(candidates, compact_code)
    for prefix in ("]C1", "]E0", "]D2", "]Q3"):
        if compact_code.startswith(prefix):
            add_candidate(candidates, compact_code[len(prefix):])
    for token in re.findall(r"[A-Z0-9]{4,}", compact_code):
        add_candidate(candidates, token)
    for token in re.findall(r"\d{8}", compact_code):
        add_candidate(candidates, token)

    for candidate in candidates:
        digits = re.sub(r"\D", "", candidate)
        template = find_template_for_manual_issue(db, digits) if len(digits) == 8 else None
        if template:
            return template

        template, _ = find_part_template_or_cross(db, candidate)
        if template:
            ensure_template_barcode(db, template)
            return template

        compact_candidate = re.sub(r"[^A-Z0-9]", "", candidate)
        template = (
            db.query(PartTemplate)
            .filter(
                func.replace(func.replace(func.upper(PartTemplate.part_number), " ", ""), "-", "") == compact_candidate
            )
            .order_by(desc(PartTemplate.updated_at), PartTemplate.id.asc())
            .first()
        )
        if template:
            ensure_template_barcode(db, template)
            return template

        part = (
            db.query(Part)
            .filter(func.upper(Part.part_number) == candidate, Part.is_deleted == False)
            .order_by(desc(Part.updated_at), Part.id.asc())
            .first()
        )
        if not part and compact_candidate:
            part = (
                db.query(Part)
                .filter(
                    func.replace(func.replace(func.upper(Part.part_number), " ", ""), "-", "") == compact_candidate,
                    Part.is_deleted == False,
                )
                .order_by(desc(Part.updated_at), Part.id.asc())
                .first()
            )
        if not part:
            continue

        ensure_part_barcode(db, part)
        template = sync_template_from_part(db, part) or find_part_template(db, part.part_number or "")
        if template:
            ensure_template_barcode(db, template)
            return template
    return None


def manual_issue_parts(db, part_number: str):
    normalized = normalize_text(part_number or "").strip().upper()
    if not normalized:
        return []
    return (
        db.query(Part)
        .filter(func.upper(Part.part_number) == normalized, Part.is_deleted == False)
        .order_by(desc(Part.in_stock), desc(Part.qty), desc(Part.updated_at), Part.id.asc())
        .all()
    )


def manual_issue_available_qty(db, template: PartTemplate | None) -> int:
    if not template:
        return 0
    stock_qty = sum(max(int(part.qty or 0), 0) for part in manual_issue_parts(db, template.part_number or ""))
    return template_unassigned_qty(template) + stock_qty


def manual_issue_primary_part(db, template: PartTemplate | None):
    if not template:
        return None
    parts = manual_issue_parts(db, template.part_number or "")
    if not parts:
        return None
    return next((part for part in parts if max(int(part.qty or 0), 0) > 0), parts[0])


def manual_issue_title(template: PartTemplate | None, fallback: str = "") -> str:
    return (
        normalize_text(template.name if template else "").strip()
        or normalize_text(template.description if template else "").strip()
        or normalize_text(template.part_number if template else "").strip()
        or normalize_text(fallback).strip()
        or "Товар"
    )


def resolve_manual_issue_items(db, items_payload):
    resolved_items: dict[str, dict] = {}
    for raw in items_payload or []:
        try:
            qty = max(int(raw.get("qty") or 0), 0)
        except Exception:
            qty = 0
        if qty <= 0:
            continue

        barcode = re.sub(r"\D", "", raw.get("barcode", "") or "")
        part_number = normalize_text(raw.get("partNumber") or "").strip().upper()
        template = find_template_for_manual_issue(db, barcode) if len(barcode) == 8 else None
        if not template and part_number:
            template, _ = find_part_template_or_cross(db, part_number)

        if not template:
            raise ValueError(f"item_not_found:{part_number or barcode}")

        key = normalize_text(template.part_number or "").strip().upper()
        requested_qty = qty + int((resolved_items.get(key) or {}).get("qty") or 0)
        available_qty = manual_issue_available_qty(db, template)
        if available_qty < requested_qty:
            raise ValueError(f"not_enough:{template.part_number}:{available_qty}")

        resolved_items[key] = {
            "key": key,
            "template": template,
            "qty": requested_qty,
            "available_qty": available_qty,
            "primary_part": manual_issue_primary_part(db, template),
            "title": manual_issue_title(template),
        }

    if not resolved_items:
        raise ValueError("items_required")

    return list(resolved_items.values())


def consume_manual_issue_stock(db, template: PartTemplate | None, qty: int):
    if not template:
        raise ValueError("item_not_found:")
    remaining = max(int(qty or 0), 0)
    if remaining <= 0:
        return

    available_qty = manual_issue_available_qty(db, template)
    if available_qty < remaining:
        raise ValueError(f"not_enough:{template.part_number}:{available_qty}")

    template_qty = template_unassigned_qty(template)
    if template_qty > 0:
        take_from_template = min(template_qty, remaining)
        template.unassigned_qty = template_qty - take_from_template
        queue_template_inventory_change(
            db,
            template,
            template_qty,
            context_label="Видано вручну",
            reason=f"Списано {take_from_template} шт.",
        )
        remaining -= take_from_template

    if remaining > 0:
        for part in manual_issue_parts(db, template.part_number or ""):
            current_qty = max(int(part.qty or 0), 0)
            if current_qty <= 0:
                continue
            take_from_part = min(current_qty, remaining)
            part.qty = current_qty - take_from_part
            part.in_stock = bool(part.qty > 0)
            part.updated_at = now()
            queue_part_inventory_change(
                db,
                part,
                current_qty,
                context_label=f"Видано вручну • {part.warehouse.name if part.warehouse else 'Без складу'}",
                reason=f"Списано {take_from_part} шт.",
            )
            remaining -= take_from_part
            if remaining <= 0:
                break

    template.updated_at = now()
    if remaining > 0:
        raise ValueError(f"not_enough:{template.part_number}:{available_qty}")


def serialize_manual_issue_item(db, template: PartTemplate):
    ensure_template_barcode(db, template)
    title = manual_issue_title(template)
    return {
        "partNumber": normalize_text(template.part_number or "").strip(),
        "barcode": normalize_text(template.barcode or "").strip(),
        "title": title,
        "availableQty": manual_issue_available_qty(db, template),
        "priceUsd": float(template.price_usd or 0),
    }


def create_manual_issue_order(db, destination: str, items_payload):
    destination_text = normalize_text(destination or "").strip()
    if not destination_text:
        raise ValueError("destination_required")

    resolved_items = resolve_manual_issue_items(db, items_payload)
    total_usd = 0.0

    order = Order(
        customer_name=destination_text or "Видано вручну",
        phone="",
        city="",
        comment="Видано вручну",
        total_usd=0,
        status="done",
        is_processing=False,
        prepayment_usd=0,
        ttn="",
        ttn_status="",
        cancel_reason="",
        stock_reserved=False,
        external_source="",
        external_order_id="",
        external_status="",
        created_at=now(),
        updated_at=now(),
    )
    db.add(order)
    db.flush()

    for resolved in resolved_items:
        template = resolved["template"]
        qty = int(resolved["qty"] or 0)
        item_title = resolved["title"]
        price_usd = float(template.price_usd or 0)
        consume_manual_issue_stock(db, template, qty)
        total_usd += price_usd * qty
        order.items.append(
            OrderItem(
                part_id=None,
                part_number=normalize_text(template.part_number or "").strip(),
                name=item_title,
                qty=qty,
                price_usd=price_usd,
            )
        )

    order.total_usd = total_usd
    order.updated_at = now()
    flash_news(
        db,
        "orders",
        "Товар видано вручну",
        f"Створено запис #{order.id}: {destination_text}. Видано вручну {sum(item.qty for item in order.items)} шт.",
        "success",
    )
    return order


def create_manual_issue_request(db, destination: str, items_payload):
    destination_text = normalize_text(destination or "").strip()
    if not destination_text:
        destination_text = "Без опису"

    resolved_items = resolve_manual_issue_items(db, items_payload)
    request_obj = PackingRequest(
        source_type="manual",
        source_order_id=None,
        delivery_type="pickup",
        status="open",
        customer_name="Видано вручну",
        phone="",
        city="",
        comment=destination_text,
        created_at=now(),
        updated_at=now(),
    )
    db.add(request_obj)
    db.flush()

    total_qty = 0
    for resolved in resolved_items:
        template = resolved["template"]
        qty = int(resolved["qty"] or 0)
        primary_part = resolved["primary_part"]
        total_qty += qty
        request_obj.items.append(
            PackingRequestItem(
                request_id=request_obj.id,
                part_id=primary_part.id if primary_part else None,
                part_number=normalize_text(template.part_number or "").strip(),
                title=resolved["title"],
                expected_qty=qty,
                found_qty=qty,
                missing_qty=0,
                status="found",
                photos_json="[]",
                created_at=now(),
                updated_at=now(),
            )
        )

    recalc_packing_request(request_obj)
    request_obj.updated_at = now()
    flash_news(
        db,
        "orders",
        "Створено заявку вручну",
        f"Стоїть на видачі #{request_obj.id}: {destination_text}. Додано {total_qty} шт.",
        "success",
    )
    return request_obj


def packing_request_writeoff_payload(request_obj: PackingRequest, force_full: bool = False):
    payload = []
    for item in request_obj.items or []:
        expected_qty = max(int(item.expected_qty or 0), 0)
        found_qty = min(max(int(item.found_qty or 0), 0), expected_qty)
        qty = expected_qty if force_full else found_qty
        if qty <= 0:
            continue
        payload.append(
            {
                "partNumber": normalize_text(item.part_number or "").strip().upper(),
                "qty": qty,
            }
        )
    return payload


def resolve_packing_request_template(db, item: PackingRequestItem):
    template = find_part_template(db, item.part_number or "")
    if template:
        return template
    part = db.get(Part, item.part_id) if item.part_id else None
    if part:
        return sync_template_from_part(db, part)
    return None


def reserve_packing_request_inventory(db, request_obj: PackingRequest) -> int:
    reserved_units = 0
    for packing_item in request_obj.items or []:
        qty = max(int(packing_item.found_qty or packing_item.expected_qty or 0), 0)
        if qty <= 0:
            continue
        source_part = db.get(Part, packing_item.part_id) if packing_item.part_id else None
        if source_part:
            current_qty = max(int(source_part.qty or 0), 0)
            if current_qty < qty:
                raise ValueError(f"not_enough:{packing_item.part_number or packing_item.id}:{current_qty}")
            source_part.qty = current_qty - qty
            source_part.in_stock = int(source_part.qty or 0) > 0
            source_part.updated_at = now()
            reserved_units += qty
            continue

        template = resolve_packing_request_template(db, packing_item)
        if not template:
            raise ValueError(f"item_not_found:{packing_item.part_number or packing_item.id}")
        available_qty = template_unassigned_qty(template)
        if available_qty < qty:
            raise ValueError(f"not_enough:{template.part_number}:{available_qty}")
        template.unassigned_qty = available_qty - qty
        template.updated_at = now()
        reserved_units += qty
    return reserved_units


def append_order_note(comment: str, note: str) -> str:
    base = normalize_text(comment or "").strip()
    clean_note = normalize_text(note or "").strip()
    if not clean_note:
        return base
    if clean_note.lower() in base.lower():
        return base
    return f"{base} • {clean_note}" if base else clean_note


def complete_issue_request(db, request_obj: PackingRequest):
    pending_items = [
        packing_item
        for packing_item in (request_obj.items or [])
        if max(int(packing_item.expected_qty or 0), 0) > 0
        and int(packing_item.found_qty or 0) < int(packing_item.expected_qty or 0)
    ]
    if pending_items:
        raise ValueError("not_fully_scanned")

    resolved_items = []
    for packing_item in request_obj.items or []:
        qty = max(int(packing_item.found_qty or 0), 0)
        if qty <= 0:
            continue
        template = resolve_packing_request_template(db, packing_item)
        if not template:
            raise ValueError(f"item_not_found:{packing_item.part_number or packing_item.id}")
        source_part = db.get(Part, packing_item.part_id) if packing_item.part_id else None
        title = normalize_text(packing_item.title or template.name or template.description or template.part_number).strip() or "Товар"
        price_usd = float(template.price_usd or 0)
        resolved_items.append(
            {
                "key": normalize_text(template.part_number or "").strip().upper(),
                "part_number": normalize_text(template.part_number or "").strip(),
                "title": title,
                "qty": qty,
                "price_usd": price_usd,
                "template": template,
                "part": source_part,
            }
        )
    if not resolved_items:
        raise ValueError("nothing_scanned")

    total_usd = sum(float(resolved["price_usd"]) * resolved["qty"] for resolved in resolved_items)
    is_manual_request = (request_obj.source_type or "manual") == "manual"
    note_text = "готово до пакування"
    order = db.get(Order, request_obj.source_order_id) if request_obj.source_order_id else None
    stock_already_reserved = bool(order and order.stock_reserved)

    if is_manual_request or not stock_already_reserved:
        for resolved in resolved_items:
            template = resolved["template"]
            source_part = resolved.get("part")
            qty = resolved["qty"]
            if source_part:
                source_part.qty = max(int(source_part.qty or 0) - qty, 0)
                source_part.in_stock = int(source_part.qty or 0) > 0
                source_part.updated_at = now()
            else:
                template.unassigned_qty = max(template_unassigned_qty(template) - qty, 0)
            template.updated_at = now()

        if order:
            by_key = {resolved["key"]: resolved for resolved in resolved_items}
            seen_keys = set()
            for order_item in list(order.items):
                key = normalize_text(order_item.part_number or "").strip().upper()
                resolved = by_key.get(key)
                if not resolved:
                    db.delete(order_item)
                    continue
                source_part = resolved.get("part")
                order_item.part_id = source_part.id if source_part else order_item.part_id
                order_item.qty = int(resolved["qty"])
                order_item.price_usd = float(resolved["price_usd"])
                order_item.name = normalize_text(resolved["title"]).strip() or order_item.name or order_item.part_number or "Товар"
                seen_keys.add(key)
            for resolved in resolved_items:
                if resolved["key"] in seen_keys:
                    continue
                source_part = resolved.get("part")
                order.items.append(
                    OrderItem(
                        part_id=source_part.id if source_part else None,
                        part_number=resolved["part_number"],
                        name=resolved["title"],
                        qty=int(resolved["qty"]),
                        price_usd=float(resolved["price_usd"]),
                    )
                )
            order.total_usd = total_usd
            order.status = "processing"
            order.is_processing = True
            order.stock_reserved = True
            order.comment = append_order_note(order.comment or "", note_text)
            apply_np_payload_to_order(order, request_obj.delivery_type or infer_order_delivery_type(order), packing_request_np_payload(request_obj))
            order.updated_at = now()
        else:
            order = Order(
                customer_name=normalize_text(request_obj.customer_name or "").strip() or "Клієнт",
                phone=normalize_text(request_obj.phone or "").strip(),
                city=normalize_text(request_obj.city or "").strip(),
                comment=note_text,
                total_usd=total_usd,
                status="processing",
                is_processing=True,
                prepayment_usd=0,
                ttn="",
                ttn_status="",
                cancel_reason="",
                stock_reserved=True,
                external_source="",
                external_order_id="",
                external_status="",
                created_at=now(),
                updated_at=now(),
            )
            apply_np_payload_to_order(order, request_obj.delivery_type or "pickup", packing_request_np_payload(request_obj))
            db.add(order)
            db.flush()
            for resolved in resolved_items:
                source_part = resolved.get("part")
                order.items.append(
                    OrderItem(
                        part_id=source_part.id if source_part else None,
                        part_number=resolved["part_number"],
                        name=resolved["title"],
                        qty=int(resolved["qty"]),
                        price_usd=float(resolved["price_usd"]),
                    )
                )
            request_obj.source_order_id = order.id

    request_obj.status = "packed"
    request_obj.updated_at = now()

    if is_manual_request:
        flash_news(
            db,
            "orders",
            "Заявку перевірено",
            f"Заявку #{request_obj.id} перевірено. Підтверджено {sum(item['qty'] for item in resolved_items)} шт.",
            "success",
        )
    else:
        flash_news(
            db,
            "orders",
            "Готово до пакування",
            f"Замовлення #{order.id if order else request_obj.id} готове до пакування. Підтверджено {sum(item['qty'] for item in resolved_items)} шт.",
            "success",
        )
    return order


def normalized_delivery_type(value: str) -> str:
    return "nova_poshta" if (value or "").strip() == "nova_poshta" else "pickup"


def normalized_np_service_type(value: str) -> str:
    current = (value or "").strip().lower()
    if current in {"warehouse", "postomat", "address"}:
        return current
    return "warehouse"


def empty_np_payload() -> dict:
    return {
        "service_type": "warehouse",
        "city_name": "",
        "city_ref": "",
        "warehouse_ref": "",
        "warehouse_label": "",
        "street_ref": "",
        "street_name": "",
        "house": "",
    }


def order_np_service_type(order: Order | None) -> str:
    if not order:
        return "warehouse"
    stored = normalize_text(getattr(order, "np_service_type", "") or "").strip()
    if stored:
        return normalized_np_service_type(stored)
    comment = normalize_text(order.comment or "").strip().lower()
    prefix = comment.split(":", 1)[0].strip()
    if prefix in {"warehouse", "postomat", "address"}:
        return normalized_np_service_type(prefix)
    if "поштомат" in comment:
        return "postomat"
    if "адрес" in comment:
        return "address"
    return "warehouse"


def extract_order_np_target_from_comment(order: Order | None) -> str:
    if not order:
        return ""
    comment = normalize_text(order.comment or "").strip()
    if not comment:
        return ""
    lower_comment = comment.lower()
    prefix = lower_comment.split(":", 1)[0].strip()
    if prefix in {"warehouse", "postomat", "address"} and ":" in comment:
        target = comment.split(":", 1)[1].strip()
    elif "нова пошта" in lower_comment and ":" in comment:
        target = comment.split(":", 1)[1].strip()
        city_name = normalize_text(order.city or "").strip()
        if city_name and target.lower().startswith(city_name.lower()):
            target = target[len(city_name):].lstrip(" •,.-")
    else:
        return ""
    target = re.sub(r"\s*(Замовлення|Збірка)\s*#\d+.*$", "", target, flags=re.IGNORECASE).strip()
    return target.rstrip(". ").strip()


def order_np_payload(order: Order | None) -> dict:
    if infer_order_delivery_type(order) != "nova_poshta":
        return empty_np_payload()
    service_type = order_np_service_type(order)
    warehouse_label = normalize_text(getattr(order, "np_warehouse_label", "") or "").strip()
    street_name = normalize_text(getattr(order, "np_street_name", "") or "").strip()
    house = normalize_text(getattr(order, "np_house", "") or "").strip()
    if not warehouse_label and service_type in {"warehouse", "postomat"}:
        warehouse_label = extract_order_np_target_from_comment(order)
    if not street_name and service_type == "address":
        street_name = extract_order_np_target_from_comment(order)
    return {
        "service_type": service_type,
        "city_name": normalize_text(getattr(order, "city", "") or "").strip(),
        "city_ref": normalize_text(getattr(order, "np_city_ref", "") or "").strip(),
        "warehouse_ref": normalize_text(getattr(order, "np_warehouse_ref", "") or "").strip(),
        "warehouse_label": warehouse_label,
        "street_ref": normalize_text(getattr(order, "np_street_ref", "") or "").strip(),
        "street_name": street_name,
        "house": house,
    }


def apply_np_payload_to_order(order: Order, delivery_type: str, np_payload: dict | None) -> None:
    delivery_kind = normalized_delivery_type(delivery_type)
    order.delivery_type = delivery_kind
    np_payload = np_payload or empty_np_payload()
    if delivery_kind != "nova_poshta":
        order.np_service_type = ""
        order.np_city_ref = ""
        order.np_warehouse_ref = ""
        order.np_warehouse_label = ""
        order.np_street_ref = ""
        order.np_street_name = ""
        order.np_house = ""
        return
    order.np_service_type = normalized_np_service_type(np_payload.get("service_type", "warehouse"))
    order.np_city_ref = normalize_text(np_payload.get("city_ref", "")).strip()
    order.np_warehouse_ref = normalize_text(np_payload.get("warehouse_ref", "")).strip()
    order.np_warehouse_label = normalize_text(np_payload.get("warehouse_label", "")).strip()
    order.np_street_ref = normalize_text(np_payload.get("street_ref", "")).strip()
    order.np_street_name = normalize_text(np_payload.get("street_name", "")).strip()
    order.np_house = normalize_text(np_payload.get("house", "")).strip()


def packing_np_payload_from_form(form) -> dict:
    delivery_type = normalized_delivery_type(form.get("delivery_type", "pickup"))
    if delivery_type != "nova_poshta":
        return empty_np_payload()
    return {
        "service_type": normalized_np_service_type(form.get("np_service_type", "warehouse")),
        "city_name": normalize_text(form.get("city_name", "")).strip(),
        "city_ref": normalize_text(form.get("city_ref", "")).strip(),
        "warehouse_ref": normalize_text(form.get("warehouse_ref", "")).strip(),
        "warehouse_label": normalize_text(form.get("warehouse_label", "")).strip(),
        "street_ref": normalize_text(form.get("street_ref", "")).strip(),
        "street_name": normalize_text(form.get("street_name", "")).strip(),
        "house": normalize_text(form.get("house", "")).strip(),
    }


def packing_request_np_payload(request_obj: PackingRequest | None) -> dict:
    if not request_obj or (request_obj.delivery_type or "").strip() != "nova_poshta":
        return empty_np_payload()
    return {
        "service_type": normalized_np_service_type(request_obj.np_service_type or "warehouse"),
        "city_name": normalize_text(request_obj.city or "").strip(),
        "city_ref": normalize_text(request_obj.np_city_ref or "").strip(),
        "warehouse_ref": normalize_text(request_obj.np_warehouse_ref or "").strip(),
        "warehouse_label": normalize_text(request_obj.np_warehouse_label or "").strip(),
        "street_ref": normalize_text(request_obj.np_street_ref or "").strip(),
        "street_name": normalize_text(request_obj.np_street_name or "").strip(),
        "house": normalize_text(request_obj.np_house or "").strip(),
    }


def packing_request_delivery_summary(request_obj: PackingRequest) -> str:
    if (request_obj.delivery_type or "").strip() != "nova_poshta":
        return ""
    service_type = normalized_np_service_type(request_obj.np_service_type or "warehouse")
    if service_type == "address":
        target = ", ".join(item for item in [request_obj.np_street_name or "", request_obj.np_house or ""] if item)
    else:
        target = normalize_text(request_obj.np_warehouse_label or "").strip()
    parts = [normalize_text(request_obj.city or "").strip(), target]
    return " • ".join(item for item in parts if item)


def packing_request_np_branch_label(request_obj: PackingRequest) -> str:
    if (request_obj.delivery_type or "").strip() != "nova_poshta":
        return ""
    return np_branch_label_from_values(request_obj.np_service_type or "warehouse", request_obj.np_warehouse_label or "")


def np_branch_label_from_values(service_type: str, warehouse_label: str) -> str:
    service_type = normalized_np_service_type(service_type or "warehouse")
    if service_type == "address":
        return ""
    raw_label = normalize_text(warehouse_label or "").strip()
    if not raw_label:
        return ""
    service_label = "Поштомат" if service_type == "postomat" else "Відділення"
    match = re.search(r"(?:відділення|поштомат)\s*№\s*([A-Za-zА-Яа-я0-9\-/]+)", raw_label, flags=re.IGNORECASE)
    if not match:
        match = re.search(r"№\s*([A-Za-zА-Яа-я0-9\-/]+)", raw_label, flags=re.IGNORECASE)
    if match:
        return f"{service_label} №{match.group(1)}"
    prefix = raw_label.split(":", 1)[0].strip()
    return prefix or raw_label


def packing_request_order_comment(request_obj: PackingRequest) -> str:
    base_comment = normalize_text(request_obj.comment or "").strip()
    if (request_obj.delivery_type or "").strip() != "nova_poshta":
        return base_comment
    service_label = {
        "warehouse": "відділення",
        "postomat": "поштомат",
        "address": "адреса",
    }.get(normalized_np_service_type(request_obj.np_service_type or "warehouse"), "відділення")
    summary = packing_request_delivery_summary(request_obj)
    prefix = f"Нова пошта ({service_label})"
    if summary:
        prefix = f"{prefix}: {summary}"
    if base_comment:
        return f"{prefix}. {base_comment}"
    return prefix


def create_availability_request_from_payload(db, warehouse_id: int, title: str, items_payload):
    request_obj = AvailabilityRequest(
        warehouse_id=warehouse_id,
        title=normalize_text(title or "").strip() or f"Запит {format_dt(now())}",
        status="open",
        progress_percent=0,
        total_items=0,
        checked_items=0,
        created_at=now(),
        updated_at=now(),
    )
    db.add(request_obj)
    db.flush()

    seen = set()
    for raw in items_payload or []:
        try:
            part_id = int(raw.get("partId") or 0)
        except Exception:
            continue
        if not part_id or part_id in seen:
            continue
        part = db.get(Part, part_id)
        if not part or part.warehouse_id != warehouse_id:
            continue
        seen.add(part_id)
        qty = int(raw.get("qty") or part.qty or 0)
        request_obj.items.append(
            AvailabilityRequestItem(
                request_id=request_obj.id,
                part_id=part.id,
                part_number=part.part_number or "",
                title=part.name or "",
                expected_qty=max(qty, 0),
                checked_qty=None,
                status="pending",
                note="",
                created_at=now(),
                updated_at=now(),
            )
        )
    recalc_availability_request(request_obj)
    return request_obj


def create_packing_request_from_payload(
    db,
    items_payload,
    delivery_type: str,
    source_type: str = "manual",
    source_order_id: int | None = None,
    customer_name: str = "",
    phone: str = "",
    city: str = "",
    comment: str = "",
    control_payment_uah: float | int | None = 0,
    np_payload: dict | None = None,
):
    delivery_kind = normalized_delivery_type(delivery_type)
    np_payload = np_payload or {}
    city_name = normalize_text(
        np_payload.get("city_name") if delivery_kind == "nova_poshta" else city or ""
    ).strip()
    request_obj = PackingRequest(
        source_type=source_type,
        source_order_id=source_order_id,
        delivery_type=delivery_kind,
        np_service_type=normalized_np_service_type(np_payload.get("service_type", "warehouse")),
        np_city_ref=normalize_text(np_payload.get("city_ref", "")).strip() if delivery_kind == "nova_poshta" else "",
        np_warehouse_ref=normalize_text(np_payload.get("warehouse_ref", "")).strip() if delivery_kind == "nova_poshta" else "",
        np_warehouse_label=normalize_text(np_payload.get("warehouse_label", "")).strip() if delivery_kind == "nova_poshta" else "",
        np_street_ref=normalize_text(np_payload.get("street_ref", "")).strip() if delivery_kind == "nova_poshta" else "",
        np_street_name=normalize_text(np_payload.get("street_name", "")).strip() if delivery_kind == "nova_poshta" else "",
        np_house=normalize_text(np_payload.get("house", "")).strip() if delivery_kind == "nova_poshta" else "",
        status="open",
        customer_name=normalize_text(customer_name or "").strip(),
        phone=normalize_text(phone or "").strip(),
        city=city_name,
        comment=normalize_text(comment or "").strip(),
        control_payment_uah=round(float(control_payment_uah or 0), 2) if delivery_kind == "nova_poshta" else 0,
        created_at=now(),
        updated_at=now(),
    )
    db.add(request_obj)
    db.flush()

    for raw in items_payload or []:
        try:
            part_id = int(raw.get("partId") or 0)
        except Exception:
            part_id = 0
        part_number = normalize_text(raw.get("partNumber") or "").strip().upper()
        qty = max(int(raw.get("qty") or 0), 0)
        if qty <= 0:
            continue
        part = db.get(Part, part_id) if part_id else None
        if not part and part_number:
            part = find_part_prefill(db, part_number)
        if not part:
            continue
        request_obj.items.append(
            PackingRequestItem(
                request_id=request_obj.id,
                part_id=part.id,
                part_number=part.part_number or "",
                title=part.name or "",
                expected_qty=qty,
                found_qty=0,
                missing_qty=qty,
                status="pending",
                photos_json="[]",
                created_at=now(),
                updated_at=now(),
            )
        )
    recalc_packing_request(request_obj)
    return request_obj


def update_packing_request_items(
    db,
    request_obj: PackingRequest,
    items_payload,
    delivery_type: str,
    customer_name: str | None = None,
    phone: str | None = None,
    comment: str | None = None,
    control_payment_uah: float | None = None,
    np_payload: dict | None = None,
):
    request_obj.delivery_type = normalized_delivery_type(delivery_type)
    request_obj.customer_name = normalize_text(
        request_obj.customer_name if customer_name is None else customer_name
    ).strip()
    request_obj.phone = normalize_text(request_obj.phone if phone is None else phone).strip()
    request_obj.comment = normalize_text(request_obj.comment if comment is None else comment).strip()
    np_payload = np_payload or {}
    if request_obj.delivery_type == "nova_poshta":
        effective_control_payment = request_obj.control_payment_uah if control_payment_uah is None else control_payment_uah
        request_obj.city = normalize_text(np_payload.get("city_name", "")).strip()
        request_obj.np_service_type = normalized_np_service_type(np_payload.get("service_type", "warehouse"))
        request_obj.np_city_ref = normalize_text(np_payload.get("city_ref", "")).strip()
        request_obj.np_warehouse_ref = normalize_text(np_payload.get("warehouse_ref", "")).strip()
        request_obj.np_warehouse_label = normalize_text(np_payload.get("warehouse_label", "")).strip()
        request_obj.np_street_ref = normalize_text(np_payload.get("street_ref", "")).strip()
        request_obj.np_street_name = normalize_text(np_payload.get("street_name", "")).strip()
        request_obj.np_house = normalize_text(np_payload.get("house", "")).strip()
        request_obj.control_payment_uah = round(float(effective_control_payment or 0), 2)
    else:
        request_obj.city = ""
        request_obj.np_service_type = "warehouse"
        request_obj.np_city_ref = ""
        request_obj.np_warehouse_ref = ""
        request_obj.np_warehouse_label = ""
        request_obj.np_street_ref = ""
        request_obj.np_street_name = ""
        request_obj.np_house = ""
        request_obj.control_payment_uah = 0
    for item in list(request_obj.items):
        db.delete(item)
    db.flush()
    for raw in items_payload or []:
        try:
            part_id = int(raw.get("partId") or 0)
        except Exception:
            continue
        qty = max(int(raw.get("qty") or 0), 0)
        if not part_id or qty <= 0:
            continue
        part = db.get(Part, part_id)
        if not part:
            continue
        request_obj.items.append(
            PackingRequestItem(
                request_id=request_obj.id,
                part_id=part.id,
                part_number=part.part_number or "",
                title=part.name or "",
                expected_qty=qty,
                found_qty=0,
                missing_qty=qty,
                status="pending",
                photos_json="[]",
                created_at=now(),
                updated_at=now(),
            )
        )
    request_obj.status = "open"
    request_obj.updated_at = now()
    recalc_packing_request(request_obj)


def ean8_svg(code: str) -> str:
    digits = re.sub(r"\D", "", code or "")
    if len(digits) != 8:
        return ""
    left_map = {
        "0": "0001101", "1": "0011001", "2": "0010011", "3": "0111101", "4": "0100011",
        "5": "0110001", "6": "0101111", "7": "0111011", "8": "0110111", "9": "0001011",
    }
    right_map = {
        "0": "1110010", "1": "1100110", "2": "1101100", "3": "1000010", "4": "1011100",
        "5": "1001110", "6": "1010000", "7": "1000100", "8": "1001000", "9": "1110100",
    }
    bits = "101"
    for digit in digits[:4]:
        bits += left_map[digit]
    bits += "01010"
    for digit in digits[4:]:
        bits += right_map[digit]
    bits += "101"

    module = 2
    width = len(bits) * module
    height = 64
    bars = []
    for idx, bit in enumerate(bits):
        if bit == "1":
            bars.append(f'<rect x="{idx * module}" y="0" width="{module}" height="{height}" fill="#111827"/>')
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" width="{width}" height="{height}" role="img" aria-label="{digits}">'
        f'{"".join(bars)}'
        "</svg>"
    )


def compact_print_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def qr_svg(value: str) -> str:
    payload = compact_print_text(value)
    if not payload:
        return ""
    image = qrcode.make(payload, image_factory=SvgPathImage, box_size=5, border=1)
    buffer = io.BytesIO()
    image.save(buffer)
    svg = buffer.getvalue().decode("utf-8")
    return re.sub(r"^\s*<\?xml[^>]+>\s*", "", svg, count=1)


def build_print_label(headline: str, title: str, description: str, barcode: str, context: str = "") -> dict:
    clean_headline = compact_print_text(headline)
    clean_title = compact_print_text(title)
    clean_description = compact_print_text(description)

    if not clean_title and clean_description:
        clean_title, clean_description = clean_description, ""

    if clean_title and clean_description:
        normalized_title = clean_title.casefold()
        normalized_description = clean_description.casefold()
        if (
            normalized_title == normalized_description
            or normalized_title in normalized_description
            or normalized_description in normalized_title
        ):
            clean_description = ""

    summary = clean_title or clean_description or clean_headline
    if summary.casefold() == clean_headline.casefold():
        summary = ""
    qr_value = compact_print_text(barcode) or clean_headline

    return {
        "headline": clean_headline,
        "summary": summary,
        "description": clean_description,
        "barcode": compact_print_text(barcode),
        "context": compact_print_text(context),
        "barcodeSvg": ean8_svg(barcode or ""),
        "qrSvg": qr_svg(qr_value),
    }


def label_copies(qty) -> int:
    try:
        return max(int(qty or 0), 0)
    except (TypeError, ValueError):
        return 0


def parse_label_qty_override(raw_value):
    value = (raw_value or "").strip()
    if not value:
        return None
    try:
        qty = int(value)
    except (TypeError, ValueError):
        return -1
    return qty if qty > 0 else -1


def find_part_prefill(db, part_number: str, warehouse_id: int | None = None):
    normalized = (part_number or "").strip().upper()
    if not normalized:
        return None
    parts = db.query(Part).filter(Part.part_number.ilike(normalized)).all()
    if not parts:
        return None
    parts.sort(
        key=lambda part: (
            0 if warehouse_id and part.warehouse_id == warehouse_id else 1,
            0 if not part.is_deleted else 1,
            0 if part.has_photo else 1,
            0 if part.has_description else 1,
            -(part.updated_at.timestamp() if part.updated_at else 0),
            part.id,
        )
    )
    return parts[0]


def find_part_template(db, part_number: str):
    normalized = (part_number or "").strip().upper()
    if not normalized:
        return None
    return (
        db.query(PartTemplate)
        .filter(PartTemplate.part_number == normalized)
        .order_by(desc(PartTemplate.updated_at), PartTemplate.id.asc())
        .first()
    )


def compact_part_code(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", normalize_text(value or "").upper())


def normalize_cross_number(value: str) -> str:
    text = normalize_text(value or "").strip().upper()
    text = re.sub(r"\s+", "", text)
    return text[:255]


def split_cross_number_values(values) -> list[str]:
    if values is None:
        return []
    if isinstance(values, str):
        values = [values]
    result = []
    for value in values:
        for item in re.split(r"[\n\r,;]+", normalize_text(value or "")):
            clean = normalize_cross_number(item)
            if clean:
                result.append(clean)
    return result


def template_cross_numbers(template: PartTemplate | None) -> list[str]:
    if not template:
        return []
    raw = template.cross_numbers_json or "[]"
    try:
        parsed = json.loads(raw)
    except Exception:
        parsed = []
    return normalize_cross_numbers(parsed, template.part_number or "")


def normalize_cross_numbers(values, main_part_number: str = "") -> list[str]:
    main = normalize_cross_number(main_part_number)
    main_compact = compact_part_code(main)
    unique: list[str] = []
    seen = set()
    for item in split_cross_number_values(values):
        item_compact = compact_part_code(item)
        if not item_compact:
            continue
        if item == main or (main_compact and item_compact == main_compact):
            continue
        if item_compact in seen:
            continue
        seen.add(item_compact)
        unique.append(item)
    return unique


def dump_cross_numbers(values, main_part_number: str = "") -> str:
    return json.dumps(normalize_cross_numbers(values, main_part_number), ensure_ascii=False)


def cross_numbers_from_form(form, field_name: str = "cross_numbers", main_part_number: str = "") -> list[str]:
    values = []
    try:
        values.extend(form.getlist(field_name))
    except Exception:
        pass
    single = form.get(field_name) if hasattr(form, "get") else None
    if single:
        values.append(single)
    return normalize_cross_numbers(values, main_part_number)


def find_part_template_by_cross(db, cross_number: str):
    target = normalize_cross_number(cross_number)
    target_compact = compact_part_code(target)
    if not target_compact:
        return None
    templates = db.query(PartTemplate).order_by(desc(PartTemplate.updated_at), PartTemplate.id.asc()).all()
    for template in templates:
        for item in template_cross_numbers(template):
            if item == target or compact_part_code(item) == target_compact:
                return template
    return None


def find_part_template_or_cross(db, part_number: str):
    template = find_part_template(db, part_number)
    if template:
        return template, False
    template = find_part_template_by_cross(db, part_number)
    return template, bool(template)


def remove_cross_number_from_template(template: PartTemplate | None, cross_number: str) -> bool:
    if not template:
        return False
    target_compact = compact_part_code(cross_number)
    if not target_compact:
        return False
    current = template_cross_numbers(template)
    updated = [item for item in current if compact_part_code(item) != target_compact]
    if len(updated) == len(current):
        return False
    template.cross_numbers_json = dump_cross_numbers(updated, template.part_number or "")
    template.updated_at = now()
    return True


def cross_numbers_map_for_parts(db, parts: list[Part]) -> dict[str, list[str]]:
    part_numbers = sorted({
        normalize_text(part.part_number or "").strip().upper()
        for part in parts or []
        if part and normalize_text(part.part_number or "").strip()
    })
    if not part_numbers:
        return {}
    templates = (
        db.query(PartTemplate)
        .filter(PartTemplate.part_number.in_(part_numbers))
        .all()
    )
    return {
        normalize_text(template.part_number or "").strip().upper(): template_cross_numbers(template)
        for template in templates
    }


def upsert_part_template(db, part_number: str, payload: dict | None = None):
    normalized = normalize_text(part_number or "").strip().upper()
    if not normalized:
        return None, False

    template = find_part_template(db, normalized)
    created = False
    if not template:
        template = PartTemplate(
            part_number=normalized,
            producer_type=producer_type_label("OEM"),
            created_at=now(),
            updated_at=now(),
        )
        db.add(template)
        created = True

    payload = payload or {}
    template.part_number = normalized
    if "brand" in payload:
        template.brand = normalize_text(payload.get("brand") or "").strip()
    if "producer_type" in payload:
        template.producer_type = producer_type_label(payload.get("producer_type") or "OEM")
    if "name" in payload:
        template.name = normalize_text(payload.get("name") or "").strip()
    if "description" in payload:
        template.description = normalize_text(payload.get("description") or "").strip()
    if "price_usd" in payload:
        try:
            template.price_usd = float(payload.get("price_usd") or 0)
        except Exception:
            template.price_usd = 0
    if "unassigned_qty" in payload:
        try:
            template.unassigned_qty = max(int(float(payload.get("unassigned_qty") or 0)), 0)
        except Exception:
            template.unassigned_qty = 0
    if "photo_urls" in payload:
        template.photo_urls = normalize_text(payload.get("photo_urls") or "").strip()
    if "showcase_photo_urls" in payload:
        template.showcase_photo_urls = dump_media_urls(payload.get("showcase_photo_urls"))
    elif "photo_urls" in payload and payload.get("photo_urls"):
        template.showcase_photo_urls = dump_media_urls([payload.get("photo_urls")])
    if "youtube_url" in payload:
        template.youtube_url = normalize_text(payload.get("youtube_url") or "").strip()
    if "cross_numbers" in payload:
        clean_cross_numbers = []
        for cross_number in normalize_cross_numbers(payload.get("cross_numbers"), normalized):
            exact_owner = find_part_template(db, cross_number)
            if exact_owner and exact_owner.id != template.id:
                continue
            cross_owner = find_part_template_by_cross(db, cross_number)
            if cross_owner and cross_owner.id != template.id:
                continue
            clean_cross_numbers.append(cross_number)
        template.cross_numbers_json = dump_cross_numbers(clean_cross_numbers, normalized)

    template.has_photo = bool(primary_template_photo(template))
    template.has_description = bool(template.description)
    template.updated_at = now()
    ensure_template_barcode(db, template)
    return template, created


def search_parts_for_picker(db, query_text: str, warehouse_id: int | None = None, limit: int = 12, include_deleted: bool = False):
    query_text = normalize_text(query_text or "").strip()
    if len(query_text) < 3:
        return []
    like = f"%{query_text}%"
    query = db.query(Part)
    if warehouse_id:
        query = query.filter(Part.warehouse_id == warehouse_id)
    if not include_deleted:
        query = query.filter(Part.is_deleted == False)
    parts = (
        query.filter(
            (Part.part_number.ilike(like))
            | (Part.name.ilike(like))
            | (Part.brand.ilike(like))
            | (Part.barcode.ilike(like))
        )
        .order_by(desc(Part.in_stock), desc(Part.updated_at), Part.part_number.asc())
        .limit(limit)
        .all()
    )
    cross_template = find_part_template_by_cross(db, query_text)
    if cross_template and len(parts) < limit:
        cross_query = db.query(Part).filter(Part.part_number == cross_template.part_number)
        if warehouse_id:
            cross_query = cross_query.filter(Part.warehouse_id == warehouse_id)
        if not include_deleted:
            cross_query = cross_query.filter(Part.is_deleted == False)
        existing_ids = {part.id for part in parts}
        for part in (
            cross_query.order_by(desc(Part.in_stock), desc(Part.updated_at), Part.part_number.asc())
            .limit(limit)
            .all()
        ):
            if part.id not in existing_ids:
                parts.append(part)
                existing_ids.add(part.id)
            if len(parts) >= limit:
                break
    for part in parts:
        ensure_part_barcode(db, part)
    return parts


def resolve_transit_source(db, part_number: str):
    part = find_part_prefill(db, part_number, None)
    template, matched_by_cross = find_part_template_or_cross(db, part_number)
    if matched_by_cross and template:
        part = find_part_prefill(db, template.part_number, None)
    if part:
        ensure_part_barcode(db, part)
    if template:
        ensure_template_barcode(db, template)
    return part, template


def save_upload(file_storage, prefix: str) -> str:
    if not file_storage or not file_storage.filename:
        return ""
    original = secure_filename(file_storage.filename) or "image.jpg"
    ext = os.path.splitext(original)[1] or ".jpg"
    stamp = f"{int(datetime.utcnow().timestamp())}_{hashlib.md5(original.encode()).hexdigest()[:10]}"
    filename = f"{prefix}_{stamp}{ext}"
    out = UPLOAD_DIR / filename
    file_storage.save(out)
    return f"/uploads/{filename}"


def save_uploads(files, prefix: str) -> list[str]:
    urls = []
    for index, file_storage in enumerate(files or [], start=1):
        url = save_upload(file_storage, f"{prefix}_{index}")
        if url:
            urls.append(url)
    return urls


def upload_path_from_url(url: str) -> Path | None:
    text = normalize_text(url or "").strip()
    if not text.startswith("/uploads/"):
        return None
    filename = Path(text).name
    if not filename:
        return None
    candidate = (UPLOAD_DIR / filename).resolve()
    try:
        candidate.relative_to(UPLOAD_DIR.resolve())
    except Exception:
        return None
    return candidate


def normalized_image_extension(filename: str = "", fallback: str = ".jpg") -> str:
    ext = (Path(filename or "").suffix or fallback).lower()
    return ext if ext in {".jpg", ".jpeg", ".png", ".webp"} else fallback


def export_photo_target(prefix: str, source_url: str = "", current_url: str = "") -> tuple[str, Path]:
    current_path = upload_path_from_url(current_url)
    if current_path and current_path.suffix.lower() == normalized_image_extension(current_path.name):
        return normalize_text(current_url or "").strip(), current_path

    source_path = upload_path_from_url(source_url)
    source_name = source_path.name if source_path else "image.jpg"
    ext = normalized_image_extension(source_name)
    stamp = f"{int(datetime.utcnow().timestamp())}_{hashlib.md5(f'{prefix}:{source_url}'.encode()).hexdigest()[:10]}"
    filename = f"{prefix}_export_{stamp}{ext}"
    target_path = UPLOAD_DIR / filename
    return f"/uploads/{filename}", target_path


def save_resized_export_photo(source_url: str, prefix: str, current_url: str = "") -> str:
    source_path = upload_path_from_url(source_url)
    if not source_path or not source_path.exists():
        return normalize_text(current_url or source_url or "").strip()

    target_url, target_path = export_photo_target(prefix, source_url, current_url)
    target_path.parent.mkdir(exist_ok=True)

    try:
        with Image.open(source_path) as image:
            image = ImageOps.exif_transpose(image)
            fitted = ImageOps.fit(
                image,
                EXPORT_PHOTO_SIZE,
                method=Image.Resampling.LANCZOS,
                centering=(0.5, 0.5),
            )
            ext = normalized_image_extension(target_path.name)
            if ext in {".jpg", ".jpeg"}:
                if "A" in fitted.getbands():
                    flattened = Image.new("RGB", EXPORT_PHOTO_SIZE, "white")
                    flattened.paste(fitted, mask=fitted.getchannel("A"))
                    fitted = flattened
                else:
                    fitted = fitted.convert("RGB")
                fitted.save(target_path, format="JPEG", quality=90, optimize=True)
            elif ext == ".png":
                if fitted.mode not in {"RGB", "RGBA"}:
                    fitted = fitted.convert("RGBA" if "A" in fitted.getbands() else "RGB")
                fitted.save(target_path, format="PNG", optimize=True)
            else:
                if fitted.mode not in {"RGB", "RGBA"}:
                    fitted = fitted.convert("RGBA" if "A" in fitted.getbands() else "RGB")
                fitted.save(target_path, format="WEBP", quality=90, method=6)
    except (UnidentifiedImageError, OSError, ValueError):
        return normalize_text(current_url or source_url or "").strip()

    return target_url


def parse_media_urls(value) -> list[str]:
    if not value:
        return []
    if isinstance(value, list):
        raw_items = value
    else:
        loaded = json_loads_safe(value, None)
        if isinstance(loaded, list):
            raw_items = loaded
        elif isinstance(loaded, str):
            raw_items = [loaded]
        else:
            raw_items = str(value).split(",")
    items = []
    seen = set()
    for item in raw_items:
        chunk = normalize_text(str(item or "").strip())
        if not chunk:
            continue
        candidates = [chunk]
        if "," in chunk:
            split_values = [normalize_text(part.strip()) for part in chunk.split(",") if normalize_text(part.strip())]
            if len(split_values) > 1:
                candidates = split_values
        for url in candidates:
            if not url or url in seen:
                continue
            seen.add(url)
            items.append(url)
    return items


def dump_media_urls(items) -> str:
    return json.dumps(parse_media_urls(items), ensure_ascii=False)


def reorder_media_with_primary(items, primary_url: str = "") -> list[str]:
    gallery = parse_media_urls(items)
    primary = normalize_text(primary_url or "").strip()
    if primary and primary in gallery:
        return [primary] + [url for url in gallery if url != primary]
    return gallery


def build_new_part_media(files, prefix: str, selected_upload_index: str = "", inherited_photo: str = ""):
    uploaded_urls = save_uploads(files, prefix)
    gallery = parse_media_urls(uploaded_urls)
    if not gallery and inherited_photo:
        gallery = parse_media_urls([inherited_photo])
    if len(gallery) == 1:
        selected_source = gallery[0]
    elif uploaded_urls:
        try:
            selected_source = uploaded_urls[int(selected_upload_index or 0)]
        except Exception:
            selected_source = uploaded_urls[0]
    else:
        selected_source = gallery[0] if gallery else ""
    gallery = reorder_media_with_primary(gallery, selected_source)
    export_url = save_resized_export_photo(selected_source, prefix) if selected_source else ""
    return export_url, dump_media_urls(gallery), bool(gallery or export_url)


def build_updated_part_media(
    part: Part,
    files,
    prefix: str,
    selected_upload_index: str = "",
    selected_existing_url: str = "",
    remove_urls=None,
):
    remove_set = {url for url in (remove_urls or []) if url}
    current_gallery = [url for url in part_gallery_urls(part) if url not in remove_set]
    uploaded_urls = save_uploads(files, prefix)
    gallery = parse_media_urls(current_gallery + uploaded_urls)

    if len(gallery) == 1:
        selected_source = gallery[0]
    elif selected_existing_url and selected_existing_url in gallery:
        selected_source = selected_existing_url
    elif uploaded_urls:
        try:
            selected_source = uploaded_urls[int(selected_upload_index or 0)]
        except Exception:
            selected_source = uploaded_urls[0]
    elif current_gallery:
        selected_source = current_gallery[0]
    else:
        selected_source = gallery[0] if gallery else ""
    gallery = reorder_media_with_primary(gallery, selected_source)
    export_url = save_resized_export_photo(selected_source, prefix, safe_photo(part.photo_urls)) if selected_source else ""
    return export_url, dump_media_urls(gallery), bool(gallery or export_url)


def youtube_embed_url(raw_url: str) -> str:
    text = normalize_text(raw_url or "").strip()
    if not text:
        return ""
    patterns = [
        r"youtu\.be/([A-Za-z0-9_-]{11})",
        r"youtube\.com/watch\?v=([A-Za-z0-9_-]{11})",
        r"youtube\.com/embed/([A-Za-z0-9_-]{11})",
        r"youtube\.com/shorts/([A-Za-z0-9_-]{11})",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return f"https://www.youtube.com/embed/{match.group(1)}"
    return ""


def primary_part_photo(part: Part) -> str:
    gallery = part_gallery_urls(part)
    return gallery[0] if gallery else ""


def part_gallery_urls(part: Part) -> list[str]:
    if not part:
        return []
    showcase = parse_media_urls(part.showcase_photo_urls)
    if showcase:
        return showcase
    export_photo = safe_photo(part.photo_urls)
    return [export_photo] if export_photo else []


def template_gallery_urls(template: PartTemplate) -> list[str]:
    if not template:
        return []
    showcase = parse_media_urls(template.showcase_photo_urls)
    if showcase:
        return showcase
    export_photo = safe_photo(template.photo_urls)
    return [export_photo] if export_photo else []


def primary_template_photo(template: PartTemplate) -> str:
    gallery = template_gallery_urls(template)
    return gallery[0] if gallery else ""


def primary_car_photo(car: Car) -> str:
    return safe_photo(car.image_urls)


def serialize_receiving_item(db, item: ReceivingDraftItem):
    warehouse = db.get(Warehouse, item.warehouse_id) if item.warehouse_id else None
    ensure_draft_barcode(db, item)
    photo_urls = parse_media_urls(item.photo_urls)
    return {
        "id": item.id,
        "warehouseId": item.warehouse_id,
        "warehouseName": warehouse.name if warehouse else "Без складу",
        "barcode": item.barcode or "",
        "partNumber": item.part_number,
        "title": item.title,
        "qty": int(item.qty or 0),
        "priceUsd": float(item.price_usd or 0),
        "description": item.description or "",
        "photoUrl": safe_photo(item.photo_urls),
        "photoUrls": photo_urls,
        "photoCount": len(photo_urls),
        "hasPhoto": bool(item.has_photo),
        "existingStocks": json_loads_safe(item.existing_stocks_json, []),
        "source": item.source or "mobile",
        "createdAt": item.created_at.isoformat() if item.created_at else "",
    }


def get_receiving_draft_items(db, warehouse_id: int | None = None):
    query = db.query(ReceivingDraftItem)
    if warehouse_id is not None:
        query = query.filter(ReceivingDraftItem.warehouse_id == warehouse_id)
    return query.order_by(ReceivingDraftItem.warehouse_id.asc(), desc(ReceivingDraftItem.updated_at), desc(ReceivingDraftItem.id)).all()


def parse_receiving_scope(raw_value) -> tuple[str, int | None]:
    value = normalize_text(raw_value or "").strip().lower()
    if value in {"", "0", "all"}:
        return "all", None
    if value in {"none", "unassigned", "without_warehouse"}:
        return "unassigned", None
    try:
        warehouse_id = int(value)
    except Exception:
        return "all", None
    return str(warehouse_id), warehouse_id


def get_receiving_draft_items_for_scope(db, scope_value: str):
    scope, warehouse_id = parse_receiving_scope(scope_value)
    if scope == "unassigned":
        return (
            db.query(ReceivingDraftItem)
            .filter(ReceivingDraftItem.warehouse_id == None)
            .order_by(desc(ReceivingDraftItem.updated_at), desc(ReceivingDraftItem.id))
            .all()
        )
    return get_receiving_draft_items(db, warehouse_id)


def find_recent_mobile_draft_duplicate(
    db,
    warehouse_id: int | None,
    part_number: str,
    title: str,
    qty: int,
    description: str,
):
    query = db.query(ReceivingDraftItem).filter(
        ReceivingDraftItem.source == "mobile",
        ReceivingDraftItem.part_number == part_number,
        ReceivingDraftItem.title == title,
        ReceivingDraftItem.qty == qty,
        ReceivingDraftItem.description == description,
        ReceivingDraftItem.created_at >= datetime.utcnow() - timedelta(seconds=15),
    )
    if warehouse_id is None:
        query = query.filter(ReceivingDraftItem.warehouse_id == None)
    else:
        query = query.filter(ReceivingDraftItem.warehouse_id == warehouse_id)
    return query.order_by(desc(ReceivingDraftItem.created_at), desc(ReceivingDraftItem.id)).first()


def upsert_template_from_receiving_item(db, item: ReceivingDraftItem, add_unassigned_qty: bool = False):
    part_number = (item.part_number or "").strip().upper()
    if not part_number:
        raise ValueError("part_number_required")

    existing_template = find_part_template(db, part_number)
    incoming_gallery = parse_media_urls(item.photo_urls)
    merged_gallery = parse_media_urls(template_gallery_urls(existing_template) + incoming_gallery)
    export_photo = incoming_gallery[0] if incoming_gallery else safe_photo(existing_template.photo_urls if existing_template else "")
    if not export_photo and merged_gallery:
        export_photo = merged_gallery[0]
    if not export_photo and incoming_gallery:
        export_photo = incoming_gallery[0]

    payload = {
        "brand": normalize_text(existing_template.brand if existing_template else "").strip(),
        "producer_type": producer_type_label(existing_template.producer_type if existing_template else "OEM"),
        "name": normalize_text(item.title or (existing_template.name if existing_template else "")).strip(),
        "description": normalize_text(
            item.description if (item.description or "").strip() else (existing_template.description if existing_template else "")
        ).strip(),
        "price_usd": float(item.price_usd or (existing_template.price_usd if existing_template else 0) or 0),
        "photo_urls": export_photo,
        "showcase_photo_urls": merged_gallery,
        "youtube_url": normalize_text(existing_template.youtube_url if existing_template else "").strip(),
    }
    if add_unassigned_qty:
        payload["unassigned_qty"] = template_unassigned_qty(existing_template) + int(item.qty or 0)

    template, created = upsert_part_template(db, part_number, payload)
    if item.barcode and not template.barcode:
        template.barcode = item.barcode
    ensure_template_barcode(db, template)
    apply_template_to_parts(db, template)
    return template, created


def import_receiving_items(db, items, warehouse_id: int | None):
    warehouse = db.get(Warehouse, warehouse_id) if warehouse_id else None
    if warehouse_id and not warehouse:
        raise ValueError("warehouse_not_found")

    imported = 0
    updated = 0
    template_created = 0
    template_updated = 0
    for item in items:
        part_number = (item.part_number or "").strip().upper()
        before_template = find_part_template(db, part_number)
        before_template_qty = template_unassigned_qty(before_template)
        template, created_template = upsert_template_from_receiving_item(db, item, add_unassigned_qty=warehouse is None)
        if created_template:
            template_created += 1
        else:
            template_updated += 1

        if warehouse is None:
            queue_template_inventory_change(
                db,
                template,
                before_template_qty,
                context_label="Прийом товару → Всі товари",
                reason=f"Додано {int(item.qty or 0)} шт.",
            )

        if warehouse is None:
            continue

        existing = (
            db.query(Part)
            .filter(Part.warehouse_id == warehouse_id, Part.part_number == part_number)
            .order_by(Part.id.asc())
            .first()
        )
        before_part_qty = 0 if not existing or existing.is_deleted else int(existing.qty or 0)
        if existing:
            if item.barcode and not existing.barcode:
                existing.barcode = item.barcode
            ensure_part_barcode(db, existing)
            if existing.is_deleted:
                existing.is_deleted = False
                existing.deleted_at = None
                existing.qty = int(item.qty or 0)
            else:
                existing.qty = int(existing.qty or 0) + int(item.qty or 0)
            existing.in_stock = existing.qty > 0
            apply_template_to_parts(db, template, only_parts=[existing])
            existing.producer_type = producer_type_label(template.producer_type or existing.producer_type or "OEM")
            existing.has_photo = bool(primary_part_photo(existing))
            existing.has_description = bool((existing.description or "").strip())
            existing.updated_at = now()
            updated += 1
        else:
            new_part = Part(
                warehouse_id=warehouse_id,
                part_number=part_number,
                barcode=(template.barcode or item.barcode or "").strip(),
                brand=normalize_text(template.brand or "").strip(),
                producer_type=producer_type_label(template.producer_type or "OEM"),
                name=normalize_text(template.name or item.title or "").strip(),
                description=normalize_text(template.description or item.description or "").strip(),
                price_usd=float(template.price_usd or item.price_usd or 0),
                qty=int(item.qty or 0),
                in_stock=int(item.qty or 0) > 0,
                photo_urls=template.photo_urls or "",
                showcase_photo_urls=template.showcase_photo_urls or dump_media_urls([template.photo_urls] if template.photo_urls else []),
                has_photo=bool(primary_template_photo(template)),
                has_description=bool((template.description or item.description or "").strip()),
                brand_export=normalize_text(template.brand or "").strip(),
                part_number_export=part_number,
                youtube_url=normalize_text(template.youtube_url or "").strip(),
                created_at=now(),
                updated_at=now(),
            )
            db.add(new_part)
            ensure_part_barcode(db, new_part)
            imported += 1
            queue_part_inventory_change(
                db,
                new_part,
                before_part_qty,
                context_label=f"Прийом товару → {warehouse.name}",
                reason=f"Додано {int(item.qty or 0)} шт.",
            )
            continue
        queue_part_inventory_change(
            db,
            existing,
            before_part_qty,
            context_label=f"Прийом товару → {warehouse.name}",
            reason=f"Додано {int(item.qty or 0)} шт.",
        )
    if warehouse is not None:
        db.flush()
        for part in db.query(Part).filter(Part.warehouse_id == warehouse_id, Part.is_deleted == False).all():
            template = find_part_template(db, part.part_number)
            if template:
                apply_template_to_parts(db, template, only_parts=[part])

    if items:
        flash_news(
            db,
            "receiving",
            "Прийом товару",
            (
                f"Базу оновлено: створено {template_created}, оновлено {template_updated}. "
                + (f"Склад {warehouse.name}: створено {imported}, оновлено {updated}." if warehouse else "Позиції збережено у Всі товари.")
            ),
            "success",
        )
    return imported, updated, warehouse, template_created, template_updated


def guess_internal_order_status(external_status: str, current_status: str = "") -> str:
    normalized = (external_status or "").strip().lower()
    if current_status in ("processing", "done", "cancelled"):
        return current_status
    done_words = ("delivered", "received", "completed", "done", "finished", "shipped")
    processing_words = ("processing", "accepted", "paid", "packing", "sending", "sent", "created")
    if any(word in normalized for word in done_words):
        return "done"
    if any(word in normalized for word in processing_words):
        return "processing"
    return "new"


def find_part_for_order_item(db, item: OrderItem):
    if item.part_id:
        part = db.get(Part, item.part_id)
        if part:
            return part
    part_number = (item.part_number or "").strip()
    if not part_number:
        return None
    return (
        db.query(Part)
        .filter(Part.part_number == part_number, Part.is_deleted == False)
        .order_by(desc(Part.updated_at), Part.id.asc())
        .first()
    )


def available_part_qty(part) -> int:
    if not part:
        return 0
    try:
        return max(int(part.qty or 0), 0)
    except Exception:
        return 0


def inventory_reserve_error_message(error_code: str) -> str:
    code = str(error_code or "").strip()
    if code.startswith("item_not_found:"):
        item_ref = code.split(":", 1)[1].strip() or "невідому позицію"
        return f"Позицію {item_ref} не знайдено у складі."
    if code.startswith("not_enough:"):
        _, part_number, available_qty = (code.split(":", 2) + ["", ""])[:3]
        part_number = part_number.strip() or "цієї позиції"
        try:
            available_qty_int = max(int(available_qty or 0), 0)
        except Exception:
            available_qty_int = 0
        return f"Для {part_number} доступно лише {available_qty_int} шт. у наявності."
    return "Не вдалося зарезервувати товар. Оновіть сторінку та перевірте залишок."


def reserve_order_inventory(db, order: Order) -> int:
    if order.stock_reserved:
        return 0
    planned_reservations = {}
    for item in order.items:
        part = find_part_for_order_item(db, item)
        item_ref = (item.part_number or item.name or str(item.id or "")).strip()
        if not part:
            raise ValueError(f"item_not_found:{item_ref}")
        if item.part_id != part.id:
            item.part_id = part.id
        qty = max(int(item.qty or 0), 0)
        if not qty:
            continue
        planned = planned_reservations.setdefault(
            part.id,
            {"part": part, "required_qty": 0},
        )
        planned["required_qty"] += qty
    for planned in planned_reservations.values():
        part = planned["part"]
        required_qty = int(planned["required_qty"] or 0)
        available_qty = available_part_qty(part)
        if required_qty > available_qty:
            raise ValueError(f"not_enough:{part.part_number or ''}:{available_qty}")
    reserved_units = 0
    for item in order.items:
        part = find_part_for_order_item(db, item)
        if not part:
            item_ref = (item.part_number or item.name or str(item.id or "")).strip()
            raise ValueError(f"item_not_found:{item_ref}")
        if item.part_id != part.id:
            item.part_id = part.id
        qty = max(int(item.qty or 0), 0)
        if not qty:
            continue
        before_qty = available_part_qty(part)
        part.qty = before_qty - qty
        part.in_stock = available_part_qty(part) > 0
        part.updated_at = now()
        queue_part_inventory_change(
            db,
            part,
            before_qty,
            context_label=f"Замовлення #{order.id}",
            reason=f"Списано {qty} шт. у резерв/видачу",
        )
        reserved_units += qty
    order.stock_reserved = True
    order.updated_at = now()
    return reserved_units


def release_order_inventory(db, order: Order) -> int:
    if not order.stock_reserved:
        return 0
    returned_units = 0
    for item in order.items:
        part = find_part_for_order_item(db, item)
        if not part:
            continue
        if item.part_id != part.id:
            item.part_id = part.id
        qty = int(item.qty or 0)
        if not qty:
            continue
        before_qty = available_part_qty(part)
        part.qty = before_qty + qty
        part.in_stock = available_part_qty(part) > 0
        part.updated_at = now()
        queue_part_inventory_change(
            db,
            part,
            before_qty,
            context_label=f"Замовлення #{order.id}",
            reason=f"Повернуто {qty} шт. у залишок",
        )
        returned_units += qty
    order.stock_reserved = False
    order.updated_at = now()
    return returned_units


def upsert_external_order(db, source: str, fields: dict, external_status: str):
    external_id = (fields.get("oid") or "").strip()
    if not external_id:
        return "skipped"

    order = (
        db.query(Order)
        .filter(Order.external_source == source, Order.external_order_id == external_id)
        .one_or_none()
    )
    created = order is None
    if order is None:
        order = Order(
            customer_name=fields.get("customer", "") or f"{source.upper()} buyer",
            phone=fields.get("phone", "") or "",
            city=fields.get("city", "") or "",
            comment=fields.get("comment", "") or "",
            total_usd=float(fields.get("total") or 0),
            status=guess_internal_order_status(external_status),
            is_processing=False,
            prepayment_usd=0,
            ttn="",
            ttn_status="",
            cancel_reason="",
            stock_reserved=False,
            external_source=source,
            external_order_id=external_id,
            external_status=external_status or "",
            created_at=now(),
            updated_at=now(),
        )
        db.add(order)
        db.flush()
    else:
        order.customer_name = fields.get("customer", order.customer_name) or order.customer_name
        order.phone = fields.get("phone", order.phone) or order.phone
        order.city = fields.get("city", order.city) or order.city
        order.comment = fields.get("comment", order.comment) or order.comment
        order.total_usd = float(fields.get("total") or order.total_usd or 0)
        order.external_status = external_status or order.external_status or ""
        order.status = guess_internal_order_status(order.external_status, order.status)
        order.updated_at = now()
        release_order_inventory(db, order)
        for existing_item in list(order.items):
            db.delete(existing_item)
        db.flush()

    order.external_source = source
    order.external_order_id = external_id
    order.external_status = external_status or ""
    order.is_processing = order.status == "processing"
    if order.status == "done":
        order.is_processing = False
    if order.status == "cancelled":
        order.is_processing = False

    for item in fields.get("items", []):
        matched_part = None
        part_number = (item.get("part_number") or "").strip()
        if part_number:
            matched_part = (
                db.query(Part)
                .filter(Part.part_number == part_number, Part.is_deleted == False)
                .order_by(Part.id.asc())
                .first()
            )
        order.items.append(
            OrderItem(
                part_id=matched_part.id if matched_part else None,
                part_number=part_number,
                name=item.get("name") or part_number or "Item",
                qty=int(item.get("qty") or 1),
                price_usd=float(item.get("price_usd") or 0),
            )
        )
    db.flush()
    order.stock_reserved = False
    return "created" if created else "updated"


def build_cart_state(db, cart: dict | None, *, lock_rows: bool = False):
    raw_cart = cart or {}
    requested_entries = []
    for raw_part_id, raw_qty in raw_cart.items():
        try:
            part_id = int(raw_part_id)
            qty = max(int(raw_qty), 0)
        except Exception:
            continue
        if part_id and qty > 0:
            requested_entries.append((part_id, qty))

    ids = [part_id for part_id, _qty in requested_entries]
    query = db.query(Part).filter(Part.id.in_(ids), Part.is_deleted == False) if ids else None
    if query is not None and lock_rows:
        query = query.with_for_update()
    parts = query.all() if query is not None else []
    parts_by_id = {part.id: part for part in parts}

    normalized_cart = {}
    issues = []
    items = []
    total = 0.0

    for part_id, requested_qty in requested_entries:
        part = parts_by_id.get(part_id)
        if not part:
            issues.append({"type": "removed", "partId": part_id})
            continue

        available_qty = available_part_qty(part)
        if available_qty <= 0:
            issues.append(
                {
                    "type": "out_of_stock",
                    "partId": part.id,
                    "partNumber": part.part_number or "",
                    "availableQty": 0,
                }
            )
            continue

        actual_qty = min(requested_qty, available_qty)
        if actual_qty < requested_qty:
            issues.append(
                {
                    "type": "limited",
                    "partId": part.id,
                    "partNumber": part.part_number or "",
                    "availableQty": available_qty,
                }
            )

        subtotal = float(part.price_usd or 0) * actual_qty
        total += subtotal
        normalized_cart[str(part.id)] = actual_qty
        items.append(
            {
                "part": part,
                "qty": actual_qty,
                "subtotal": subtotal,
                "available_qty": available_qty,
            }
        )

    return {
        "items": items,
        "total": total,
        "normalized_cart": normalized_cart,
        "issues": issues,
    }


def flash_cart_issues(issues: list[dict]):
    for issue in issues or []:
        issue_type = (issue.get("type") or "").strip()
        part_number = normalize_text(issue.get("partNumber") or "").strip() or "позиції"
        if issue_type == "limited":
            flash(f"Для {part_number} доступно лише {int(issue.get('availableQty') or 0)} шт. Кошик оновлено.", "error")
        elif issue_type == "out_of_stock":
            flash(f"Позиція {part_number} вже відсутня на складі та прибрана з кошика.", "error")
        elif issue_type == "removed":
            flash("Одна з позицій більше недоступна та прибрана з кошика.", "error")

def mobile_lookup_part_stock(db, part_number: str):
    normalized = (part_number or "").strip().upper()
    if not normalized:
        return {"partNumber": "", "exactExists": False, "stocks": []}
    parts = (
        db.query(Part)
        .filter(Part.part_number.ilike(normalized), Part.is_deleted == False)
        .all()
    )
    stocks = []
    for p in parts:
        ensure_part_barcode(db, p)
        warehouse = db.get(Warehouse, p.warehouse_id)
        stocks.append({
            "warehouseName": warehouse.name if warehouse else "вЂ”",
            "qty": int(p.qty or 0),
        })
    return {
        "partNumber": normalized,
        "exactExists": any(int(p.qty or 0) > 0 for p in parts),
        "stocks": stocks,
    }

def mobile_revision_payload(db, warehouse_id: int, ask_resume: bool = False):
    warehouse = db.get(Warehouse, warehouse_id)
    parts = (
        db.query(Part)
        .filter(Part.warehouse_id == warehouse_id, Part.is_deleted == False)
        .order_by(Part.id.asc())
        .all()
    )
    total = len(parts)
    current_index = int(warehouse.revision_current_index or 0) if warehouse else 0
    current_index = max(0, min(current_index, max(total - 1, 0)))
    current = parts[current_index] if parts and current_index < total else None
    if current:
        ensure_part_barcode(db, current)
    return {
        "warehouseId": warehouse_id,
        "warehouseName": warehouse.name if warehouse else "",
        "progressPercent": int(warehouse.revision_percent or 0) if warehouse else 0,
        "status": warehouse.revision_status if warehouse else "not_started",
        "askResume": ask_resume,
        "currentIndex": current_index,
        "total": total,
        "currentItem": {
            "partId": current.id,
            "barcode": current.barcode or "",
            "partNumber": current.part_number,
            "title": current.name,
            "currentQty": int(current.qty or 0),
            "photoUrl": safe_photo(current.photo_urls) if current else None,
        } if current else None,
    }


def np_request(db, model: str, called_method: str, method_properties: dict):
    settings = get_api_settings_map(db)
    api_key = settings.get("nova_poshta_api_key", "")
    payload = {
        "apiKey": api_key,
        "modelName": model,
        "calledMethod": called_method,
        "methodProperties": method_properties
    }
    resp = requests.post("https://api.novaposhta.ua/v2.0/json/", json=payload, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    return data.get("data", []), data


def refresh_order_ttn_status_from_np(db, order: Order) -> str:
    if not order or not (order.ttn or "").strip():
        return ""
    items, _raw = np_request(db, "TrackingDocument", "getStatusDocuments", {"Documents": [{"DocumentNumber": order.ttn}]})
    if not items:
        return ""
    status = normalize_text(items[0].get("Status", "")).strip()
    order.ttn_status = status
    if "отриман" in status.lower():
        if not order.stock_reserved:
            reserve_order_inventory(db, order)
        order.status = "done"
        order.is_processing = False
    elif order.ttn:
        order.status = "shipped"
        order.is_processing = True
    order.updated_at = now()
    return status


def autopro_token_info(db):
    settings = get_api_settings_map(db)
    return {
        "api_key": settings.get("autopro_api_key", ""),
        "environment": settings.get("autopro_environment", "production") or "production",
        "orders_url": settings.get("autopro_orders_url", ""),
        "base_url": settings.get("autopro_base_url", "https://avto.pro/api/v1"),
        "cached_jwt": settings.get("autopro_jwt_token", ""),
        "cached_at": settings.get("autopro_jwt_created_at", ""),
    }

def set_setting(db, key, value):
    row = db.query(ApiSetting).filter(ApiSetting.setting_key == key).one_or_none()
    if row:
        row.setting_value = value
        row.updated_at = now()
    else:
        db.add(ApiSetting(setting_key=key, setting_value=value, updated_at=now()))


def parse_backup_bool(value, default=False):
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on", "так", "y"}


def parse_backup_int(value, default, min_value=None, max_value=None):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    if min_value is not None:
        parsed = max(parsed, min_value)
    if max_value is not None:
        parsed = min(parsed, max_value)
    return parsed


def get_backup_settings(settings=None):
    settings = settings or {}
    remote = (settings.get("backup_rclone_remote") or os.getenv("BACKUP_RCLONE_REMOTE", "")).strip()
    auto_default = os.getenv("BACKUP_AUTO_ENABLED", "1")
    sync_default = "1" if remote else os.getenv("BACKUP_SYNC_ENABLED", "0")
    return {
        "rclone_remote": remote,
        "auto_enabled": parse_backup_bool(settings.get("backup_auto_enabled"), parse_backup_bool(auto_default, True)),
        "sync_enabled": parse_backup_bool(settings.get("backup_sync_enabled"), parse_backup_bool(sync_default, False)),
        "schedule_hour": parse_backup_int(
            settings.get("backup_schedule_hour") or os.getenv("BACKUP_SCHEDULE_HOUR", "3"),
            3,
            0,
            23,
        ),
        "retention_days": parse_backup_int(
            settings.get("backup_retention_days") or os.getenv("BACKUP_RETENTION_DAYS", "30"),
            30,
            1,
            3650,
        ),
    }


def backup_tool_status():
    return {
        "pg_dump": shutil.which("pg_dump") or "",
        "rclone": shutil.which("rclone") or "",
        "backup_dir": str(BACKUP_DIR),
    }


def serialize_backup_value(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


def serialize_backup_row(row):
    return {column.name: serialize_backup_value(getattr(row, column.name)) for column in row.__table__.columns}


def write_json_table_export(db, output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)
    table_counts = {}
    for mapper in sorted(Base.registry.mappers, key=lambda item: item.class_.__tablename__):
        model = mapper.class_
        table_name = model.__tablename__
        output_path = output_dir / f"{table_name}.json"
        count = 0
        with open(output_path, "w", encoding="utf-8") as fh:
            fh.write("[\n")
            first = True
            for row in db.query(model).yield_per(500):
                if not first:
                    fh.write(",\n")
                fh.write(json.dumps(serialize_backup_row(row), ensure_ascii=False, sort_keys=True))
                first = False
                count += 1
            fh.write("\n]\n")
        table_counts[table_name] = count
    return table_counts


def write_all_products_csv(db, output_path):
    active_parts = (
        db.query(Part)
        .filter(Part.is_deleted == False)
        .order_by(Part.part_number.asc(), Part.id.asc())
        .all()
    )
    parts_by_number = {}
    for part in active_parts:
        key = normalize_text(part.part_number).strip().upper()
        if not key:
            continue
        parts_by_number.setdefault(key, []).append(part)

    templates = db.query(PartTemplate).order_by(PartTemplate.part_number.asc()).all()
    template_numbers = {normalize_text(item.part_number).strip().upper() for item in templates}

    with open(output_path, "w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.writer(fh, delimiter=";")
        writer.writerow([
            "OEM номер",
            "Опис",
            "Виробник/бренд",
            "Тип",
            "Ціна USD",
            "Кількість без складу",
            "Кількість по складах",
            "Склади",
            "Штрихкод",
            "Фото імпорт/експорт",
            "Фото вітрини",
            "YouTube",
            "Оновлено",
        ])
        for template in templates:
            key = normalize_text(template.part_number).strip().upper()
            warehouse_parts = parts_by_number.get(key, [])
            stock_qty = sum(max(int(part.qty or 0), 0) for part in warehouse_parts)
            warehouse_names = ", ".join(
                sorted({part.warehouse.name for part in warehouse_parts if part.warehouse})
            )
            writer.writerow([
                template.part_number,
                normalize_text(template.name),
                normalize_text(template.brand),
                normalize_text(template.producer_type),
                template.price_usd,
                int(template.unassigned_qty or 0),
                stock_qty,
                warehouse_names,
                template.barcode,
                template.photo_urls,
                template.showcase_photo_urls,
                template.youtube_url,
                format_dt(template.updated_at),
            ])
        for key in sorted(set(parts_by_number) - template_numbers):
            warehouse_parts = parts_by_number[key]
            first = warehouse_parts[0]
            stock_qty = sum(max(int(part.qty or 0), 0) for part in warehouse_parts)
            warehouse_names = ", ".join(
                sorted({part.warehouse.name for part in warehouse_parts if part.warehouse})
            )
            writer.writerow([
                first.part_number,
                normalize_text(first.name),
                normalize_text(first.brand),
                normalize_text(first.producer_type),
                first.price_usd,
                0,
                stock_qty,
                warehouse_names,
                first.barcode,
                first.photo_urls,
                first.showcase_photo_urls,
                first.youtube_url,
                format_dt(first.updated_at),
            ])


def write_warehouse_inventory_csv(db, output_path):
    with open(output_path, "w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.writer(fh, delimiter=";")
        writer.writerow([
            "Склад",
            "OEM номер",
            "Опис",
            "Кількість",
            "Ціна USD",
            "Наявність",
            "Приховано",
            "Штрихкод",
            "Оновлено",
        ])
        rows = (
            db.query(Part)
            .join(Warehouse)
            .order_by(Warehouse.name.asc(), Part.part_number.asc(), Part.id.asc())
            .all()
        )
        for part in rows:
            writer.writerow([
                part.warehouse.name if part.warehouse else "",
                part.part_number,
                normalize_text(part.name),
                int(part.qty or 0),
                part.price_usd,
                "так" if part.in_stock else "ні",
                "так" if part.is_deleted else "ні",
                part.barcode,
                format_dt(part.updated_at),
            ])


def run_pg_dump(output_path):
    pg_dump = shutil.which("pg_dump")
    if not pg_dump:
        raise RuntimeError("pg_dump не знайдено в контейнері. Перезберіть Docker-образ після оновлення Dockerfile.")

    db_url = engine.url
    command = [
        pg_dump,
        "--format=custom",
        "--no-owner",
        "--no-privileges",
        "--file",
        str(output_path),
    ]
    if db_url.host:
        command.extend(["--host", db_url.host])
    if db_url.port:
        command.extend(["--port", str(db_url.port)])
    if db_url.username:
        command.extend(["--username", db_url.username])
    if not db_url.database:
        raise RuntimeError("У DATABASE_URL не вказана назва бази даних.")
    command.extend(["--dbname", db_url.database])

    env = os.environ.copy()
    if db_url.password:
        env["PGPASSWORD"] = db_url.password

    result = subprocess.run(command, capture_output=True, text=True, env=env, timeout=900)
    if result.returncode != 0:
        error_text = (result.stderr or result.stdout or "pg_dump завершився з помилкою").strip()
        raise RuntimeError(error_text[:1200])
    return output_path


def backup_path_is_relative_to(path, parent):
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def backup_source_skip(path):
    resolved = path.resolve()
    path_parts = {part.lower() for part in resolved.parts}
    if {"__pycache__", ".git", "backups", "rclone"} & path_parts:
        return True
    if backup_path_is_relative_to(resolved, UPLOAD_DIR):
        return True
    if backup_path_is_relative_to(resolved, BACKUP_DIR):
        return True
    if resolved.suffix.lower() in {".pyc", ".pyo", ".log", ".tmp"}:
        return True
    if resolved.parent == PROJECT_ROOT and resolved.name.startswith("public_home_") and resolved.suffix.lower() == ".png":
        return True
    return False


def add_directory_to_zip(zipf, source_dir, archive_prefix, skip_fn=None):
    source_dir = Path(source_dir)
    if not source_dir.exists():
        return 0
    added = 0
    for path in source_dir.rglob("*"):
        if not path.is_file():
            continue
        if skip_fn and skip_fn(path):
            continue
        rel_path = path.relative_to(source_dir).as_posix()
        zipf.write(path, f"{archive_prefix}/{rel_path}")
        added += 1
    return added


def rclone_target(remote, filename):
    remote = normalize_text(remote).strip()
    if remote.endswith(":"):
        return f"{remote}{filename}"
    return f"{remote.rstrip('/')}/{filename}"


def sync_backup_to_drive(backup_path, remote):
    remote = normalize_text(remote).strip()
    if not remote:
        return {"ok": False, "skipped": True, "message": "Google Drive remote не налаштований."}
    rclone = shutil.which("rclone")
    if not rclone:
        return {"ok": False, "skipped": True, "message": "rclone не знайдено в контейнері."}
    target = rclone_target(remote, Path(backup_path).name)
    result = subprocess.run(
        [rclone, "copyto", str(backup_path), target],
        capture_output=True,
        text=True,
        timeout=1800,
    )
    if result.returncode != 0:
        message = (result.stderr or result.stdout or "rclone завершився з помилкою").strip()
        return {"ok": False, "skipped": False, "message": message[:1200], "target": target}
    return {"ok": True, "skipped": False, "message": "Бекап синхронізовано з Google Drive.", "target": target}


def prune_old_backups(retention_days):
    cutoff = time.time() - (max(int(retention_days or 1), 1) * 86400)
    removed = []
    if not BACKUP_DIR.exists():
        return removed
    for path in BACKUP_DIR.glob("usa_auto_parts_full_*.zip"):
        try:
            if path.stat().st_mtime < cutoff:
                path.unlink()
                removed.append(path.name)
        except OSError:
            continue
    return removed


def list_backup_files(limit=25):
    if not BACKUP_DIR.exists():
        return []
    rows = []
    for path in sorted(BACKUP_DIR.glob("usa_auto_parts_full_*.zip"), key=lambda item: item.stat().st_mtime, reverse=True):
        stat = path.stat()
        rows.append({
            "name": path.name,
            "size_mb": round(stat.st_size / 1024 / 1024, 2),
            "created_at": datetime.fromtimestamp(stat.st_mtime),
        })
        if len(rows) >= limit:
            break
    return rows


def safe_backup_file(filename):
    clean_name = Path(filename or "").name
    if not clean_name.startswith("usa_auto_parts_full_") or not clean_name.endswith(".zip"):
        return None
    path = (BACKUP_DIR / clean_name).resolve()
    if not backup_path_is_relative_to(path, BACKUP_DIR) or not path.exists():
        return None
    return path


def create_site_backup(sync_to_drive=False, triggered_by="manual"):
    if not BACKUP_LOCK.acquire(blocking=False):
        raise RuntimeError("Бекап уже виконується. Дочекайтесь завершення поточного архівування.")
    try:
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = BACKUP_DIR / f"usa_auto_parts_full_{stamp}.zip"
        with TemporaryDirectory(prefix="usa_auto_parts_backup_") as tmp_dir:
            tmp_root = Path(tmp_dir)
            export_dir = tmp_root / "exports"
            json_dir = tmp_root / "data_json"
            database_dump = tmp_root / f"usa_auto_parts_{stamp}.dump"
            export_dir.mkdir(parents=True, exist_ok=True)

            db = SessionLocal()
            try:
                settings = get_api_settings_map(db)
                backup_settings = get_backup_settings(settings)
                write_all_products_csv(db, export_dir / "all_products.csv")
                write_warehouse_inventory_csv(db, export_dir / "warehouse_inventory.csv")
                table_counts = write_json_table_export(db, json_dir)
            finally:
                db.close()

            run_pg_dump(database_dump)
            manifest = {
                "created_at": datetime.now().isoformat(timespec="seconds"),
                "triggered_by": triggered_by,
                "database_url_host": engine.url.host or "",
                "database_name": engine.url.database or "",
                "table_counts": table_counts,
                "contains": [
                    "database/pg_dump custom format",
                    "uploads/",
                    "exports/all_products.csv",
                    "exports/warehouse_inventory.csv",
                    "data_json/*.json",
                    "source/",
                ],
                "restore_hint": "pg_restore database/*.dump у PostgreSQL, потім повернути uploads у app/uploads.",
            }
            restore_readme = (
                "USAparts.top backup\n\n"
                "1. Database dump: database/*.dump\n"
                "   Restore example:\n"
                "   pg_restore --clean --if-exists --no-owner --no-privileges -U usa -d usa_auto_parts database/usa_auto_parts_YYYYMMDD_HHMMSS.dump\n\n"
                "2. Uploaded photos/files: uploads/\n"
                "   Copy this folder back to app/uploads.\n\n"
                "3. Quick-readable exports are in exports/ and full JSON table copies are in data_json/.\n"
            )

            with zipfile.ZipFile(backup_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zipf:
                zipf.write(database_dump, f"database/{database_dump.name}")
                zipf.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2))
                zipf.writestr("README_RESTORE.txt", restore_readme)
                add_directory_to_zip(zipf, export_dir, "exports")
                add_directory_to_zip(zipf, json_dir, "data_json")
                add_directory_to_zip(zipf, UPLOAD_DIR, "uploads")
                add_directory_to_zip(zipf, PROJECT_ROOT, "source", skip_fn=backup_source_skip)

            removed = prune_old_backups(backup_settings["retention_days"])
            sync_result = {"ok": False, "skipped": True, "message": "Синхронізація не запускалась."}
            if sync_to_drive:
                sync_result = sync_backup_to_drive(backup_path, backup_settings["rclone_remote"])
            return {
                "ok": True,
                "path": str(backup_path),
                "filename": backup_path.name,
                "size_bytes": backup_path.stat().st_size,
                "size_mb": round(backup_path.stat().st_size / 1024 / 1024, 2),
                "removed_old": removed,
                "sync": sync_result,
            }
    finally:
        BACKUP_LOCK.release()


def record_backup_result(db, result=None, error=None, auto=False):
    if result:
        set_setting(db, "backup_last_status", "success")
        set_setting(db, "backup_last_message", f"{result['filename']} • {result['size_mb']} MB")
        set_setting(db, "backup_last_file", result["filename"])
        set_setting(db, "backup_last_at", datetime.now().isoformat(timespec="seconds"))
        sync = result.get("sync") or {}
        set_setting(db, "backup_last_sync_status", "success" if sync.get("ok") else ("skipped" if sync.get("skipped") else "error"))
        set_setting(db, "backup_last_sync_message", sync.get("message", ""))
        if auto:
            set_setting(db, "backup_last_auto_date", datetime.now().strftime("%Y-%m-%d"))
    elif error:
        set_setting(db, "backup_last_status", "error")
        set_setting(db, "backup_last_message", str(error)[:1200])
        set_setting(db, "backup_last_at", datetime.now().isoformat(timespec="seconds"))
        set_setting(db, "backup_last_sync_status", "")
        set_setting(db, "backup_last_sync_message", "")


def backup_scheduler_enabled():
    return parse_backup_bool(os.getenv("BACKUP_SCHEDULER_ENABLED", "1"), True)


def run_scheduled_backup_if_due():
    db = SessionLocal()
    try:
        settings = get_api_settings_map(db)
        backup_settings = get_backup_settings(settings)
        today = datetime.now().strftime("%Y-%m-%d")
        if not backup_settings["auto_enabled"]:
            return
        if settings.get("backup_last_auto_date") == today:
            return
        if datetime.now().hour < backup_settings["schedule_hour"]:
            return
    finally:
        db.close()

    try:
        result = create_site_backup(
            sync_to_drive=backup_settings["sync_enabled"],
            triggered_by="auto",
        )
        db = SessionLocal()
        try:
            record_backup_result(db, result=result, auto=True)
            flash_news(
                db,
                "backup",
                "Автоматичний бекап створено",
                f"{result['filename']} • {result['size_mb']} MB",
                "success",
            )
            db.commit()
        finally:
            db.close()
    except Exception as exc:
        db = SessionLocal()
        try:
            record_backup_result(db, error=exc, auto=False)
            flash_news(db, "backup", "Помилка автоматичного бекапу", str(exc)[:500], "error")
            db.commit()
        finally:
            db.close()


def backup_scheduler_loop():
    while True:
        try:
            run_scheduled_backup_if_due()
        except Exception:
            pass
        time.sleep(BACKUP_SCHEDULER_INTERVAL_SECONDS)


def start_backup_scheduler():
    global BACKUP_SCHEDULER_STARTED
    if BACKUP_SCHEDULER_STARTED or not backup_scheduler_enabled():
        return
    thread = threading.Thread(target=backup_scheduler_loop, name="backup-scheduler", daemon=True)
    thread.start()
    BACKUP_SCHEDULER_STARTED = True


def get_autopro_jwt(db, force_refresh=False):
    info = autopro_token_info(db)
    if not info["api_key"]:
        raise Exception("Autopro API key РЅРµ РІРєР°Р·Р°РЅРёР№")
    if info["cached_jwt"] and info["cached_at"] and not force_refresh:
        try:
            ts = datetime.fromisoformat(info["cached_at"])
            if (datetime.utcnow() - ts).total_seconds() < 23 * 3600:
                return info["cached_jwt"]
        except Exception:
            pass
    payload = {"apiKey": info["api_key"], "environment": info["environment"]}
    token_url = "https://avto.pro/api/v1/authentication/token"
    resp = requests.post(token_url, json=payload, timeout=25)
    if resp.status_code >= 400:
        raise Exception(f"Autopro auth error: {resp.status_code} {resp.text[:300]}")
    data = resp.json()
    jwt = data.get("jwtToken", "")
    if not jwt:
        raise Exception("Autopro РЅРµ РїРѕРІРµСЂРЅСѓРІ jwtToken")
    set_setting(db, "autopro_jwt_token", jwt)
    set_setting(db, "autopro_jwt_created_at", datetime.utcnow().isoformat())
    db.commit()
    return jwt

def autopro_request(db, method, url, body=None, force_refresh=False):
    jwt = get_autopro_jwt(db, force_refresh=force_refresh)
    headers = {"Authorization": f"Bearer {jwt}", "Content-Type": "application/json"}
    resp = requests.request(method, url, headers=headers, json=body, timeout=30)
    if resp.status_code == 401 and not force_refresh:
        jwt = get_autopro_jwt(db, force_refresh=True)
        headers["Authorization"] = f"Bearer {jwt}"
        resp = requests.request(method, url, headers=headers, json=body, timeout=30)
    if resp.status_code >= 400:
        raise Exception(f"Autopro request error: {resp.status_code} {resp.text[:500]}")
    if "application/json" in resp.headers.get("Content-Type", ""):
        return resp.json()
    return {"raw_text": resp.text}

def normalize_autopro_orders(payload):
    if isinstance(payload, dict):
        if isinstance(payload.get("items"), list):
            return payload["items"]
        if isinstance(payload.get("orders"), list):
            return payload["orders"]
        if isinstance(payload.get("data"), list):
            return payload["data"]
        return [payload]
    if isinstance(payload, list):
        return payload
    return []

def extract_autopro_order_fields(raw):
    oid = str(raw.get("id") or raw.get("orderId") or raw.get("number") or raw.get("uid") or "")
    customer = raw.get("customerName") or raw.get("buyerName") or raw.get("clientName") or "Autopro buyer"
    phone = raw.get("phone") or raw.get("buyerPhone") or ""
    city = raw.get("city") or raw.get("deliveryCity") or ""
    comment = raw.get("comment") or raw.get("deliveryComment") or ""
    total = float(raw.get("total") or raw.get("totalPrice") or raw.get("amount") or 0)
    item_candidates = raw.get("items") or raw.get("products") or raw.get("positions") or []
    items = []
    for it in item_candidates:
        part_number = str(it.get("partNumber") or it.get("article") or it.get("sku") or it.get("number") or "")
        name = it.get("name") or it.get("title") or part_number or "Autopro item"
        qty = int(it.get("qty") or it.get("quantity") or 1)
        price = float(it.get("price") or it.get("unitPrice") or 0)
        items.append({"part_number": part_number, "name": name, "qty": qty, "price_usd": price})
    return {"oid": oid, "customer": customer, "phone": phone, "city": city, "comment": comment, "total": total, "items": items}


def normalize_prom_orders(payload):
    if isinstance(payload, dict):
        for key in ("orders", "items", "data", "results"):
            if isinstance(payload.get(key), list):
                return payload[key]
        return [payload]
    if isinstance(payload, list):
        return payload
    return []


def extract_prom_order_fields(raw):
    delivery = raw.get("delivery_address") if isinstance(raw.get("delivery_address"), dict) else {}
    customer = " ".join(
        x
        for x in [
            raw.get("customer_name"),
            raw.get("full_name"),
            raw.get("buyer_name"),
            raw.get("client_first_name"),
            raw.get("client_last_name"),
        ]
        if x
    ).strip()
    phone = raw.get("phone") or raw.get("client_phone") or raw.get("phone_number") or ""
    city = raw.get("city") or raw.get("delivery_city") or delivery.get("city") or delivery.get("city_name") or ""
    comment = " ".join(
        x for x in [
            raw.get("comment"),
            raw.get("client_notes"),
            raw.get("customer_notes"),
            raw.get("delivery_option"),
        ] if x
    ).strip()
    total = float(raw.get("full_price") or raw.get("total_price") or raw.get("amount") or raw.get("total") or 0)
    items = []
    for it in raw.get("products") or raw.get("items") or raw.get("positions") or []:
        part_number = str(it.get("sku") or it.get("article") or it.get("external_id") or it.get("name") or "")
        name = it.get("name") or it.get("title") or part_number or "Prom item"
        qty = int(it.get("quantity") or it.get("qty") or 1)
        price = float(it.get("price") or it.get("price_with_discount") or it.get("unit_price") or 0)
        items.append({"part_number": part_number, "name": name, "qty": qty, "price_usd": price})
    return {
        "oid": str(raw.get("id") or raw.get("order_id") or raw.get("number") or ""),
        "customer": customer or "Prom buyer",
        "phone": phone,
        "city": city,
        "comment": comment,
        "total": total,
        "items": items,
    }


def build_mobile_lookup_payload(db, part_number: str):
    normalized = (part_number or "").strip().upper()
    if not normalized:
        return {
            "partNumber": "",
            "exactExists": False,
            "stocks": [],
            "matches": [],
            "suggestedTitle": "",
            "photoUrl": "",
        }

    template, matched_by_cross = find_part_template_or_cross(db, normalized)
    lookup_number = normalize_text(template.part_number or "").strip().upper() if template else normalized
    parts = (
        db.query(Part)
        .filter(Part.part_number.ilike(lookup_number), Part.is_deleted == False)
        .all()
    )
    stocks = []
    matches = []
    for p in parts:
        ensure_part_barcode(db, p)
        warehouse = db.get(Warehouse, p.warehouse_id)
        stocks.append({
            "warehouseName": warehouse.name if warehouse else "вЂ”",
            "qty": int(p.qty or 0),
        })
        matches.append({
            "partId": p.id,
            "warehouseId": p.warehouse_id,
            "warehouseName": warehouse.name if warehouse else "",
            "barcode": p.barcode or "",
            "title": p.name or "",
            "qty": int(p.qty or 0),
            "photoUrl": primary_part_photo(p),
        })

    primary = next((item for item in matches if item["title"] or item["photoUrl"]), matches[0] if matches else None)
    if not primary and template:
        ensure_template_barcode(db, template)
    return {
        "partNumber": lookup_number,
        "requestedPartNumber": normalized,
        "matchedByCross": matched_by_cross,
        "exactExists": any(int(p.qty or 0) > 0 for p in parts),
        "stocks": stocks,
        "matches": matches,
        "suggestedTitle": primary["title"] if primary else (template.name or "") if template else "",
        "photoUrl": primary["photoUrl"] if primary else primary_template_photo(template) if template else "",
    }


def display_usd(price_usd: float, markup_percent: float) -> str:
    return f"{float(price_usd) * (1 + float(markup_percent) / 100.0):.2f}"


def display_uah(price_usd: float, markup_percent: float) -> str:
    usd = float(price_usd) * (1 + float(markup_percent) / 100.0)
    return f"{usd * DEFAULT_UAH_RATE:.2f}"


def safe_photo(urls: str) -> str:
    items = parse_media_urls(urls)
    return items[0] if items else ""


def stats_visitor_id() -> str:
    visitor_id = normalize_text(session.get("stats_visitor_id") or "").strip()
    if not visitor_id:
        visitor_id = uuid4().hex
        session["stats_visitor_id"] = visitor_id
    return visitor_id


def track_stats_event(
    db,
    event_type: str,
    part: Part | None = None,
    query_text: str = "",
    quantity: int = 1,
    order_id: int | None = None,
    meta: dict | None = None,
):
    clean_event = normalize_text(event_type or "").strip()
    if not clean_event:
        return None
    meta_json = "{}"
    if meta:
        try:
            meta_json = json.dumps(meta, ensure_ascii=False, sort_keys=True)
        except Exception:
            meta_json = "{}"
    event = StatsEvent(
        event_type=clean_event,
        visitor_id=stats_visitor_id(),
        part_id=part.id if part else None,
        part_number=normalize_text(part.part_number if part else "").strip()[:255],
        part_name=normalize_text(part.name if part else "").strip()[:500],
        query_text=normalize_text(query_text or "").strip()[:500],
        quantity=max(int(quantity or 0), 0),
        order_id=order_id,
        meta_json=meta_json,
        created_at=now(),
    )
    db.add(event)
    return event


def should_track_guest_visit() -> bool:
    endpoint = request.endpoint or ""
    return request.method == "GET" and endpoint in {
        "home",
        "catalog",
        "part_detail",
        "cars_public",
        "car_detail_public",
        "cart_view",
    }


def track_daily_guest_visit():
    if not should_track_guest_visit():
        return
    today_key = datetime.utcnow().strftime("%Y-%m-%d")
    tracked_days = session.get("stats_guest_days") or []
    if not isinstance(tracked_days, list):
        tracked_days = []
    if today_key in tracked_days:
        return
    db = SessionLocal()
    try:
        track_stats_event(db, "guest_visit", meta={"path": request.path})
        db.commit()
        tracked_days.append(today_key)
        session["stats_guest_days"] = tracked_days[-45:]
    except Exception:
        db.rollback()
    finally:
        db.close()


def parse_stats_date(value: str, fallback):
    try:
        return datetime.strptime((value or "").strip(), "%Y-%m-%d").date()
    except Exception:
        return fallback


def stats_date_range_from_request():
    today = datetime.utcnow().date()
    mode = normalize_text(request.args.get("mode") or "day").strip()
    if mode == "range":
        start_date = parse_stats_date(request.args.get("date_from", ""), today)
        end_date = parse_stats_date(request.args.get("date_to", ""), start_date)
    else:
        mode = "day"
        start_date = parse_stats_date(request.args.get("date", ""), today)
        end_date = start_date
    if end_date < start_date:
        start_date, end_date = end_date, start_date
    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date + timedelta(days=1), datetime.min.time())
    return mode, start_date, end_date, start_dt, end_dt


def aggregate_part_stats(events):
    grouped = {}
    for event in events:
        key = normalize_text(event.part_number or "").strip() or f"ID {event.part_id or event.id}"
        row = grouped.setdefault(
            key,
            {
                "part_number": key,
                "part_name": normalize_text(event.part_name or "").strip(),
                "count": 0,
                "quantity": 0,
                "last_at": event.created_at,
            },
        )
        if not row["part_name"] and event.part_name:
            row["part_name"] = normalize_text(event.part_name).strip()
        row["count"] += 1
        row["quantity"] += max(int(event.quantity or 0), 0)
        if event.created_at and (not row["last_at"] or event.created_at > row["last_at"]):
            row["last_at"] = event.created_at
    return sorted(grouped.values(), key=lambda row: (row["quantity"], row["count"], row["last_at"]), reverse=True)


def aggregate_search_stats(events):
    grouped = {}
    for event in events:
        query = normalize_text(event.query_text or "").strip()
        if not query:
            continue
        row = grouped.setdefault(query.casefold(), {"query": query, "count": 0, "last_at": event.created_at})
        row["count"] += 1
        if event.created_at and (not row["last_at"] or event.created_at > row["last_at"]):
            row["last_at"] = event.created_at
    return sorted(grouped.values(), key=lambda row: (row["count"], row["last_at"]), reverse=True)


def vehicle_names_from_warehouses(warehouses) -> list[str]:
    result = []
    seen = set()
    for warehouse in warehouses:
        name = seo_vehicle_label_from_warehouse(warehouse)
        if not name:
            continue
        key = name.upper().casefold()
        if key not in seen:
            seen.add(key)
            result.append(name)
    return result


def public_site_base_url() -> str:
    configured = (os.getenv("PUBLIC_SITE_URL") or os.getenv("SITE_BASE_URL") or "").strip()
    if configured:
        return configured.rstrip("/")
    forwarded_proto = (request.headers.get("X-Forwarded-Proto") or "").split(",")[0].strip()
    forwarded_host = (request.headers.get("X-Forwarded-Host") or "").split(",")[0].strip()
    scheme = forwarded_proto or request.scheme or "https"
    host = forwarded_host or request.host
    host_name = host.split(":", 1)[0].lower()
    if host_name in {"usaparts.top", "www.usaparts.top"}:
        scheme = "https"
        host = "usaparts.top"
    return f"{scheme}://{host}".rstrip("/")


def absolute_public_url(path_or_url: str) -> str:
    value = normalize_text(path_or_url or "").strip()
    if not value:
        return ""
    if value.startswith(("http://", "https://")):
        return value
    if value.startswith("//"):
        return f"{request.scheme}:{value}"
    return f"{public_site_base_url()}/{value.lstrip('/')}"


def public_url_for(endpoint: str, **values) -> str:
    return absolute_public_url(url_for(endpoint, **values))


def compact_meta_text(*parts, limit: int = 160) -> str:
    text = normalize_text(" ".join(str(part or "") for part in parts)).strip()
    text = re.sub(r"\s+", " ", text)
    if len(text) <= limit:
        return text
    return text[: max(limit - 1, 0)].rstrip(" ,.;:-") + "…"


CYRILLIC_SLUG_MAP = str.maketrans({
    "а": "a", "б": "b", "в": "v", "г": "g", "ґ": "g", "д": "d", "е": "e", "є": "ye",
    "ё": "yo", "ж": "zh", "з": "z", "и": "i", "і": "i", "ї": "yi", "й": "y", "к": "k",
    "л": "l", "м": "m", "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t",
    "у": "u", "ф": "f", "х": "kh", "ц": "ts", "ч": "ch", "ш": "sh", "щ": "shch",
    "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya",
})


def transliterate_slug_text(value: str) -> str:
    text = normalize_text(value or "").strip().lower()
    return text.translate(CYRILLIC_SLUG_MAP)


def part_seo_slug_from_values(part_number: str = "", name: str = "") -> str:
    text = transliterate_slug_text(f"{part_number or ''} {name or ''}")
    text = re.sub(r"[^a-z0-9]+", "-", text, flags=re.IGNORECASE)
    text = re.sub(r"-{2,}", "-", text).strip("-")
    return text[:120].strip("-") or "part"


def part_seo_slug(part: Part) -> str:
    return part_seo_slug_from_values(part.part_number, part.name)


def part_detail_url(part: Part, **values) -> str:
    return url_for("part_detail", part_id=part.id, slug=part_seo_slug(part), **values)


def public_part_url(part: Part) -> str:
    return absolute_public_url(part_detail_url(part))


def cross_part_detail_url(part: Part, cross_number: str, **values) -> str:
    cross_slug = part_seo_slug_from_values(cross_number, part.name)
    return url_for(
        "cross_part_detail",
        cross_number=normalize_cross_number(cross_number),
        part_id=part.id,
        slug=cross_slug,
        **values,
    )


def public_cross_part_url(part: Part, cross_number: str) -> str:
    return absolute_public_url(cross_part_detail_url(part, cross_number))


def seo_json_dumps(payload) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def organization_schema_payload() -> dict:
    base_url = public_site_base_url()
    return {
        "@type": "Organization",
        "@id": f"{base_url}/#organization",
        "name": "USAparts.top",
        "url": base_url,
        "logo": public_url_for("static", filename="usaparts-logo-transparent.png"),
    }


def website_schema_payload() -> dict:
    base_url = public_site_base_url()
    return {
        "@type": "WebSite",
        "@id": f"{base_url}/#website",
        "url": base_url,
        "name": "USAparts.top",
        "alternateName": ["USA Parts Top", "USAparts", "USAparts.top"],
        "publisher": {"@id": f"{base_url}/#organization"},
        "inLanguage": "uk-UA",
    }


def merchant_return_policy_payload() -> dict:
    country = (os.getenv("SEO_RETURN_COUNTRY") or "UA").strip().upper()[:2] or "UA"
    policy = (os.getenv("SEO_RETURN_POLICY") or "not_permitted").strip().casefold()
    if policy in {"finite", "window", "return_window", "allowed"}:
        days = max(int(os.getenv("SEO_RETURN_DAYS") or 14), 1)
        return {
            "@type": "MerchantReturnPolicy",
            "applicableCountry": country,
            "returnPolicyCategory": "https://schema.org/MerchantReturnFiniteReturnWindow",
            "merchantReturnDays": days,
            "returnMethod": "https://schema.org/ReturnByMail",
            "returnFees": "https://schema.org/ReturnFeesCustomerResponsibility",
        }
    return {
        "@type": "MerchantReturnPolicy",
        "applicableCountry": country,
        "returnPolicyCategory": "https://schema.org/MerchantReturnNotPermitted",
    }


def offer_shipping_details_payload() -> dict:
    country = (os.getenv("SEO_SHIPPING_COUNTRY") or "UA").strip().upper()[:2] or "UA"
    currency = (os.getenv("SEO_SHIPPING_CURRENCY") or "USD").strip().upper()[:3] or "USD"
    max_shipping = os.getenv("SEO_SHIPPING_MAX_VALUE", "10.00").strip() or "10.00"
    handling_min = max(int(os.getenv("SEO_HANDLING_MIN_DAYS") or 0), 0)
    handling_max = max(int(os.getenv("SEO_HANDLING_MAX_DAYS") or 1), handling_min)
    transit_min = max(int(os.getenv("SEO_TRANSIT_MIN_DAYS") or 1), 0)
    transit_max = max(int(os.getenv("SEO_TRANSIT_MAX_DAYS") or 3), transit_min)
    return {
        "@type": "OfferShippingDetails",
        "shippingDestination": {
            "@type": "DefinedRegion",
            "addressCountry": country,
        },
        "shippingRate": {
            "@type": "MonetaryAmount",
            "maxValue": max_shipping,
            "currency": currency,
        },
        "deliveryTime": {
            "@type": "ShippingDeliveryTime",
            "handlingTime": {
                "@type": "QuantitativeValue",
                "minValue": handling_min,
                "maxValue": handling_max,
                "unitCode": "DAY",
            },
            "transitTime": {
                "@type": "QuantitativeValue",
                "minValue": transit_min,
                "maxValue": transit_max,
                "unitCode": "DAY",
            },
        },
    }


def seo_price_valid_until() -> str:
    days = max(int(os.getenv("SEO_PRICE_VALID_DAYS") or 30), 1)
    return (datetime.utcnow() + timedelta(days=days)).date().isoformat()


def webpage_schema_payload(title: str, description: str, url: str) -> dict:
    return {
        "@type": "WebPage",
        "@id": f"{url}#webpage",
        "url": url,
        "name": title,
        "description": description,
        "isPartOf": {"@id": f"{public_site_base_url()}/#website"},
        "inLanguage": "uk-UA",
    }


def breadcrumb_schema_payload(items: list[tuple[str, str]]) -> dict:
    item_list = []
    for index, (name, url) in enumerate(items, 1):
        if not name or not url:
            continue
        item_list.append(
            {
                "@type": "ListItem",
                "position": index,
                "name": name,
                "item": url,
            }
        )
    return {
        "@type": "BreadcrumbList",
        "itemListElement": item_list,
    }


def graph_json_ld(*payloads) -> str:
    graph = [organization_schema_payload(), website_schema_payload()]
    for payload in payloads:
        if not payload:
            continue
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                continue
        if isinstance(payload, dict) and payload.get("@graph"):
            graph.extend(payload.get("@graph") or [])
        elif isinstance(payload, list):
            graph.extend(payload)
        elif isinstance(payload, dict):
            graph.append(payload)
    return seo_json_dumps({"@context": "https://schema.org", "@graph": graph})


def build_home_schema(title: str, description: str, featured: list[Part]) -> str:
    url = public_url_for("home")
    # Product codes are indexed on their own canonical product pages. Keeping them
    # out of the homepage schema prevents Google from showing the homepage for OEM queries.
    return graph_json_ld(
        webpage_schema_payload(title, description, url),
        {
            "@type": "AutoPartsStore",
            "@id": f"{url}#store",
            "name": "USAparts.top",
            "url": url,
            "description": description,
            "image": public_url_for("static", filename="usaparts-logo-transparent.png"),
            "areaServed": "UA",
        }
    )


def build_part_product_schema(part: Part, warehouse: Warehouse | None, display_part_number: str | None = None, canonical_url: str | None = None) -> str:
    display_number = normalize_text(display_part_number or part.part_number or "").strip()
    part_url = canonical_url or public_part_url(part)
    gallery = [absolute_public_url(url) for url in part_gallery_urls(part)]
    price = display_usd(part.price_usd, warehouse.markup_percent if warehouse else 0)
    categories = seo_part_categories(part)
    brand_name = seo_clean_label(part.brand or part.brand_export or producer_type_label(part.producer_type))
    product_payload = {
        "@type": "Product",
        "@id": f"{part_url}#product",
        "name": compact_meta_text(display_number, part.name, limit=120),
        "url": part_url,
        "sku": display_number,
        "mpn": display_number,
        "brand": {"@type": "Brand", "name": brand_name or "USAparts.top"},
        "description": compact_meta_text(part.description or part.name, display_number, limit=300),
        "mainEntityOfPage": {"@id": f"{part_url}#webpage"},
        "offers": {
            "@type": "Offer",
            "url": part_url,
            "priceCurrency": "USD",
            "price": price,
            "priceValidUntil": seo_price_valid_until(),
            "availability": "https://schema.org/InStock" if part.in_stock and int(part.qty or 0) > 0 else "https://schema.org/OutOfStock",
            "itemCondition": "https://schema.org/NewCondition" if producer_type_label(part.producer_type) == "OEM" else "https://schema.org/UsedCondition",
            "inventoryLevel": {
                "@type": "QuantitativeValue",
                "value": max(int(part.qty or 0), 0),
            },
            "seller": {"@id": f"{public_site_base_url()}/#organization"},
            "shippingDetails": offer_shipping_details_payload(),
            "hasMerchantReturnPolicy": merchant_return_policy_payload(),
        },
        "additionalProperty": [
            {"@type": "PropertyValue", "name": "OEM номер", "value": display_number},
            {"@type": "PropertyValue", "name": "Кількість", "value": str(int(part.qty or 0))},
        ],
    }
    if display_number != normalize_text(part.part_number or "").strip():
        product_payload["additionalProperty"].append(
            {"@type": "PropertyValue", "name": "Основний OEM", "value": normalize_text(part.part_number or "").strip()}
        )
    if gallery:
        product_payload["image"] = gallery
    if categories:
        product_payload["category"] = ", ".join(category["label"] for category in categories[:3])
    if warehouse:
        product_payload["additionalProperty"].append(
            {"@type": "PropertyValue", "name": "Склад", "value": seo_clean_label(warehouse.name)}
        )
    title = compact_meta_text(display_number, part.name, limit=90)
    description = compact_meta_text("Купити запчастину", display_number, part.name, f"наявність {int(part.qty or 0)} шт.", limit=160)
    return graph_json_ld(
        webpage_schema_payload(title, description, part_url),
        breadcrumb_schema_payload(
            [
                ("Головна", public_url_for("home")),
                ("Каталог запчастин", public_url_for("catalog")),
                (display_number, part_url),
            ]
        ),
        product_payload,
    )


def build_car_product_schema(car: Car, photos: list[str]) -> str:
    car_url = public_url_for("car_detail_public", car_id=car.id)
    title = compact_meta_text(car.brand, car.model, car.year, limit=120)
    description = compact_meta_text(car.description, car.vin, limit=300) or title
    payload = {
        "@type": "Product",
        "@id": f"{car_url}#vehicle",
        "name": title,
        "url": car_url,
        "sku": normalize_text(car.vin or f"car-{car.id}").strip(),
        "category": "Vehicle",
        "image": [absolute_public_url(url) for url in photos],
        "description": description,
        "offers": {
            "@type": "Offer",
            "url": car_url,
            "priceCurrency": "USD",
            "price": f"{float(car.price_usd or 0):.2f}",
            "priceValidUntil": seo_price_valid_until(),
            "availability": "https://schema.org/InStock" if car.status == "in_stock" else "https://schema.org/PreOrder",
            "itemCondition": "https://schema.org/UsedCondition",
            "seller": {"@id": f"{public_site_base_url()}/#organization"},
            "shippingDetails": offer_shipping_details_payload(),
            "hasMerchantReturnPolicy": merchant_return_policy_payload(),
        },
        "additionalProperty": [
            {"@type": "PropertyValue", "name": "VIN", "value": normalize_text(car.vin or "").strip()},
            {"@type": "PropertyValue", "name": "Year", "value": str(car.year or "")},
            {"@type": "PropertyValue", "name": "Mileage", "value": str(car.mileage or "")},
        ],
    }
    return graph_json_ld(
        webpage_schema_payload(title, description, car_url),
        breadcrumb_schema_payload(
            [
                ("Головна", public_url_for("home")),
                ("Авто", public_url_for("cars_public")),
                (title, car_url),
            ]
        ),
        payload,
    )


def sitemap_lastmod(value) -> str:
    if not value:
        return ""
    if isinstance(value, datetime):
        return value.date().isoformat()
    return str(value)[:10]


def sitemap_url_node(location: str, lastmod: str = "", changefreq: str = "", priority: str = "") -> str:
    lines = ["  <url>", f"    <loc>{xml_escape(location)}</loc>"]
    if lastmod:
        lines.append(f"    <lastmod>{xml_escape(lastmod)}</lastmod>")
    if changefreq:
        lines.append(f"    <changefreq>{xml_escape(changefreq)}</changefreq>")
    if priority:
        lines.append(f"    <priority>{xml_escape(priority)}</priority>")
    lines.append("  </url>")
    return "\n".join(lines)


def sitemap_image_url_node(location: str, images: list[dict], lastmod: str = "") -> str:
    lines = ["  <url>", f"    <loc>{xml_escape(location)}</loc>"]
    if lastmod:
        lines.append(f"    <lastmod>{xml_escape(lastmod)}</lastmod>")
    for image in images[:10]:
        image_url = image.get("loc") or ""
        if not image_url:
            continue
        lines.append("    <image:image>")
        lines.append(f"      <image:loc>{xml_escape(image_url)}</image:loc>")
        if image.get("title"):
            lines.append(f"      <image:title>{xml_escape(image['title'])}</image:title>")
        if image.get("caption"):
            lines.append(f"      <image:caption>{xml_escape(image['caption'])}</image:caption>")
        lines.append("    </image:image>")
    lines.append("  </url>")
    return "\n".join(lines)


SEO_VEHICLE_KEYWORDS = {
    "ACURA", "AUDI", "BMW", "CADILLAC", "CHEVROLET", "CHRYSLER", "DODGE", "FIAT",
    "FORD", "GMC", "HONDA", "HYUNDAI", "INFINITI", "JEEP", "KIA", "LEXUS",
    "LINCOLN", "MAZDA", "MERCEDES", "MERCEDES-BENZ", "MITSUBISHI", "NISSAN",
    "PORSCHE", "RENAULT", "SATURN", "SCION", "SSANGYONG", "SUBARU", "SUZUKI",
    "TOYOTA", "VOLKSWAGEN", "VW", "VAG", "BUICK", "TESLA", "VOLVO", "RAM",
    "TIGUAN", "PASSAT", "JETTA", "GOLF", "TOUAREG", "ATLAS", "TAHOE", "CAMRY",
    "RAV4", "VENZA", "ROGUE", "ESCAPE", "FUSION", "MALIBU", "CHEROKEE",
    "COMPASS", "DURANGO", "PACIFICA", "X5", "X3", "Q5", "Q7",
}

SEO_VEHICLE_LABELS = {
    "VW": "Volkswagen",
    "VAG": "Volkswagen / VAG",
    "MERCEDES-BENZ": "Mercedes-Benz",
    "MERCEDES": "Mercedes-Benz",
}

SEO_SERVICE_WAREHOUSE_WORDS = {
    "СКЛАД", "ГАРАНТОВАН", "НАЯВ", "TEST", "MARKET", "OEM", "ЗАМІННИК",
    "ЗАМЕННИК", "ПРИЙОМ", "ПРИЕМ", "ТОВАР", "TRANSIT", "ALL", "ВСІ", "ВСЕ",
}

SEO_SERVICE_WAREHOUSE_WORD_STEMS = (
    "СКЛАД", "ГАРАНТОВАН", "НАЯВ", "TEST", "MARKET", "OEM", "ЗАМІННИК",
    "ЗАМЕННИК", "ПРИЙОМ", "ПРИЕМ", "ТОВАР", "TRANSIT", "ALL", "ВСІ", "ВСЕ",
)

SEO_IGNORED_BRANDS = {
    "", "OEM", "MARKET", "MARKET OEM", "MARKET (OEM)", "ОРИГІНАЛ", "ОРИГИНАЛ",
    "ЗАМІННИК", "ЗАМЕННИК", "АНАЛОГ", "НОВИЙ", "БУ", "B/U", "USED", "NEW",
}

SEO_CATEGORY_RULES = (
    {"slug": "bampery-kryla-kuzov", "label": "Кузовні запчастини", "keywords": ("бампер", "крило", "капот", "двер", "крыш", "криш", "кузов", "наклад", "обшив", "панел", "решет", "дзерк", "зерк", "molding", "trim")},
    {"slug": "optika-fary-likhtari", "label": "Оптика, фари та ліхтарі", "keywords": ("фара", "ліхтар", "фонар", "оптик", "ламп", "противотуман", "стоп", "reflector", "headlight", "tail light")},
    {"slug": "pidviska-ryulove", "label": "Підвіска та рульове", "keywords": ("важіль", "рычаг", "амортиз", "стойк", "сайлент", "шарова", "ступиц", "маточ", "тяга", "руль", "рейк", "підвіск", "подвес")},
    {"slug": "halmivna-systema", "label": "Гальмівна система", "keywords": ("гальм", "тормоз", "колод", "диск", "супорт", "суппорт", "abs", "brake")},
    {"slug": "dvygun-ta-opory", "label": "Двигун та опори", "keywords": ("двигун", "двигател", "мотор", "опор", "подушка", "клапан", "порш", "колін", "колен", "engine")},
    {"slug": "karter-piddon-mastylna", "label": "Картер, піддон і мастильна система", "keywords": ("піддон", "поддон", "картер", "масл", "олив", "oil pan", "фільтр олив", "фильтр масл")},
    {"slug": "okholodzhennya", "label": "Система охолодження", "keywords": ("радіатор", "радиатор", "вентилятор", "термостат", "патруб", "насос", "помпа", "охолод", "coolant")},
    {"slug": "elektryka-datchyky", "label": "Електрика та датчики", "keywords": ("датчик", "сенсор", "провод", "блок", "модул", "реле", "кнопк", "перемика", "плата", "електр", "электр", "module", "sensor")},
    {"slug": "salon-interyer", "label": "Салон та інтер'єр", "keywords": ("салон", "сидін", "сиден", "панель прибор", "щиток", "консоль", "ремін", "ремень", "interior", "airbag")},
    {"slug": "transmisiya-akpp", "label": "Трансмісія та АКПП", "keywords": ("акпп", "кпп", "коробк", "трансм", "редуктор", "шрус", "привід", "привод", "transmission")},
    {"slug": "palivna-systema", "label": "Паливна система", "keywords": ("палив", "топлив", "бак", "форсунк", "насос пал", "fuel")},
    {"slug": "sklo-dah-ushchilnyuvachi", "label": "Скло, дах та ущільнювачі", "keywords": ("скло", "стекл", "люк", "дах", "крыша", "ущільн", "уплотн", "glass", "seal")},
)


def seo_slug(value: str) -> str:
    text = transliterate_slug_text(value or "")
    text = re.sub(r"[^a-z0-9]+", "-", text, flags=re.IGNORECASE)
    text = re.sub(r"-{2,}", "-", text).strip("-")
    return text[:90].strip("-") or "item"


def seo_clean_label(value: str) -> str:
    return re.sub(r"\s+", " ", normalize_text(value or "").strip())


def seo_normalized_tokens(value: str) -> list[str]:
    normalized = re.sub(r"[^A-ZА-ЯІЇЄҐ0-9]+", " ", (value or "").upper()).strip()
    return [token for token in normalized.split() if token]


def seo_is_service_warehouse_name(value: str) -> bool:
    tokens = seo_normalized_tokens(value)
    if not tokens:
        return True
    service_hits = 0
    for token in tokens:
        if token in SEO_SERVICE_WAREHOUSE_WORDS or any(token.startswith(stem) or stem in token for stem in SEO_SERVICE_WAREHOUSE_WORD_STEMS):
            service_hits += 1
    return service_hits == len(tokens)


def public_active_parts(db):
    return (
        db.query(Part)
        .filter(Part.is_deleted == False, Part.in_stock == True, Part.qty > 0)
        .order_by(desc(Part.updated_at), desc(Part.id))
        .all()
    )


def best_unique_public_parts(parts) -> list[Part]:
    best_by_number: dict[str, Part] = {}
    for part in parts:
        key = seo_clean_label(part.part_number).upper() or f"id-{part.id}"
        current = best_by_number.get(key)
        candidate_score = (
            1 if primary_part_photo(part) else 0,
            max(int(part.qty or 0), 0),
            part.updated_at.timestamp() if part.updated_at else 0,
            part.id,
        )
        current_score = (
            1 if current and primary_part_photo(current) else 0,
            max(int(current.qty or 0), 0) if current else 0,
            current.updated_at.timestamp() if current and current.updated_at else 0,
            current.id if current else 0,
        )
        if not current or candidate_score > current_score:
            best_by_number[key] = part
    return sorted(best_by_number.values(), key=lambda part: (seo_clean_label(part.part_number).upper(), part.id))


def seo_part_text(part: Part) -> str:
    warehouse_name = part.warehouse.name if getattr(part, "warehouse", None) else ""
    return " ".join(
        [
            seo_clean_label(part.part_number),
            seo_clean_label(part.brand),
            seo_clean_label(part.brand_export),
            seo_clean_label(part.producer_type),
            seo_clean_label(part.name),
            seo_clean_label(part.description),
            seo_clean_label(warehouse_name),
        ]
    ).casefold()


def seo_part_brand(part: Part) -> str:
    for value in (part.brand, part.brand_export):
        label = seo_clean_label(value)
        normalized = re.sub(r"[^A-ZА-ЯІЇЄҐ0-9]+", " ", label.upper()).strip()
        if normalized not in SEO_IGNORED_BRANDS and len(label) >= 2:
            return label[:60]
    return ""


def seo_vehicle_label_from_warehouse(warehouse: Warehouse | None) -> str:
    if not warehouse:
        return ""
    name = seo_clean_label(warehouse.name)
    if not name:
        return ""
    upper = name.upper()
    if seo_is_service_warehouse_name(name):
        return ""
    if not any(keyword in upper for keyword in SEO_VEHICLE_KEYWORDS):
        return ""
    return name[:80]


def seo_vehicle_label_from_part(part: Part) -> str:
    warehouse_label = seo_vehicle_label_from_warehouse(part.warehouse if getattr(part, "warehouse", None) else None)
    return warehouse_label


def seo_warehouse_catalog_entries(db) -> list[dict]:
    grouped: dict[str, dict] = {}
    warehouses = db.query(Warehouse).order_by(Warehouse.name.asc()).all()
    for warehouse in warehouses:
        label = seo_vehicle_label_from_warehouse(warehouse)
        if not label:
            continue
        slug = seo_slug(label)
        parts = (
            db.query(Part)
            .filter(
                Part.warehouse_id == warehouse.id,
                Part.is_deleted == False,
                Part.in_stock == True,
                Part.qty > 0,
            )
            .order_by(desc(Part.updated_at), desc(Part.id))
            .all()
        )
        if not parts:
            continue
        latest = max((part.updated_at or part.created_at for part in parts if part.updated_at or part.created_at), default=warehouse.updated_at)
        row = grouped.setdefault(
            slug,
            {
                "slug": slug,
                "label": label,
                "count": 0,
                "lastmod": latest,
                "warehouses": [],
            },
        )
        row["count"] += len(parts)
        row["warehouses"].append(warehouse.name)
        if latest and (not row["lastmod"] or latest > row["lastmod"]):
            row["lastmod"] = latest
    return sorted(grouped.values(), key=lambda row: (-row["count"], row["label"].casefold()))


def seo_part_categories(part: Part) -> list[dict]:
    text = seo_part_text(part)
    categories = []
    for rule in SEO_CATEGORY_RULES:
        if any(keyword.casefold() in text for keyword in rule["keywords"]):
            categories.append(rule)
    return categories


def seo_collect_entries(parts: list[Part]):
    brands: dict[str, dict] = {}
    categories: dict[str, dict] = {}
    vehicles: dict[str, dict] = {}
    vehicle_categories: dict[tuple[str, str], dict] = {}
    for part in parts:
        updated_at = part.updated_at or part.created_at
        brand_label = seo_part_brand(part)
        if brand_label:
            slug = seo_slug(brand_label)
            row = brands.setdefault(slug, {"slug": slug, "label": brand_label, "count": 0, "lastmod": updated_at})
            row["count"] += 1
            if updated_at and (not row["lastmod"] or updated_at > row["lastmod"]):
                row["lastmod"] = updated_at

        vehicle_label = seo_vehicle_label_from_part(part)
        vehicle_slug = seo_slug(vehicle_label) if vehicle_label else ""
        if vehicle_label:
            row = vehicles.setdefault(vehicle_slug, {"slug": vehicle_slug, "label": vehicle_label, "count": 0, "lastmod": updated_at})
            row["count"] += 1
            if updated_at and (not row["lastmod"] or updated_at > row["lastmod"]):
                row["lastmod"] = updated_at

        for category in seo_part_categories(part):
            row = categories.setdefault(
                category["slug"],
                {"slug": category["slug"], "label": category["label"], "count": 0, "lastmod": updated_at},
            )
            row["count"] += 1
            if updated_at and (not row["lastmod"] or updated_at > row["lastmod"]):
                row["lastmod"] = updated_at

            if vehicle_slug:
                combo_key = (category["slug"], vehicle_slug)
                combo = vehicle_categories.setdefault(
                    combo_key,
                    {
                        "category_slug": category["slug"],
                        "category_label": category["label"],
                        "vehicle_slug": vehicle_slug,
                        "vehicle_label": vehicle_label,
                        "count": 0,
                        "lastmod": updated_at,
                    },
                )
                combo["count"] += 1
                if updated_at and (not combo["lastmod"] or updated_at > combo["lastmod"]):
                    combo["lastmod"] = updated_at

    return {
        "brands": sorted(brands.values(), key=lambda row: (-row["count"], row["label"].casefold())),
        "categories": sorted(categories.values(), key=lambda row: (-row["count"], row["label"].casefold())),
        "vehicles": sorted(vehicles.values(), key=lambda row: (-row["count"], row["label"].casefold())),
        "vehicle_categories": sorted(
            [row for row in vehicle_categories.values() if row["count"] >= 2],
            key=lambda row: (-row["count"], row["vehicle_label"].casefold(), row["category_label"].casefold()),
        ),
    }


def seo_filter_parts(parts: list[Part], *, brand_slug: str = "", category_slug: str = "", vehicle_slug: str = "") -> list[Part]:
    filtered = []
    for part in parts:
        if brand_slug and seo_slug(seo_part_brand(part)) != brand_slug:
            continue
        if vehicle_slug and seo_slug(seo_vehicle_label_from_part(part)) != vehicle_slug:
            continue
        if category_slug and category_slug not in {category["slug"] for category in seo_part_categories(part)}:
            continue
        filtered.append(part)
    return filtered


def seo_listing_schema(title: str, description: str, url: str, parts: list[Part]) -> str:
    items = []
    for index, part in enumerate(parts[:24], 1):
        items.append(
            {
                "@type": "ListItem",
                "position": index,
                "url": public_part_url(part),
                "name": compact_meta_text(part.part_number, part.name, limit=120),
            }
        )
    payload = {
        "@type": "CollectionPage",
        "@id": f"{url}#collection",
        "name": title,
        "description": description,
        "url": url,
        "isPartOf": {"@id": f"{public_site_base_url()}/#website"},
        "inLanguage": "uk-UA",
        "mainEntity": {"@type": "ItemList", "itemListElement": items},
    }
    return graph_json_ld(
        breadcrumb_schema_payload(
            [
                ("Головна", public_url_for("home")),
                ("Каталог запчастин", public_url_for("catalog")),
                (title, url),
            ]
        ),
        payload,
    )


def render_seo_listing(parts: list[Part], title: str, description: str, canonical_url: str, *, intro: str = "", related=None):
    related = related or {}
    return render_template(
        "seo_listing.html",
        title=title,
        description=description,
        intro=intro or description,
        parts=parts[:72],
        total_count=len(parts),
        related=related,
        display_usd=display_usd,
        display_uah=display_uah,
        seo_title=f"{title} | USAparts.top",
        seo_description=compact_meta_text(description, limit=155),
        canonical_url=canonical_url,
        json_ld=seo_listing_schema(title, description, canonical_url, parts),
    )


def sitemap_xml_response(nodes: list[str]) -> Response:
    body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
    body += "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n"
    body += "\n".join(nodes)
    body += "\n</urlset>\n"
    response = Response(body, mimetype="application/xml")
    response.headers["Cache-Control"] = "public, max-age=3600"
    return response


def sitemap_image_xml_response(nodes: list[str]) -> Response:
    body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
    body += '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:image="http://www.google.com/schemas/sitemap-image/1.1">\n'
    body += "\n".join(nodes)
    body += "\n</urlset>\n"
    response = Response(body, mimetype="application/xml")
    response.headers["Cache-Control"] = "public, max-age=3600"
    return response


def sitemap_index_response(locations: list[tuple[str, str]]) -> Response:
    lines = ["<?xml version=\"1.0\" encoding=\"UTF-8\"?>", "<sitemapindex xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">"]
    for location, lastmod in locations:
        lines.append("  <sitemap>")
        lines.append(f"    <loc>{xml_escape(location)}</loc>")
        if lastmod:
            lines.append(f"    <lastmod>{xml_escape(lastmod)}</lastmod>")
        lines.append("  </sitemap>")
    lines.append("</sitemapindex>")
    response = Response("\n".join(lines) + "\n", mimetype="application/xml")
    response.headers["Cache-Control"] = "public, max-age=3600"
    return response


def recalc_warehouse_stats(warehouse: Warehouse):
    parts = [part for part in warehouse.parts if not part.is_deleted]
    total = len(parts)
    with_photo = sum(1 for p in parts if p.has_photo)
    with_desc = sum(1 for p in parts if p.has_description)
    processed = min(int(warehouse.revision_current_index or 0), total) if total else 0

    if total == 0:
        warehouse.revision_status = "not_started"
        warehouse.revision_percent = 0
    else:
        warehouse.revision_percent = int((processed / total) * 100)
        if processed >= total and total > 0:
            warehouse.revision_status = "completed"
            if not warehouse.revision_date:
                warehouse.revision_date = now()
        elif processed > 0:
            warehouse.revision_status = "in_progress"
        else:
            warehouse.revision_status = "not_started"

    warehouse.updated_at = now()
    return {
        "total": total,
        "with_photo_pct": int((with_photo / total) * 100) if total else 0,
        "with_desc_pct": int((with_desc / total) * 100) if total else 0,
    }


def parse_avtopro_csv(file_storage):
    def to_float(value):
        try:
            return float(str(value or "0").replace(",", ".").strip() or 0)
        except Exception:
            return 0.0

    def to_int(value):
        try:
            return int(float(str(value or "0").replace(",", ".").strip() or 0))
        except Exception:
            return 0

    def bool_cell(value, default=True):
        text = normalize_text(value or "").strip().casefold()
        if text in {"0", "false", "ні", "no", "нема", "немає"}:
            return False
        if text in {"1", "true", "так", "yes", "є"}:
            return True
        return default

    def xlsx_cell_text(cell, shared_strings):
        value_node = cell.find("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}v")
        raw_value = value_node.text if value_node is not None else ""
        cell_type = cell.attrib.get("t", "")
        if cell_type == "s":
            try:
                return shared_strings[int(raw_value)]
            except Exception:
                return raw_value or ""
        if cell_type == "inlineStr":
            texts = cell.findall(".//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t")
            return "".join(item.text or "" for item in texts)
        return raw_value or ""

    def read_xlsx_rows(raw_bytes):
        rows = []
        with zipfile.ZipFile(io.BytesIO(raw_bytes)) as archive:
            shared_strings = []
            if "xl/sharedStrings.xml" in archive.namelist():
                shared_root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
                for item in shared_root.findall("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}si"):
                    texts = item.findall(".//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t")
                    shared_strings.append("".join(text.text or "" for text in texts))
            workbook_root = ET.fromstring(archive.read("xl/workbook.xml"))
            rel_root = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
            rels = {
                rel.attrib.get("Id"): rel.attrib.get("Target", "")
                for rel in rel_root.findall("{http://schemas.openxmlformats.org/package/2006/relationships}Relationship")
            }
            first_sheet = workbook_root.find(".//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}sheet")
            if first_sheet is None:
                return rows
            rid = first_sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
            target = rels.get(rid, "worksheets/sheet1.xml")
            sheet_path = f"xl/{target.lstrip('/')}"
            sheet_root = ET.fromstring(archive.read(sheet_path))
            for row in sheet_root.findall(".//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}row"):
                values = []
                for cell in row.findall("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}c"):
                    ref = cell.attrib.get("r", "")
                    col_name = re.sub(r"[^A-Z]", "", ref.upper())
                    if col_name:
                        col_index = 0
                        for char in col_name:
                            col_index = col_index * 26 + (ord(char) - ord("A") + 1)
                        while len(values) < col_index:
                            values.append("")
                        values[col_index - 1] = xlsx_cell_text(cell, shared_strings)
                rows.append(values)
        return rows

    def looks_like_header(row):
        first_cells = " ".join(normalize_text(item).casefold() for item in row[:4])
        return any(token in first_cells for token in ["oem", "номер", "артикул", "опис", "цена", "ціна"])

    def normalize_import_rows(raw_rows):
        rows = []
        for row in raw_rows:
            row = [normalize_text(item or "").strip() for item in row]
            if not row or not any(row):
                continue
            if looks_like_header(row):
                continue

            # New Avtopro XLSX price-list layout:
            # A OEM, B brand, C title, D photo URLs, F qty, G price, H availability flag.
            is_xlsx_pricelist = (
                len(row) >= 7
                and row[0]
                and row[1]
                and (not row[3] or "http" in row[3].lower() or "," in row[3])
                and to_float(row[6]) >= 0
                and not ("http" in (row[6] or "").lower())
            )
            if is_xlsx_pricelist:
                part_number = row[0].strip().upper()
                brand = row[1].strip()
                name = row[2].strip() if len(row) > 2 else ""
                gallery = parse_media_urls(row[3] if len(row) > 3 else "")
                qty = to_int(row[5] if len(row) > 5 else 0)
                in_stock = bool_cell(row[7] if len(row) > 7 else "", qty > 0)
                if not in_stock:
                    qty = 0
                price_usd = to_float(row[6] if len(row) > 6 else 0)
                brand_export = brand
                part_number_export = part_number
                avtopro_flag_1 = row[4] if len(row) > 4 else ""
                avtopro_flag_2 = row[7] if len(row) > 7 else ""
                avtopro_flag_3 = ""
                avtopro_flag_4 = ""
                producer_type = producer_type_label(brand)
            else:
                if len(row) < 9:
                    continue
                brand = row[0].strip()
                part_number = row[1].strip().upper()
                name = row[2].strip()
                if not part_number:
                    continue
                price_usd = to_float(row[3] if len(row) > 3 else 0)
                qty = to_int(row[4] if len(row) > 4 else 0)
                in_stock = qty > 0
                gallery = parse_media_urls(row[6] if len(row) > 6 else "")
                producer_type = producer_type_label("OEM" if "OEM" in brand.upper() else "Замінник")
                brand_export = row[7].strip() if len(row) > 7 else brand
                part_number_export = row[8].strip() if len(row) > 8 else part_number
                avtopro_flag_1 = row[5].strip() if len(row) > 5 else ""
                avtopro_flag_2 = row[10].strip() if len(row) > 10 else ""
                avtopro_flag_3 = row[15].strip() if len(row) > 15 else ""
                avtopro_flag_4 = row[16].strip() if len(row) > 16 else ""

            if not part_number:
                continue
            views_seed = int(hashlib.md5(part_number.encode()).hexdigest()[:4], 16)
            rows.append({
                "brand": brand,
                "part_number": part_number,
                "name": name,
                "price_usd": price_usd,
                "qty": qty,
                "in_stock": in_stock,
                "photo_urls": gallery[0] if gallery else "",
                "showcase_photo_urls": dump_media_urls(gallery),
                "has_photo": bool(gallery),
                "has_description": False,
                "producer_type": producer_type,
                "brand_export": brand_export,
                "part_number_export": part_number_export,
                "avtopro_flag_1": avtopro_flag_1,
                "avtopro_flag_2": avtopro_flag_2,
                "avtopro_flag_3": avtopro_flag_3,
                "avtopro_flag_4": avtopro_flag_4,
                "raw_import_row": ";".join(row),
                "views_24h": views_seed % 40,
                "views_168h": (views_seed % 120) + 20,
            })
        return rows

    raw = file_storage.read()
    raw_bytes = raw.encode("utf-8") if isinstance(raw, str) else raw
    filename = normalize_text(getattr(file_storage, "filename", "") or "").strip().lower()
    if filename.endswith((".xlsx", ".xlsm")) or raw_bytes[:2] == b"PK":
        return normalize_import_rows(read_xlsx_rows(raw_bytes))

    content = raw if isinstance(raw, str) else raw.decode("utf-8-sig", errors="ignore")
    reader = csv.reader(io.StringIO(content), delimiter=";")
    return normalize_import_rows(reader)



def wait_for_db(max_attempts=30, delay=2):
    last_error = None
    for _ in range(max_attempts):
        try:
            with engine.connect() as conn:
                conn.exec_driver_sql("SELECT 1")
            return True
        except Exception as e:
            last_error = e
            time.sleep(delay)
    raise last_error

def seed_if_empty():
    Base.metadata.create_all(engine)
    with engine.begin() as conn:
        try:
            conn.exec_driver_sql("ALTER TABLE orders ADD COLUMN IF NOT EXISTS status VARCHAR(32) NOT NULL DEFAULT 'new'")
            conn.exec_driver_sql("ALTER TABLE orders ADD COLUMN IF NOT EXISTS is_processing BOOLEAN NOT NULL DEFAULT false")
            conn.exec_driver_sql("ALTER TABLE orders ADD COLUMN IF NOT EXISTS prepayment_usd NUMERIC(12,2) NOT NULL DEFAULT 0")
            conn.exec_driver_sql("ALTER TABLE orders ADD COLUMN IF NOT EXISTS ttn VARCHAR(64) NOT NULL DEFAULT ''")
            conn.exec_driver_sql("ALTER TABLE orders ADD COLUMN IF NOT EXISTS ttn_status VARCHAR(255) NOT NULL DEFAULT ''")
            conn.exec_driver_sql("ALTER TABLE orders ADD COLUMN IF NOT EXISTS cancel_reason TEXT NOT NULL DEFAULT ''")
            conn.exec_driver_sql("ALTER TABLE orders ADD COLUMN IF NOT EXISTS stock_reserved BOOLEAN NOT NULL DEFAULT false")
            conn.exec_driver_sql("ALTER TABLE orders ADD COLUMN IF NOT EXISTS delivery_type VARCHAR(32) NOT NULL DEFAULT 'pickup'")
            conn.exec_driver_sql("ALTER TABLE orders ADD COLUMN IF NOT EXISTS np_service_type VARCHAR(32) NOT NULL DEFAULT ''")
            conn.exec_driver_sql("ALTER TABLE orders ADD COLUMN IF NOT EXISTS np_city_ref VARCHAR(64) NOT NULL DEFAULT ''")
            conn.exec_driver_sql("ALTER TABLE orders ADD COLUMN IF NOT EXISTS np_warehouse_ref VARCHAR(64) NOT NULL DEFAULT ''")
            conn.exec_driver_sql("ALTER TABLE orders ADD COLUMN IF NOT EXISTS np_warehouse_label VARCHAR(255) NOT NULL DEFAULT ''")
            conn.exec_driver_sql("ALTER TABLE orders ADD COLUMN IF NOT EXISTS np_street_ref VARCHAR(64) NOT NULL DEFAULT ''")
            conn.exec_driver_sql("ALTER TABLE orders ADD COLUMN IF NOT EXISTS np_street_name VARCHAR(255) NOT NULL DEFAULT ''")
            conn.exec_driver_sql("ALTER TABLE orders ADD COLUMN IF NOT EXISTS np_house VARCHAR(64) NOT NULL DEFAULT ''")
            conn.exec_driver_sql("ALTER TABLE orders ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP NOT NULL DEFAULT NOW()")
            conn.exec_driver_sql("ALTER TABLE orders ADD COLUMN IF NOT EXISTS external_source VARCHAR(64) NOT NULL DEFAULT ''")
            conn.exec_driver_sql("ALTER TABLE orders ADD COLUMN IF NOT EXISTS external_order_id VARCHAR(255) NOT NULL DEFAULT ''")
            conn.exec_driver_sql("ALTER TABLE orders ADD COLUMN IF NOT EXISTS external_status VARCHAR(255) NOT NULL DEFAULT ''")
            conn.exec_driver_sql("ALTER TABLE order_items ALTER COLUMN part_id DROP NOT NULL")
            conn.exec_driver_sql("ALTER TABLE warehouses ADD COLUMN IF NOT EXISTS revision_current_index INTEGER NOT NULL DEFAULT 0")
            conn.exec_driver_sql("ALTER TABLE warehouses ADD COLUMN IF NOT EXISTS revision_started_at TIMESTAMP NULL")
            conn.exec_driver_sql("ALTER TABLE parts ADD COLUMN IF NOT EXISTS barcode VARCHAR(8) NOT NULL DEFAULT ''")
            conn.exec_driver_sql("ALTER TABLE parts ADD COLUMN IF NOT EXISTS showcase_photo_urls TEXT NOT NULL DEFAULT '[]'")
            conn.exec_driver_sql("ALTER TABLE parts ADD COLUMN IF NOT EXISTS youtube_url VARCHAR(500) NOT NULL DEFAULT ''")
            conn.exec_driver_sql("ALTER TABLE parts ADD COLUMN IF NOT EXISTS stock_checked_at TIMESTAMP NULL")
            conn.exec_driver_sql("ALTER TABLE parts ADD COLUMN IF NOT EXISTS stock_check_status VARCHAR(32) NOT NULL DEFAULT 'unchecked'")
            conn.exec_driver_sql("ALTER TABLE parts ADD COLUMN IF NOT EXISTS stock_check_note VARCHAR(255) NOT NULL DEFAULT ''")
            conn.exec_driver_sql("ALTER TABLE parts ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN NOT NULL DEFAULT false")
            conn.exec_driver_sql("ALTER TABLE parts ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP NULL")
            conn.exec_driver_sql("ALTER TABLE part_templates ADD COLUMN IF NOT EXISTS unassigned_qty INTEGER NOT NULL DEFAULT 0")
            conn.exec_driver_sql("ALTER TABLE part_templates ADD COLUMN IF NOT EXISTS cross_numbers_json TEXT NOT NULL DEFAULT '[]'")
            conn.exec_driver_sql("ALTER TABLE cars ADD COLUMN IF NOT EXISTS youtube_url VARCHAR(500) NOT NULL DEFAULT ''")
            conn.exec_driver_sql("ALTER TABLE receiving_draft_items ADD COLUMN IF NOT EXISTS barcode VARCHAR(8) NOT NULL DEFAULT ''")
            conn.exec_driver_sql("ALTER TABLE receiving_draft_items ALTER COLUMN warehouse_id DROP NOT NULL")
            conn.exec_driver_sql("ALTER TABLE transit_orders ADD COLUMN IF NOT EXISTS batch_id VARCHAR(64) NOT NULL DEFAULT ''")
            conn.exec_driver_sql("ALTER TABLE transit_orders ADD COLUMN IF NOT EXISTS accepted_qty INTEGER NOT NULL DEFAULT 0")
            conn.exec_driver_sql("ALTER TABLE transit_orders ADD COLUMN IF NOT EXISTS arrival_notified_qty INTEGER NOT NULL DEFAULT 0")
            conn.exec_driver_sql("ALTER TABLE transit_orders ADD COLUMN IF NOT EXISTS labels_printed_at TIMESTAMP NULL")
            conn.exec_driver_sql("ALTER TABLE transit_orders ADD COLUMN IF NOT EXISTS archived_at TIMESTAMP NULL")
            conn.exec_driver_sql("ALTER TABLE packing_requests ADD COLUMN IF NOT EXISTS np_service_type VARCHAR(32) NOT NULL DEFAULT 'warehouse'")
            conn.exec_driver_sql("ALTER TABLE packing_requests ADD COLUMN IF NOT EXISTS np_city_ref VARCHAR(64) NOT NULL DEFAULT ''")
            conn.exec_driver_sql("ALTER TABLE packing_requests ADD COLUMN IF NOT EXISTS np_warehouse_ref VARCHAR(64) NOT NULL DEFAULT ''")
            conn.exec_driver_sql("ALTER TABLE packing_requests ADD COLUMN IF NOT EXISTS np_warehouse_label VARCHAR(255) NOT NULL DEFAULT ''")
            conn.exec_driver_sql("ALTER TABLE packing_requests ADD COLUMN IF NOT EXISTS np_street_ref VARCHAR(64) NOT NULL DEFAULT ''")
            conn.exec_driver_sql("ALTER TABLE packing_requests ADD COLUMN IF NOT EXISTS np_street_name VARCHAR(255) NOT NULL DEFAULT ''")
            conn.exec_driver_sql("ALTER TABLE packing_requests ADD COLUMN IF NOT EXISTS np_house VARCHAR(64) NOT NULL DEFAULT ''")
            conn.exec_driver_sql("ALTER TABLE packing_requests ADD COLUMN IF NOT EXISTS control_payment_uah NUMERIC(12, 2) NOT NULL DEFAULT 0")
            conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS ix_stats_events_event_type ON stats_events (event_type)")
            conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS ix_stats_events_created_at ON stats_events (created_at)")
            conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS ix_stats_events_part_number ON stats_events (part_number)")
            conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS ix_stats_events_query_text ON stats_events (query_text)")
        except Exception:
            pass
    db = SessionLocal()
    try:
        defaults = {
            "nova_poshta_api_key": "",
            "telegram_bot_token": "",
            "telegram_chat_id": "",
            "backup_auto_enabled": os.getenv("BACKUP_AUTO_ENABLED", "1"),
            "backup_sync_enabled": os.getenv("BACKUP_SYNC_ENABLED", "0"),
            "backup_schedule_hour": os.getenv("BACKUP_SCHEDULE_HOUR", "3"),
            "backup_retention_days": os.getenv("BACKUP_RETENTION_DAYS", "30"),
            "backup_rclone_remote": os.getenv("BACKUP_RCLONE_REMOTE", ""),
            "backup_last_status": "",
            "backup_last_message": "",
            "backup_last_file": "",
            "backup_last_at": "",
            "backup_last_auto_date": "",
            "backup_last_sync_status": "",
            "backup_last_sync_message": "",
            "admin_password_hash": generate_password_hash(ADMIN_PASSWORD),
        }
        for k, v in defaults.items():
            if not db.query(ApiSetting).filter(ApiSetting.setting_key == k).one_or_none():
                db.add(ApiSetting(setting_key=k, setting_value=v, updated_at=now()))
        if db.query(NewsFeed).count() == 0:
            flash_news(db, "system", "РЎРёСЃС‚РµРјР° РіРѕС‚РѕРІР°", "РџР»Р°С‚С„РѕСЂРјР° V3 Core Platform РїС–РґРіРѕС‚РѕРІР»РµРЅР° РґРѕ С‚РµСЃС‚СѓРІР°РЅРЅСЏ.", "info")
        if db.query(Warehouse).count() == 0 and os.getenv("ENABLE_DEMO_SEED", "").lower() in {"1", "true", "yes"}:
            warehouse = Warehouse(name="РЎРєР»Р°Рґ 384253", markup_percent=10, created_at=now(), updated_at=now())
            db.add(warehouse)
            db.commit()
            db.refresh(warehouse)
            sample = BASE_DIR / "sample_data" / "warehouse-384253.csv"
            if sample.exists():
                with open(sample, "rb") as fh:
                    class F:
                        filename = "warehouse-384253.csv"
                        def read(self_inner):
                            return fh.read()
                    rows = parse_avtopro_csv(F())
                for item in rows:
                    db.add(Part(
                        warehouse_id=warehouse.id,
                        part_number=item["part_number"],
                        brand=item["brand"],
                        producer_type=item["producer_type"],
                        name=item["name"],
                        description="",
                        price_usd=item["price_usd"],
                        qty=item["qty"],
                        in_stock=item["in_stock"],
                        photo_urls=item["photo_urls"],
                        has_photo=item["has_photo"],
                        has_description=False,
                        views_24h=item["views_24h"],
                        views_168h=item["views_168h"],
                        brand_export=item["brand_export"],
                        part_number_export=item["part_number_export"],
                        avtopro_flag_1=item["avtopro_flag_1"],
                        avtopro_flag_2=item["avtopro_flag_2"],
                        avtopro_flag_3=item["avtopro_flag_3"],
                        avtopro_flag_4=item["avtopro_flag_4"],
                        raw_import_row=item["raw_import_row"],
                        created_at=now(),
                        updated_at=now(),
                    ))
                flash_news(db, "import", "Р‘Р°Р·РѕРІРёР№ СЃРєР»Р°Рґ С–РјРїРѕСЂС‚РѕРІР°РЅРѕ", f"Р†РјРїРѕСЂС‚РѕРІР°РЅРѕ {len(rows)} РїРѕР·РёС†С–Р№ Р· demo CSV.", "success")
            cars = [
                Car(vin="WVWZZZ5NZGW000111", brand="Volkswagen", model="Tiguan", year=2016, mileage=112000, status="in_stock", price_usd=12900, description="РђРІС‚Рѕ РІ РЅР°СЏРІРЅРѕСЃС‚С–, РґРѕРЅРѕСЂ.", image_urls="", created_at=now()),
                Car(vin="1C4RJFBG6FC625222", brand="Jeep", model="Grand Cherokee", year=2015, mileage=138000, status="in_transit", price_usd=15800, description="РџРѕСЃС‚Р°РІРєР° РІ РґРѕСЂРѕР·С– Р·С– РЎРЁРђ.", image_urls="", created_at=now()),
            ]
            for c in cars:
                db.add(c)
            db.commit()
            recalc_warehouse_stats(warehouse)

        for part in db.query(Part).all():
            ensure_part_barcode(db, part)
            if not (part.stock_check_status or "").strip():
                part.stock_check_status = "unchecked"
            if part.stock_check_note is None:
                part.stock_check_note = ""
        for item in db.query(ReceivingDraftItem).all():
            ensure_draft_barcode(db, item)
        db.commit()
    finally:
        db.close()


@app.context_processor
def inject_globals():
    cart = session.get("cart", {})
    cart_count = sum(int(v) for v in cart.values()) if cart else 0
    app_notifications_count = 0
    receiving_draft_count = 0
    availability_open_count = 0
    packing_open_count = 0
    transit_open_count = 0
    orders_new_count = 0
    orders_active_count = 0
    if session.get("admin_auth"):
        db = SessionLocal()
        try:
            app_notifications_count = (
                db.query(AppNotification)
                .filter(AppNotification.status == "open")
                .count()
            )
            receiving_draft_count = db.query(ReceivingDraftItem).count()
            availability_open_count = (
                db.query(AvailabilityRequest)
                .filter(AvailabilityRequest.status.in_(["open", "in_progress"]))
                .count()
            )
            packing_open_count = (
                db.query(PackingRequest)
                .filter(PackingRequest.status.in_(["open", "in_progress", "ready", "issue", "packed"]))
                .count()
            )
            transit_open_count = (
                sum(
                    len(group["items"])
                    for group in group_transit_batches(
                        [
                            serialize_transit_order(db, order)
                            for order in db.query(TransitOrder)
                            .filter(TransitOrder.archived_at.is_(None))
                            .order_by(desc(TransitOrder.created_at), desc(TransitOrder.id))
                            .all()
                        ]
                    )
                )
            )
            for (status,) in db.query(Order.status).all():
                group = order_group_for_status((status or "new").strip() or "new")
                if group == "new":
                    orders_new_count += 1
                elif group == "active":
                    orders_active_count += 1
        finally:
            db.close()
    return {
        "cart_count": cart_count,
        "app_notifications_count": app_notifications_count,
        "receiving_draft_count": receiving_draft_count,
        "availability_open_count": availability_open_count,
        "packing_open_count": packing_open_count,
        "transit_open_count": transit_open_count,
        "orders_new_count": orders_new_count,
        "orders_active_count": orders_active_count,
        "admin_email": session.get("admin_email", ""),
        "stock_status_label": stock_status_label,
        "format_dt": format_dt,
        "clean_text": normalize_text,
        "availability_status_label": availability_status_label,
        "packing_status_label": packing_status_label,
        "transit_status_label": transit_status_label,
        "producer_type_label": producer_type_label,
        "safe_photo": safe_photo,
        "parse_media_urls": parse_media_urls,
        "youtube_embed_url": youtube_embed_url,
        "part_gallery_urls": part_gallery_urls,
        "part_detail_url": part_detail_url,
        "primary_part_photo": primary_part_photo,
        "primary_car_photo": primary_car_photo,
        "primary_template_photo": primary_template_photo,
        "template_gallery_urls": template_gallery_urls,
    }


@app.before_request
def redirect_to_primary_domain():
    host = request.host.split(":", 1)[0].lower()
    forwarded_proto = (request.headers.get("X-Forwarded-Proto") or "").split(",")[0].strip().lower()
    scheme = forwarded_proto or request.scheme or "http"
    if host == "www.usaparts.top" or (host == "usaparts.top" and scheme == "http"):
        target = f"https://usaparts.top{request.full_path}"
        if target.endswith("?"):
            target = target[:-1]
        return redirect(target, code=301)
    track_daily_guest_visit()


@app.route("/robots.txt")
def robots_txt():
    base_url = public_site_base_url()
    content = "\n".join([
        "User-agent: Googlebot-Image",
        "Allow: /favicon.ico",
        "Allow: /favicon.png",
        "Allow: /favicon-48.png",
        "Allow: /favicon-96.png",
        "Allow: /static/",
        "Allow: /uploads/",
        "",
        "User-agent: *",
        "Disallow: /admin/",
        "Disallow: /api/",
        "Disallow: /cart",
        "Disallow: /checkout",
        "Allow: /part/",
        "Allow: /catalog",
        "Allow: /list/",
        "Allow: /static/",
        "Allow: /uploads/",
        "Allow: /favicon.ico",
        "Allow: /favicon.png",
        "Allow: /favicon-48.png",
        "Allow: /favicon-96.png",
        "",
        f"Sitemap: {base_url}/sitemap.xml",
        f"Sitemap: {base_url}/sitemap/parts.xml",
        f"Sitemap: {base_url}/sitemap/images.xml",
        f"Sitemap: {base_url}/sitemap/cars.xml",
        "",
    ])
    response = Response(content, mimetype="text/plain")
    response.headers["Cache-Control"] = "public, max-age=3600"
    return response


@app.route("/favicon.ico")
def favicon_ico():
    response = send_file(BASE_DIR / "static" / "favicon.ico", mimetype="image/x-icon")
    response.headers["Cache-Control"] = "public, max-age=86400"
    return response


@app.route("/favicon-48.png")
def favicon_png_48():
    response = send_file(BASE_DIR / "static" / "favicon-48.png", mimetype="image/png")
    response.headers["Cache-Control"] = "public, max-age=86400"
    return response


@app.route("/favicon-96.png")
def favicon_png_96():
    response = send_file(BASE_DIR / "static" / "favicon-96.png", mimetype="image/png")
    response.headers["Cache-Control"] = "public, max-age=86400"
    return response


@app.route("/favicon.png")
def favicon_png():
    response = send_file(BASE_DIR / "static" / "favicon.png", mimetype="image/png")
    response.headers["Cache-Control"] = "public, max-age=86400"
    return response


@app.route("/apple-touch-icon.png")
def apple_touch_icon():
    response = send_file(BASE_DIR / "static" / "favicon-180.png", mimetype="image/png")
    response.headers["Cache-Control"] = "public, max-age=86400"
    return response


@app.route("/site.webmanifest")
def site_webmanifest():
    payload = {
        "name": "USAparts.top",
        "short_name": "USAparts.top",
        "icons": [
            {
                "src": public_url_for("favicon_png_48"),
                "sizes": "48x48",
                "type": "image/png",
            },
            {
                "src": public_url_for("favicon_png_96"),
                "sizes": "96x96",
                "type": "image/png",
            },
            {
                "src": public_url_for("favicon_png"),
                "sizes": "192x192",
                "type": "image/png",
            },
        ],
        "theme_color": "#0b1118",
        "background_color": "#0b1118",
        "display": "standalone",
    }
    response = Response(
        json.dumps(payload, ensure_ascii=False),
        mimetype="application/manifest+json",
    )
    response.headers["Cache-Control"] = "public, max-age=86400"
    return response


@app.route("/sitemap.xml")
def sitemap_xml():
    today = datetime.utcnow().date().isoformat()
    locations = [
        (public_url_for("sitemap_pages_xml"), today),
        (public_url_for("sitemap_parts_xml"), today),
        (public_url_for("sitemap_images_xml"), today),
        (public_url_for("sitemap_cars_xml"), today),
        (public_url_for("sitemap_brands_xml"), today),
        (public_url_for("sitemap_categories_xml"), today),
        (public_url_for("sitemap_vehicles_xml"), today),
        (public_url_for("sitemap_vehicle_categories_xml"), today),
    ]
    return sitemap_index_response(locations)


@app.route("/sitemap/pages.xml")
def sitemap_pages_xml():
    nodes = [
        sitemap_url_node(public_url_for("home"), changefreq="daily", priority="1.0"),
        sitemap_url_node(public_url_for("catalog"), changefreq="daily", priority="0.9"),
        sitemap_url_node(public_url_for("cars_public"), changefreq="weekly", priority="0.7"),
    ]
    return sitemap_xml_response(nodes)


@app.route("/sitemap/parts.xml")
def sitemap_parts_xml():
    db = SessionLocal()
    try:
        parts = best_unique_public_parts(public_active_parts(db))
        cross_map = cross_numbers_map_for_parts(db, parts)
        nodes = []
        for part in parts:
            nodes.append(
                sitemap_url_node(
                    public_part_url(part),
                    lastmod=sitemap_lastmod(part.updated_at),
                    changefreq="weekly",
                    priority="0.8",
                )
            )
            part_number = normalize_text(part.part_number or "").strip().upper()
            for cross_number in cross_map.get(part_number, []):
                nodes.append(
                    sitemap_url_node(
                        public_cross_part_url(part, cross_number),
                        lastmod=sitemap_lastmod(part.updated_at),
                        changefreq="weekly",
                        priority="0.8",
                    )
                )
        return sitemap_xml_response(nodes)
    finally:
        db.close()


@app.route("/sitemap/images.xml")
def sitemap_images_xml():
    db = SessionLocal()
    try:
        nodes = []
        for part in best_unique_public_parts(public_active_parts(db)):
            gallery = [absolute_public_url(url) for url in part_gallery_urls(part)]
            if not gallery:
                continue
            title = compact_meta_text(part.part_number, part.name, limit=120)
            images = [
                {
                    "loc": url,
                    "title": title,
                    "caption": compact_meta_text("Фото запчастини", part.part_number, part.name, limit=160),
                }
                for url in gallery
            ]
            nodes.append(sitemap_image_url_node(public_part_url(part), images, lastmod=sitemap_lastmod(part.updated_at)))

        cars = db.query(Car).order_by(desc(Car.created_at), desc(Car.id)).all()
        for car in cars:
            photos = [absolute_public_url(url) for url in parse_media_urls(car.image_urls)]
            if not photos:
                continue
            title = compact_meta_text(car.brand, car.model, car.year, limit=120)
            images = [
                {
                    "loc": url,
                    "title": title,
                    "caption": compact_meta_text("Фото авто", car.brand, car.model, car.vin, limit=160),
                }
                for url in photos
            ]
            nodes.append(
                sitemap_image_url_node(
                    public_url_for("car_detail_public", car_id=car.id),
                    images,
                    lastmod=sitemap_lastmod(car.created_at),
                )
            )
        return sitemap_image_xml_response(nodes)
    finally:
        db.close()


@app.route("/sitemap/cars.xml")
def sitemap_cars_xml():
    db = SessionLocal()
    try:
        cars = db.query(Car).order_by(desc(Car.created_at), desc(Car.id)).all()
        nodes = []
        for car in cars:
            nodes.append(
                sitemap_url_node(
                    public_url_for("car_detail_public", car_id=car.id),
                    lastmod=sitemap_lastmod(car.created_at),
                    changefreq="weekly",
                    priority="0.7",
                )
            )
        return sitemap_xml_response(nodes)
    finally:
        db.close()


@app.route("/sitemap/brands.xml")
def sitemap_brands_xml():
    db = SessionLocal()
    try:
        entries = seo_collect_entries(public_active_parts(db))["brands"]
        nodes = [
            sitemap_url_node(
                public_url_for("seo_brand_page", slug=entry["slug"]),
                lastmod=sitemap_lastmod(entry["lastmod"]),
                changefreq="weekly",
                priority="0.65",
            )
            for entry in entries
        ]
        return sitemap_xml_response(nodes)
    finally:
        db.close()


@app.route("/sitemap/categories.xml")
def sitemap_categories_xml():
    db = SessionLocal()
    try:
        entries = seo_collect_entries(public_active_parts(db))["categories"]
        nodes = [
            sitemap_url_node(
                public_url_for("seo_category_page", slug=entry["slug"]),
                lastmod=sitemap_lastmod(entry["lastmod"]),
                changefreq="weekly",
                priority="0.65",
            )
            for entry in entries
        ]
        return sitemap_xml_response(nodes)
    finally:
        db.close()


@app.route("/sitemap/vehicles.xml")
def sitemap_vehicles_xml():
    db = SessionLocal()
    try:
        entries = seo_warehouse_catalog_entries(db)
        nodes = [
            sitemap_url_node(
                public_url_for("seo_vehicle_page", slug=entry["slug"]),
                lastmod=sitemap_lastmod(entry["lastmod"]),
                changefreq="weekly",
                priority="0.65",
            )
            for entry in entries
        ]
        return sitemap_xml_response(nodes)
    finally:
        db.close()


@app.route("/sitemap/vehicle-categories.xml")
def sitemap_vehicle_categories_xml():
    db = SessionLocal()
    try:
        entries = seo_collect_entries(public_active_parts(db))["vehicle_categories"]
        nodes = [
            sitemap_url_node(
                public_url_for(
                    "seo_vehicle_category_page",
                    category_slug=entry["category_slug"],
                    vehicle_slug=entry["vehicle_slug"],
                ),
                lastmod=sitemap_lastmod(entry["lastmod"]),
                changefreq="weekly",
                priority="0.7",
            )
            for entry in entries
        ]
        return sitemap_xml_response(nodes)
    finally:
        db.close()


@app.route("/")
def home():
    db = SessionLocal()
    try:
        q = request.args.get("q", "").strip()
        page = max(int(request.args.get("page") or 1), 1)
        display_count = page * 12
        search_found_without_photo = False
        parts_pool = (
            db.query(Part)
            .filter(Part.in_stock == True, Part.is_deleted == False)
            .order_by(desc(Part.updated_at), desc(Part.id))
            .all()
        )
        cross_map = cross_numbers_map_for_parts(db, parts_pool)
        featured, featured_total = build_showcase_parts(parts_pool, q, limit=display_count, cross_map=cross_map)
        if q and featured_total == 0:
            needle = normalize_text(q).strip().casefold()
            search_found_without_photo = any(public_part_matches_query(part, needle, cross_map) for part in parts_pool)
        has_more = featured_total > len(featured)
        cars_pool = db.query(Car).filter(Car.status == "in_stock").order_by(desc(Car.created_at)).all()
        cars_random = random.sample(cars_pool, min(5, len(cars_pool))) if cars_pool else []
        cars_stock = db.query(Car).filter(Car.status == "in_stock").count()
        cars_transit = db.query(Car).filter(Car.status == "in_transit").count()
        vehicle_warehouses = vehicle_names_from_warehouses(
            db.query(Warehouse).order_by(Warehouse.name.asc()).all()
        )
        warehouse_catalogs = seo_warehouse_catalog_entries(db)
        if q and page == 1:
            track_stats_event(db, "search", query_text=q, meta={"source": "home", "results": featured_total})
            db.commit()
        seo_title = "USAparts.top | Запчастини для авто з США"
        seo_description = "USAparts.top - авторозбірка та автошрот в Україні. Б/у запчастини для авто зі США, пошук по OEM номеру, склади в Україні та поставки зі США."
        return render_template(
            "home.html",
            featured=featured,
            q=q,
            page=page,
            has_more=has_more,
            featured_total=featured_total,
            search_found_without_photo=search_found_without_photo,
            cars_stock=cars_stock,
            cars_transit=cars_transit,
            safe_photo=safe_photo,
            display_usd=display_usd,
            display_uah=display_uah,
            cars_random=cars_random,
            vehicle_warehouses=vehicle_warehouses,
            warehouse_catalogs=warehouse_catalogs[:18],
            seo_title=seo_title,
            seo_description=seo_description,
            canonical_url=public_url_for("home"),
            seo_noindex=bool(q) or page > 1,
            json_ld=build_home_schema(seo_title, seo_description, featured),
        )
    finally:
        db.close()



def _w8_tracking_response_matches(response, vin: str) -> bool:
    try:
        if "/api/cargo-tracking" not in response.url:
            return False
        query_value = parse_qs(urlsplit(response.url).query).get("searchQuery", [""])[0]
        normalized_query = re.sub(r"[^A-Z0-9]", "", str(query_value or "").upper())
        return normalized_query == vin
    except Exception:
        return False


def _w8_payload_contains_vin(payload, vin: str) -> bool:
    if isinstance(payload, dict):
        for value in payload.values():
            if isinstance(value, (dict, list)):
                if _w8_payload_contains_vin(value, vin):
                    return True
                continue
            normalized = re.sub(r"[^A-Z0-9]", "", str(value or "").upper())
            if normalized == vin:
                return True
    elif isinstance(payload, list):
        return any(_w8_payload_contains_vin(item, vin) for item in payload)
    return False


def _lookup_w8_tracking(vin: str):
    w8_url = f"https://w8shipping.com/en/cargo?q={quote(vin)}"
    w8_api_url = f"https://w8shipping.com/api/cargo-tracking?searchQuery={quote(vin)}"
    recaptcha_required = False
    w8_result = {
        "url": w8_url,
        "source": "w8",
        "sourceLabel": "W8 Shipping",
        "vin": vin,
    }

    try:
        api_resp = requests.get(
            w8_api_url,
            headers={
                "User-Agent": W8_TRACKING_USER_AGENT,
                "Accept": "application/json,text/plain,*/*",
                "Referer": w8_url,
            },
            timeout=18,
            allow_redirects=True,
        )
        if api_resp.status_code == 200:
            data = api_resp.json()
            if _w8_payload_contains_vin(data, vin):
                return {**w8_result, "verified": True}
        elif api_resp.status_code == 403 and "recaptcha" in api_resp.text.lower():
            recaptcha_required = True
    except Exception:
        pass

    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
    except Exception:
        return {**w8_result, "verified": False, "needsBrowserVerification": True} if recaptcha_required and len(vin) == 17 else None

    browser = None
    context = None
    result = None
    try:
        with sync_playwright() as playwright:
            launch_kwargs = {
                "headless": True,
                "args": [
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                ],
            }
            chromium_path = next(
                (path for path in W8_TRACKING_BROWSER_CANDIDATES if path and os.path.exists(path)),
                None,
            )
            if chromium_path:
                launch_kwargs["executable_path"] = chromium_path
            browser = playwright.chromium.launch(**launch_kwargs)
            context = browser.new_context(
                viewport={"width": 1366, "height": 900},
                user_agent=W8_TRACKING_USER_AGENT,
                locale="en-US",
                timezone_id="America/New_York",
                java_script_enabled=True,
            )
            page = context.new_page()
            page.add_init_script(W8_TRACKING_STEALTH_SCRIPT)
            try:
                with page.expect_response(
                    lambda r: _w8_tracking_response_matches(r, vin),
                    timeout=W8_TRACKING_TIMEOUT_MS,
                ) as response_info:
                    page.goto(w8_url, wait_until="domcontentloaded", timeout=60000)
                    try:
                        page.wait_for_load_state("networkidle", timeout=15000)
                    except PlaywrightTimeoutError:
                        pass
                response = response_info.value
            except PlaywrightTimeoutError:
                response = None
            if response is None:
                data = None
            elif response.status == 403:
                recaptcha_required = True
                data = None
            elif response.status != 200:
                data = None
            else:
                data = response.json()
            if isinstance(data, list):
                for item in data:
                    if not isinstance(item, dict):
                        continue
                    item_vin = re.sub(r"[^A-Z0-9]", "", str(item.get("vin") or "").upper())
                    if item_vin == vin:
                        result = {
                            **w8_result,
                            "verified": True,
                        }
                        break
    except Exception:
        result = None
    finally:
        if context is not None:
            try:
                context.close()
            except Exception:
                pass
        if browser is not None:
            try:
                browser.close()
            except Exception:
                pass
    if result:
        return result
    if recaptcha_required and len(vin) == 17:
        return {**w8_result, "verified": False, "needsBrowserVerification": True}
    return None


def _lookup_yoauto_tracking(vin: str):
    yoauto_api = f"https://yoauto.net/api/order/tracking/{quote(vin)}"
    yoauto_result_url = f"https://yoauto.net/tracking/{quote(vin)}"
    try:
        resp = requests.get(yoauto_api, timeout=15, allow_redirects=True)
        if resp.status_code != 200:
            return None
        data = resp.json()
        if not isinstance(data, dict):
            return None
        order_number = re.sub(r"[^A-Z0-9]", "", str(data.get("orderNumber") or "").upper())
        vehicle = data.get("orderVehicleDetails") if isinstance(data.get("orderVehicleDetails"), dict) else {}
        vehicle_vin = re.sub(r"[^A-Z0-9]", "", str(vehicle.get("VIN") or "").upper())
        if vin in {order_number, vehicle_vin}:
            return {
                "url": yoauto_result_url,
                "source": "yoauto",
                "sourceLabel": "YO Auto",
                "vin": vin,
                "verified": True,
            }
    except Exception:
        return None
    return None


@app.route("/api/tracking/lookup", methods=["POST"])
def tracking_lookup():
    payload = request.get_json(silent=True) or {}
    vin = re.sub(r"[^A-Z0-9]", "", (payload.get("vin") or "").strip().upper())
    if len(vin) < 6:
        return jsonify({"ok": False, "error": "VIN занадто короткий."})

    olicargo_api = f"https://api.olicargo.lt/public/lookup?code={vin}"
    olicargo_result_url = f"https://tracking.olicargo.lt/public/search?code={vin}"
    try:
        resp = requests.get(olicargo_api, timeout=12, allow_redirects=True)
        if resp.status_code == 200:
            data = resp.json()
            vehicles = data.get("vehicles") if isinstance(data, dict) else None
            if data and (
                data.get("trackable")
                or data.get("tracked")
                or data.get("code")
                or (isinstance(vehicles, list) and len(vehicles) > 0)
            ):
                return jsonify({
                    "ok": True,
                    "url": olicargo_result_url,
                    "source": "olicargo",
                    "sourceLabel": "OliCargo",
                    "vin": vin,
                    "verified": True,
                })
    except Exception:
        pass

    yoauto_result = _lookup_yoauto_tracking(vin)
    if yoauto_result:
        return jsonify({"ok": True, **yoauto_result})

    w8_result = _lookup_w8_tracking(vin)
    if w8_result:
        return jsonify({"ok": True, **w8_result})

    return jsonify({
        "ok": False,
        "vin": vin,
        "error": (
            "Авто не знайдено у підтвердженому трекінгу. "
            "Сторінка відкривається тільки для VIN, які реально знайдені на зовнішньому сервісі."
        ),
        "source": "tracking",
        "sourceLabel": "OliCargo / W8 Shipping / YO Auto",
        "verified": False,
    }), 404

@app.route("/catalog")
def catalog():
    db = SessionLocal()
    try:
        q = request.args.get("q", "").strip()
        condition = request.args.get("condition", "").strip()
        parts_pool = (
            db.query(Part)
            .filter(Part.is_deleted == False, Part.in_stock == True)
            .order_by(desc(Part.views_168h), Part.part_number.asc())
            .all()
        )
        search_found_without_photo = False
        if condition == "used":
            parts_pool = [part for part in parts_pool if producer_type_label(part.producer_type) == "Замінник"]
        elif condition == "new":
            parts_pool = [part for part in parts_pool if producer_type_label(part.producer_type) == "OEM"]
        cross_map = cross_numbers_map_for_parts(db, parts_pool)
        parts, parts_total = build_showcase_parts(parts_pool, q, limit=max(len(parts_pool), 1), cross_map=cross_map)
        warehouse_catalogs = seo_warehouse_catalog_entries(db)
        if q and parts_total == 0:
            needle = normalize_text(q).strip().casefold()
            search_found_without_photo = any(public_part_matches_query(part, needle, cross_map) for part in parts_pool)
        if q:
            track_stats_event(db, "search", query_text=q, meta={"source": "catalog", "results": parts_total})
            db.commit()
        return render_template(
            "catalog.html",
            parts=parts,
            q=q,
            condition=condition,
            warehouse_catalogs=warehouse_catalogs,
            search_found_without_photo=search_found_without_photo,
            safe_photo=safe_photo,
            display_usd=display_usd,
            display_uah=display_uah,
            seo_title="Каталог запчастин | USAparts.top",
            seo_description="Каталог запчастин для авто з США. Пошук по OEM номеру, назві, бренду та швидке оформлення замовлення.",
            canonical_url=public_url_for("catalog"),
            seo_noindex=bool(q) or bool(condition),
        )
    finally:
        db.close()


@app.route("/brand/<slug>")
def seo_brand_page(slug):
    db = SessionLocal()
    try:
        all_parts = best_unique_public_parts(public_active_parts(db))
        entries = seo_collect_entries(all_parts)
        brand = next((entry for entry in entries["brands"] if entry["slug"] == slug), None)
        if not brand:
            return redirect(url_for("catalog"), code=302)
        parts = seo_filter_parts(all_parts, brand_slug=slug)
        title = f"Запчастини {brand['label']}"
        description = f"Каталог запчастин {brand['label']} на USAparts.top. Пошук і замовлення по OEM номеру, фото, ціна та наявність на складі."
        return render_seo_listing(
            parts,
            title,
            description,
            public_url_for("seo_brand_page", slug=slug),
            intro=f"Сторінка автоматично формується з товарів, де бренд або виробник визначений як {brand['label']}. Для точного підбору використовуйте OEM номер запчастини.",
            related={"categories": entries["categories"][:12], "vehicles": entries["vehicles"][:12]},
        )
    finally:
        db.close()


@app.route("/vehicle/<slug>")
def seo_vehicle_page(slug):
    db = SessionLocal()
    try:
        all_parts = best_unique_public_parts(public_active_parts(db))
        entries = seo_collect_entries(all_parts)
        warehouse_entries = seo_warehouse_catalog_entries(db)
        vehicle = next((entry for entry in warehouse_entries if entry["slug"] == slug), None)
        if not vehicle:
            return redirect(url_for("catalog"), code=302)
        parts = seo_filter_parts(all_parts, vehicle_slug=slug)
        title = f"Запчастини {vehicle['label']}"
        description = f"Б/у запчастини для {vehicle['label']} з авто зі США. Наявність, фото, ціна та швидке замовлення по OEM номеру."
        return render_seo_listing(
            parts,
            title,
            description,
            public_url_for("seo_vehicle_page", slug=slug),
            intro=f"Підбірка формується автоматично зі складів, назва яких відповідає {vehicle['label']}. Якщо потрібна сумісність, орієнтуйтесь на OEM номер і звертайтесь до менеджера.",
            related={"categories": entries["categories"][:12], "brands": entries["brands"][:12]},
        )
    finally:
        db.close()


@app.route("/list/<slug>")
def seo_category_page(slug):
    db = SessionLocal()
    try:
        all_parts = best_unique_public_parts(public_active_parts(db))
        entries = seo_collect_entries(all_parts)
        category = next((entry for entry in entries["categories"] if entry["slug"] == slug), None)
        if not category:
            return redirect(url_for("catalog"), code=302)
        parts = seo_filter_parts(all_parts, category_slug=slug)
        title = f"{category['label']} для авто з США"
        description = f"{category['label']} в наявності на USAparts.top. Підбір по OEM номеру, фото товару, ціна в USD та гривні."
        return render_seo_listing(
            parts,
            title,
            description,
            public_url_for("seo_category_page", slug=slug),
            intro=f"Категорія визначається автоматично з опису та назви запчастини. Нові товари з імпорту одразу потрапляють сюди, якщо текст відповідає категорії «{category['label']}».",
            related={"vehicles": entries["vehicles"][:12], "brands": entries["brands"][:12]},
        )
    finally:
        db.close()


@app.route("/list/<category_slug>/<vehicle_slug>")
def seo_vehicle_category_page(category_slug, vehicle_slug):
    db = SessionLocal()
    try:
        all_parts = best_unique_public_parts(public_active_parts(db))
        entries = seo_collect_entries(all_parts)
        category = next((entry for entry in entries["categories"] if entry["slug"] == category_slug), None)
        vehicle = next((entry for entry in entries["vehicles"] if entry["slug"] == vehicle_slug), None)
        if not category or not vehicle:
            return redirect(url_for("catalog"), code=302)
        parts = seo_filter_parts(all_parts, category_slug=category_slug, vehicle_slug=vehicle_slug)
        if not parts:
            return redirect(url_for("seo_category_page", slug=category_slug), code=302)
        title = f"{category['label']} {vehicle['label']}"
        description = f"{category['label']} для {vehicle['label']} з авто зі США. Наявні позиції з фото, ціною та OEM номером."
        return render_seo_listing(
            parts,
            title,
            description,
            public_url_for("seo_vehicle_category_page", category_slug=category_slug, vehicle_slug=vehicle_slug),
            intro=f"SEO-сторінка створена автоматично з перетину категорії «{category['label']}» і складу/авто «{vehicle['label']}».",
            related={"categories": entries["categories"][:12], "vehicles": entries["vehicles"][:12]},
        )
    finally:
        db.close()


@app.route("/part/<int:part_id>")
@app.route("/part/<int:part_id>/<slug>")
def part_detail(part_id, slug=None):
    db = SessionLocal()
    try:
        part = db.get(Part, part_id)
        if not part or part.is_deleted:
            flash("Товар не знайдено", "error")
            return redirect(url_for("catalog"))
        canonical_slug = part_seo_slug(part)
        if slug != canonical_slug:
            return redirect(url_for("part_detail", part_id=part.id, slug=canonical_slug), code=301)
        warehouse = db.get(Warehouse, part.warehouse_id)
        part.views_24h += 1
        part.views_168h += 1
        track_stats_event(db, "part_view", part=part)
        db.commit()
        part_title = compact_meta_text(part.part_number, part.name, "купити запчастину з США", limit=95)
        part_price = display_usd(part.price_usd, warehouse.markup_percent if warehouse else 0)
        part_og_image = absolute_public_url(primary_part_photo(part)) if primary_part_photo(part) else ""
        return render_template(
            "part_detail.html",
            part=part,
            warehouse=warehouse,
            safe_photo=safe_photo,
            display_usd=display_usd,
            display_uah=display_uah,
            seo_title=f"{part_title} | USAparts.top",
            seo_description=compact_meta_text(
                "Купити запчастину",
                part.part_number,
                part.name,
                f"ціна ${part_price}",
                f"наявність {int(part.qty or 0)} шт.",
            ),
            canonical_url=public_part_url(part),
            og_type="product",
            og_image_url=part_og_image,
            json_ld=build_part_product_schema(part, warehouse),
        )
    finally:
        db.close()


@app.route("/cross/<cross_number>/<int:part_id>")
@app.route("/cross/<cross_number>/<int:part_id>/<slug>")
def cross_part_detail(cross_number, part_id, slug=None):
    db = SessionLocal()
    try:
        clean_cross = normalize_cross_number(cross_number)
        part = db.get(Part, part_id)
        if not clean_cross or not part or part.is_deleted:
            flash("Товар не знайдено", "error")
            return redirect(url_for("catalog"))
        template = find_part_template_by_cross(db, clean_cross)
        if not template or compact_part_code(template.part_number or "") != compact_part_code(part.part_number or ""):
            return redirect(part_detail_url(part), code=302)
        canonical_slug = part_seo_slug_from_values(clean_cross, part.name)
        if slug != canonical_slug:
            return redirect(cross_part_detail_url(part, clean_cross), code=301)
        warehouse = db.get(Warehouse, part.warehouse_id)
        part.views_24h += 1
        part.views_168h += 1
        track_stats_event(db, "part_view", part=part, meta={"crossNumber": clean_cross})
        db.commit()
        part_title = compact_meta_text(clean_cross, part.name, "крос-номер запчастини з США", limit=95)
        part_price = display_usd(part.price_usd, warehouse.markup_percent if warehouse else 0)
        part_og_image = absolute_public_url(primary_part_photo(part)) if primary_part_photo(part) else ""
        cross_url = public_cross_part_url(part, clean_cross)
        return render_template(
            "part_detail.html",
            part=part,
            warehouse=warehouse,
            display_part_number=clean_cross,
            main_part_number=part.part_number,
            safe_photo=safe_photo,
            display_usd=display_usd,
            display_uah=display_uah,
            seo_title=f"{part_title} | USAparts.top",
            seo_description=compact_meta_text(
                "Купити запчастину по крос-номеру",
                clean_cross,
                part.name,
                f"основний OEM {part.part_number}",
                f"ціна ${part_price}",
            ),
            canonical_url=cross_url,
            og_type="product",
            og_image_url=part_og_image,
            json_ld=build_part_product_schema(part, warehouse, display_part_number=clean_cross, canonical_url=cross_url),
        )
    finally:
        db.close()


@app.route("/part/<int:part_id>/seller-request", methods=["POST"])
def part_seller_request(part_id):
    db = SessionLocal()
    try:
        part = db.get(Part, part_id)
        if not part or part.is_deleted:
            flash("Товар не знайдено.", "error")
            return redirect(url_for("catalog"))

        phone = normalize_ua_phone(request.form.get("phone", ""))
        if not phone:
            flash("Вкажіть 9 цифр номера телефону після +380.", "error")
            return redirect(f"{part_detail_url(part)}#seller-request")

        warehouse = db.get(Warehouse, part.warehouse_id)
        part_name = normalize_text(part.name or "")
        message = "\n".join(
            [
                "❗ Запит у продавця",
                "",
                f"Номер запчастини: {part.part_number}",
                f"Опис: {part_name or 'Без опису'}",
                f"Телефон: {phone}",
                f"Склад: {(warehouse.name if warehouse else 'Без складу')}",
                f"Посилання: {public_part_url(part)}",
                f"Час: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
            ]
        )
        send_telegram_message(db, message)
        flash_news(
            db,
            "telegram",
            "Новий запит у продавця",
            f"{part.part_number} • {phone} • {part_name or 'Без опису'}",
            "info",
        )
        track_stats_event(db, "seller_request", part=part, meta={"warehouse": warehouse.name if warehouse else ""})
        db.commit()
        flash("Дякуємо з вами зв'яжуться в самий короткий термін", "success")
        return redirect(part_detail_url(part))
    except Exception as exc:
        db.rollback()
        flash(f"Не вдалося відправити запит: {exc}", "error")
        return redirect(f"{url_for('part_detail', part_id=part_id)}#seller-request")
    finally:
        db.close()


@app.route("/cars")
def cars_public():
    db = SessionLocal()
    try:
        status = request.args.get("status", "")
        cars_q = db.query(Car)
        if status:
            cars_q = cars_q.filter(Car.status == status)
        cars = cars_q.order_by(desc(Car.created_at)).all()
        status_label = "Авто в дорозі" if status == "in_transit" else "Авто в наявності" if status == "in_stock" else "Авто з США"
        return render_template(
            "cars.html",
            cars=cars,
            status=status,
            safe_photo=safe_photo,
            seo_title=f"{status_label} | USAparts.top",
            seo_description="Авто з США: перегляд фото, опису, VIN та статусу авто в наявності або в дорозі.",
            canonical_url=public_url_for("cars_public"),
            seo_noindex=bool(status),
        )
    finally:
        db.close()


@app.route("/cars/<int:car_id>")
def car_detail_public(car_id):
    db = SessionLocal()
    try:
        car = db.get(Car, car_id)
        if not car:
            flash("Авто не знайдено", "error")
            return redirect(url_for("cars_public"))
        photos = parse_media_urls(car.image_urls)
        video_url = youtube_embed_url(car.youtube_url)
        car_title = compact_meta_text(car.brand, car.model, car.year, limit=90)
        return render_template(
            "car_detail.html",
            car=car,
            photos=photos,
            video_url=video_url,
            seo_title=f"{car_title} | USAparts.top",
            seo_description=compact_meta_text(
                car.brand,
                car.model,
                car.year,
                car.vin,
                "фото, опис, VIN трекінг та статус авто.",
            ),
            canonical_url=public_url_for("car_detail_public", car_id=car.id),
            og_type="product",
            og_image_url=absolute_public_url(photos[0]) if photos else "",
            json_ld=build_car_product_schema(car, photos),
        )
    finally:
        db.close()


@app.route("/cart/add/<int:part_id>", methods=["POST"])
def cart_add(part_id):
    db = SessionLocal()
    try:
        part = db.get(Part, part_id)
        if not part or part.is_deleted:
            flash("Товар більше недоступний", "error")
            return redirect(request.referrer or url_for("catalog"))
        available_qty = available_part_qty(part)
        if available_qty <= 0:
            flash("Товар зараз відсутній на складі", "error")
            return redirect(request.referrer or url_for("catalog"))
        cart = session.get("cart", {})
        current_qty = max(int(cart.get(str(part_id), 0) or 0), 0)
        if current_qty >= available_qty:
            flash(f"Для {part.part_number or 'цієї позиції'} доступно лише {available_qty} шт.", "error")
            return redirect(request.referrer or url_for("catalog"))
        cart[str(part_id)] = current_qty + 1
        session["cart"] = cart
        track_stats_event(db, "cart_add", part=part, quantity=1, meta={"cart_qty": current_qty + 1})
        db.commit()
        flash("Товар додано в корзину", "success")
        return redirect(request.referrer or url_for("catalog"))
    finally:
        db.close()



@app.route("/cart")
def cart_view():
    db = SessionLocal()
    try:
        snapshot = build_cart_state(db, session.get("cart", {}))
        if snapshot["normalized_cart"] != (session.get("cart", {}) or {}):
            session["cart"] = snapshot["normalized_cart"]
            flash_cart_issues(snapshot["issues"])
        return render_template("cart.html", items=snapshot["items"], total=snapshot["total"])
    finally:
        db.close()

@app.route("/cart/remove/<int:part_id>", methods=["POST"])
def cart_remove(part_id):
    cart = session.get("cart", {})
    cart.pop(str(part_id), None)
    session["cart"] = cart
    flash("РџРѕР·РёС†С–СЋ РїСЂРёР±СЂР°РЅРѕ Р· РєРѕСЂР·РёРЅРё", "success")
    return redirect(url_for("cart_view"))


@app.route("/checkout", methods=["POST"])
def checkout():
    db = SessionLocal()
    try:
        snapshot = build_cart_state(db, session.get("cart", {}), lock_rows=True)
        items = snapshot["items"]
        total = snapshot["total"]
        normalized_cart = snapshot["normalized_cart"]
        if not items:
            session["cart"] = normalized_cart
            flash("Кошик порожній", "error")
            return redirect(url_for("cart_view"))
        if normalized_cart != (session.get("cart", {}) or {}):
            session["cart"] = normalized_cart
            flash_cart_issues(snapshot["issues"])
            flash("Кількість у кошику оновлено відповідно до фактичного залишку.", "error")
            return redirect(url_for("cart_view"))

        city_name = request.form.get("city_name", "").strip()
        delivery_type = request.form.get("delivery_type", "").strip()
        warehouse_label = request.form.get("warehouse_label", "").strip()
        street_name = request.form.get("street_name", "").strip()
        house = request.form.get("house", "").strip()
        np_payload = {
            "service_type": normalized_np_service_type(delivery_type),
            "city_name": city_name,
            "city_ref": request.form.get("city_ref", "").strip(),
            "warehouse_ref": request.form.get("warehouse_ref", "").strip(),
            "warehouse_label": warehouse_label,
            "street_ref": request.form.get("street_ref", "").strip(),
            "street_name": street_name,
            "house": house,
        }
        address = warehouse_label if delivery_type in ("warehouse", "postomat") else ", ".join([x for x in [street_name, house] if x])
        comment = request.form.get("comment", "").strip()

        order = Order(
            customer_name=request.form.get("customer_name", "").strip(),
            phone=request.form.get("phone", "").strip(),
            city=city_name,
            delivery_type="nova_poshta",
            np_service_type=np_payload["service_type"],
            np_city_ref=np_payload["city_ref"],
            np_warehouse_ref=np_payload["warehouse_ref"],
            np_warehouse_label=np_payload["warehouse_label"],
            np_street_ref=np_payload["street_ref"],
            np_street_name=np_payload["street_name"],
            np_house=np_payload["house"],
            comment=f"{delivery_type}: {address}. {comment}".strip(),
            total_usd=total,
            status="new",
            is_processing=False,
            prepayment_usd=0,
            ttn="",
            ttn_status="",
            cancel_reason="",
            stock_reserved=False,
            created_at=now(),
            updated_at=now(),
        )
        db.add(order)
        db.flush()
        for item in items:
            order.items.append(OrderItem(
                part_id=item["part"].id,
                part_number=item["part"].part_number,
                name=item["part"].name,
                qty=item["qty"],
                price_usd=item["part"].price_usd
            ))
            track_stats_event(
                db,
                "order_item",
                part=item["part"],
                quantity=item["qty"],
                order_id=order.id,
                meta={"customer": order.customer_name, "city": city_name},
            )
        db.flush()
        reserve_order_inventory(db, order)
        flash_news(db, "orders", "РќРѕРІРµ Р·Р°РјРѕРІР»РµРЅРЅСЏ", f"Р—Р°РјРѕРІР»РµРЅРЅСЏ #{order.id}: {city_name}, {address}.", "success")
        db.commit()
        try:
            send_telegram_message(
                db,
                build_order_telegram_message(order, items, city_name, address, comment),
            )
            flash_news(db, "telegram", "Нове замовлення в Telegram", f"Замовлення #{order.id} відправлено в Telegram.", "info")
            db.commit()
        except Exception as telegram_error:
            flash_news(db, "telegram", "Помилка Telegram", f"Замовлення #{order.id} створено, але Telegram не прийняв повідомлення: {telegram_error}", "error")
            db.commit()
        session["cart"] = {}
        flash("Дякуємо, ваше замовлення успішно прийнято. Менеджер зв'яжеться з вами за вказаними контактними даними найближчим часом", "success")
        return redirect(url_for("home"))
    except ValueError as exc:
        db.rollback()
        snapshot = build_cart_state(db, session.get("cart", {}))
        session["cart"] = snapshot["normalized_cart"]
        flash(inventory_reserve_error_message(str(exc)), "error")
        flash_cart_issues(snapshot["issues"])
        return redirect(url_for("cart_view"))
    finally:
        db.close()


@app.route("/api/np/cities")
def np_cities():
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify({"items": []})
    db = SessionLocal()
    try:
        # According to NP API 2.0 docs, getCities works in Address model and supports FindByString.
        items, raw = np_request(db, "Address", "getCities", {"FindByString": q})
        result = []
        for city in items:
            result.append({
                "ref": city.get("Ref"),
                "name": city.get("Description") or city.get("DescriptionRu") or ""
            })
        return jsonify({"items": result})
    except Exception as e:
        return jsonify({"items": [], "error": str(e)})
    finally:
        db.close()

@app.route("/api/np/warehouses")
def np_warehouses():
    city_ref = request.args.get("city_ref", "").strip()
    delivery_type = request.args.get("delivery_type", "warehouse").strip()
    if not city_ref:
        return jsonify({"items": []})
    db = SessionLocal()
    try:
        items, raw = np_request(db, "Address", "getWarehouses", {"CityRef": city_ref})
        result = []
        for w in items:
            label = w.get("Description") or w.get("ShortAddress") or ""
            lowered = label.lower()
            is_postomat = "РїРѕС€С‚РѕРјР°С‚" in lowered
            if delivery_type == "postomat" and not is_postomat:
                continue
            if delivery_type == "warehouse" and is_postomat:
                continue
            result.append({"ref": w.get("Ref"), "label": label})
        return jsonify({"items": result})
    except Exception as e:
        return jsonify({"items": [], "error": str(e)})
    finally:
        db.close()

@app.route("/api/np/streets")
def np_streets():
    city_ref = request.args.get("city_ref", "").strip()
    q = request.args.get("q", "").strip()
    if not city_ref or len(q) < 2:
        return jsonify({"items": []})
    db = SessionLocal()
    try:
        items, raw = np_request(db, "Address", "getStreet", {"CityRef": city_ref, "FindByString": q})
        result = [{"ref": s.get("Ref"), "label": s.get("Description") or ""} for s in items]
        return jsonify({"items": result})
    except Exception as e:
        return jsonify({"items": [], "error": str(e)})
    finally:
        db.close()


@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    if request.method == "POST":
        email = request.form.get("email", "").strip()
        password = request.form.get("password", "").strip()
        db = SessionLocal()
        try:
            is_valid = email == ADMIN_EMAIL and verify_admin_password(db, password)
        finally:
            db.close()
        if is_valid:
            session["admin_auth"] = True
            session["admin_email"] = email
            return redirect(url_for("admin_parts"))
        flash("Невірний логін або пароль", "error")
    return render_template("admin_login.html")


@app.route("/admin/logout")
def admin_logout():
    session.pop("admin_auth", None)
    session.pop("admin_email", None)
    return redirect(url_for("home"))


@app.route("/admin/account", methods=["GET", "POST"])
@admin_required
def admin_account():
    db = SessionLocal()
    try:
        if request.method == "POST":
            current_password = request.form.get("current_password", "").strip()
            new_password = request.form.get("new_password", "").strip()
            confirm_password = request.form.get("confirm_password", "").strip()
            if not verify_admin_password(db, current_password):
                flash("Поточний пароль введено невірно", "error")
                return redirect(url_for("admin_account"))
            if len(new_password) < 6:
                flash("Новий пароль має містити щонайменше 6 символів", "error")
                return redirect(url_for("admin_account"))
            if new_password != confirm_password:
                flash("Підтвердження пароля не збігається", "error")
                return redirect(url_for("admin_account"))
            set_admin_password(db, new_password)
            flash_news(db, "account", "Пароль входу змінено", "Адміністративний пароль було оновлено.", "info")
            db.commit()
            flash("Пароль успішно змінено", "success")
            return redirect(url_for("admin_account"))
        news = db.query(NewsFeed).order_by(desc(NewsFeed.created_at)).limit(12).all()
        return render_template("admin_account.html", news=news)
    finally:
        db.close()


@app.route("/admin/backup")
@admin_required
def admin_backup():
    db = SessionLocal()
    try:
        settings = get_api_settings_map(db)
        news = db.query(NewsFeed).order_by(desc(NewsFeed.created_at)).limit(12).all()
        return render_template(
            "admin_backup.html",
            news=news,
            settings=settings,
            backup_settings=get_backup_settings(settings),
            tool_status=backup_tool_status(),
            backup_files=list_backup_files(),
        )
    finally:
        db.close()


@app.route("/admin/backup/settings", methods=["POST"])
@admin_required
def admin_backup_settings():
    db = SessionLocal()
    try:
        backup_values = {
            "backup_auto_enabled": "1" if request.form.get("backup_auto_enabled") else "0",
            "backup_sync_enabled": "1" if request.form.get("backup_sync_enabled") else "0",
            "backup_schedule_hour": str(parse_backup_int(request.form.get("backup_schedule_hour"), 3, 0, 23)),
            "backup_retention_days": str(parse_backup_int(request.form.get("backup_retention_days"), 30, 1, 3650)),
            "backup_rclone_remote": normalize_text(request.form.get("backup_rclone_remote") or "").strip(),
        }
        for key, value in backup_values.items():
            set_setting(db, key, value)
        flash_news(db, "backup", "Налаштування бекапу оновлено", "Оновлено автоматичний бекап і Google Drive sync.", "info")
        db.commit()
        flash("Налаштування бекапу збережені.", "success")
        return redirect(url_for("admin_backup"))
    finally:
        db.close()


@app.route("/admin/backup/create", methods=["POST"])
@admin_required
def admin_backup_create():
    db = SessionLocal()
    try:
        backup_settings = get_backup_settings(get_api_settings_map(db))
        sync_to_drive = bool(request.form.get("sync_to_drive")) and backup_settings["sync_enabled"]
    finally:
        db.close()

    try:
        result = create_site_backup(sync_to_drive=sync_to_drive, triggered_by="manual")
        db = SessionLocal()
        try:
            record_backup_result(db, result=result, auto=False)
            flash_news(
                db,
                "backup",
                "Повний бекап створено",
                f"{result['filename']} • {result['size_mb']} MB",
                "success",
            )
            db.commit()
        finally:
            db.close()
        sync = result.get("sync") or {}
        if sync.get("ok"):
            flash("Повний бекап створено і синхронізовано з Google Drive.", "success")
        elif sync_to_drive:
            flash(f"Бекап створено, але Google Drive sync не завершився: {sync.get('message', '')}", "error")
        else:
            flash("Повний бекап створено.", "success")
    except Exception as exc:
        db = SessionLocal()
        try:
            record_backup_result(db, error=exc, auto=False)
            flash_news(db, "backup", "Помилка створення бекапу", str(exc)[:500], "error")
            db.commit()
        finally:
            db.close()
        flash(f"Не вдалося створити бекап: {exc}", "error")
    return redirect(url_for("admin_backup"))


@app.route("/admin/backup/download/<path:filename>")
@admin_required
def admin_backup_download(filename):
    backup_path = safe_backup_file(filename)
    if not backup_path:
        flash("Файл бекапу не знайдено.", "error")
        return redirect(url_for("admin_backup"))
    return send_file(backup_path, as_attachment=True, download_name=backup_path.name)


@app.route("/admin")
@admin_required
def admin_root():
    return redirect(url_for("admin_parts"))


@app.route("/admin/statistics")
@admin_required
def admin_statistics():
    db = SessionLocal()
    try:
        mode, start_date, end_date, start_dt, end_dt = stats_date_range_from_request()
        events = (
            db.query(StatsEvent)
            .filter(StatsEvent.created_at >= start_dt, StatsEvent.created_at < end_dt)
            .order_by(desc(StatsEvent.created_at), desc(StatsEvent.id))
            .all()
        )
        by_type = {}
        for event in events:
            by_type.setdefault(event.event_type, []).append(event)

        daily_rows = {}
        for event in events:
            day_key = event.created_at.strftime("%Y-%m-%d") if event.created_at else ""
            row = daily_rows.setdefault(
                day_key,
                {
                    "date": day_key,
                    "guests": set(),
                    "part_views": 0,
                    "searches": 0,
                    "seller_requests": 0,
                    "cart_adds": 0,
                    "orders": 0,
                },
            )
            if event.event_type == "guest_visit" and event.visitor_id:
                row["guests"].add(event.visitor_id)
            elif event.event_type == "part_view":
                row["part_views"] += 1
            elif event.event_type == "search":
                row["searches"] += 1
            elif event.event_type == "seller_request":
                row["seller_requests"] += 1
            elif event.event_type == "cart_add":
                row["cart_adds"] += max(int(event.quantity or 0), 0)
            elif event.event_type == "order_item":
                row["orders"] += max(int(event.quantity or 0), 0)
        day_stats = []
        for row in daily_rows.values():
            row["guests_count"] = len(row.pop("guests"))
            day_stats.append(row)
        day_stats.sort(key=lambda row: row["date"], reverse=True)

        summary = {
            "guests": len({event.visitor_id for event in by_type.get("guest_visit", []) if event.visitor_id}),
            "part_views": len(by_type.get("part_view", [])),
            "searches": len(by_type.get("search", [])),
            "seller_requests": len(by_type.get("seller_request", [])),
            "cart_adds": sum(max(int(event.quantity or 0), 0) for event in by_type.get("cart_add", [])),
            "ordered_items": sum(max(int(event.quantity or 0), 0) for event in by_type.get("order_item", [])),
        }
        event_labels = {
            "guest_visit": "Гість",
            "part_view": "Перегляд товару",
            "search": "Пошук",
            "seller_request": "Запит продавцю",
            "cart_add": "Додано в кошик",
            "order_item": "Замовлено",
        }
        news = db.query(NewsFeed).order_by(desc(NewsFeed.created_at)).limit(12).all()
        return render_template(
            "admin_statistics.html",
            mode=mode,
            start_date=start_date,
            end_date=end_date,
            summary=summary,
            day_stats=day_stats,
            search_stats=aggregate_search_stats(by_type.get("search", [])),
            viewed_parts=aggregate_part_stats(by_type.get("part_view", [])),
            seller_parts=aggregate_part_stats(by_type.get("seller_request", [])),
            cart_parts=aggregate_part_stats(by_type.get("cart_add", [])),
            ordered_parts=aggregate_part_stats(by_type.get("order_item", [])),
            recent_events=events[:80],
            event_labels=event_labels,
            news=news,
        )
    finally:
        db.close()


@app.route("/admin/news/clear", methods=["POST"])
@admin_required
def admin_news_clear():
    db = SessionLocal()
    try:
        db.query(NewsFeed).delete(synchronize_session=False)
        db.commit()
        flash("РЎС‚СЂС–С‡РєСѓ РЅРѕРІРёРЅ РѕС‡РёС‰РµРЅРѕ", "success")
    finally:
        db.close()
    return redirect(request.referrer or url_for("admin_parts"))


@app.route("/admin/parts")
@admin_required
def admin_parts():
    db = SessionLocal()
    try:
        warehouses = db.query(Warehouse).order_by(Warehouse.name.asc()).all()
        query_text = request.args.get("q", "").strip()
        selected_scope = normalize_text(request.args.get("warehouse_id") or "").strip()
        if not selected_scope:
            selected_scope = str(warehouses[0].id) if warehouses else "all"
        show_all_goods = selected_scope == "all"

        selected_warehouse = None
        parts = []
        all_goods = []
        warehouse_stats = {"total": 0, "with_photo_pct": 0, "with_desc_pct": 0}
        if show_all_goods:
            all_goods = build_all_goods_cards(db, query_text)
            db.commit()
        else:
            selected_warehouse_id = int(selected_scope or 0)
            selected_warehouse = db.get(Warehouse, selected_warehouse_id) if selected_warehouse_id else None
        if selected_warehouse:
            warehouse_stats = recalc_warehouse_stats(selected_warehouse)
            parts_query = db.query(Part).filter(Part.warehouse_id == selected_warehouse.id)
            parts_query = parts_query.filter(Part.is_deleted == False)
            if query_text:
                like = f"%{query_text}%"
                parts_query = parts_query.filter(
                    (Part.part_number.ilike(like))
                    | (Part.name.ilike(like))
                    | (Part.description.ilike(like))
                    | (Part.barcode.ilike(like))
                )
            parts = parts_query.all()
            parts.sort(
                key=lambda part: (
                    0 if part.updated_at and part.created_at and part.updated_at > (part.created_at + timedelta(seconds=1)) else 1,
                    -(part.updated_at.timestamp() if part.updated_at else 0),
                    part.part_number.casefold(),
                )
            )
            for part in parts:
                ensure_part_barcode(db, part)
            db.commit()

        news = db.query(NewsFeed).order_by(desc(NewsFeed.created_at)).limit(12).all()
        return render_template(
            "admin_parts.html",
            warehouses=warehouses,
            selected_warehouse=selected_warehouse,
            selected_scope=selected_scope,
            show_all_goods=show_all_goods,
            parts=parts,
            all_goods=all_goods,
            q=query_text,
            stats=warehouse_stats,
            news=news,
        )
    finally:
        db.close()


@app.route("/admin/products")
@admin_required
def admin_products():
    db = SessionLocal()
    try:
        warehouses = db.query(Warehouse).order_by(Warehouse.name.asc()).all()
        cards = []
        for w in warehouses:
            stats = recalc_warehouse_stats(w)
            cards.append((w, stats))
        news = db.query(NewsFeed).order_by(desc(NewsFeed.created_at)).limit(12).all()
        return render_template("admin_products.html", warehouse_cards=cards, news=news)
    finally:
        db.close()


def warehouse_delete_review_rows(db, warehouse: Warehouse):
    rows = []
    parts = (
        db.query(Part)
        .filter(Part.warehouse_id == warehouse.id)
        .order_by(Part.part_number.asc(), Part.id.asc())
        .all()
    )
    grouped = {}
    for part in parts:
        key = normalize_text(part.part_number or "").strip().upper()
        if not key:
            continue
        grouped.setdefault(key, []).append(part)

    for part_number, warehouse_parts in grouped.items():
        template = find_part_template(db, part_number)
        other_parts = (
            db.query(Part)
            .filter(Part.part_number == part_number, Part.warehouse_id != warehouse.id, Part.is_deleted == False)
            .order_by(Part.warehouse_id.asc(), Part.id.asc())
            .all()
        )
        other_labels = []
        seen_names = set()
        for part in other_parts:
            warehouse_name = part.warehouse.name if part.warehouse else ""
            if warehouse_name in seen_names:
                continue
            seen_names.add(warehouse_name)
            other_labels.append(f"{warehouse_name}: {int(part.qty or 0)} шт.")
        warehouse_qty = sum(max(int(part.qty or 0), 0) for part in warehouse_parts if not part.is_deleted)
        unassigned_qty = template_unassigned_qty(template)
        rows.append(
            {
                "actionKey": hashlib.md5(part_number.encode("utf-8", errors="ignore")).hexdigest()[:12],
                "partNumber": part_number,
                "title": normalize_text(
                    (template.name if template else warehouse_parts[0].name if warehouse_parts else part_number)
                ).strip()
                or part_number,
                "warehouseQty": warehouse_qty,
                "otherQty": sum(max(int(part.qty or 0), 0) for part in other_parts),
                "otherWarehouses": other_labels,
                "templateQty": unassigned_qty,
                "hasOtherBindings": bool(other_parts),
                "needsReview": bool(other_parts or unassigned_qty > 0),
                "defaultAction": "leave" if (other_parts or unassigned_qty > 0) else "delete",
                "warehousePartIds": [part.id for part in warehouse_parts],
                "templateId": template.id if template else None,
            }
        )
    return rows


def clear_part_relationships(db, part_ids):
    normalized_ids = [int(part_id) for part_id in part_ids or [] if int(part_id or 0) > 0]
    if not normalized_ids:
        return
    db.query(OrderItem).filter(OrderItem.part_id.in_(normalized_ids)).update({OrderItem.part_id: None}, synchronize_session=False)
    db.query(PackingRequestItem).filter(PackingRequestItem.part_id.in_(normalized_ids)).update({PackingRequestItem.part_id: None}, synchronize_session=False)
    db.query(TransitOrder).filter(TransitOrder.linked_part_id.in_(normalized_ids)).update({TransitOrder.linked_part_id: None}, synchronize_session=False)
    db.query(AppNotification).filter(AppNotification.part_id.in_(normalized_ids)).delete(synchronize_session=False)
    affected_request_ids = [
        row[0]
        for row in db.query(AvailabilityRequestItem.request_id)
        .filter(AvailabilityRequestItem.part_id.in_(normalized_ids))
        .distinct()
        .all()
    ]
    if affected_request_ids:
        db.query(AvailabilityRequestItem).filter(AvailabilityRequestItem.part_id.in_(normalized_ids)).delete(synchronize_session=False)
        for request_obj in db.query(AvailabilityRequest).filter(AvailabilityRequest.id.in_(affected_request_ids)).all():
            if not request_obj.items:
                db.delete(request_obj)
                continue
            recalc_availability_request(request_obj)


def clear_template_relationships(db, template_ids):
    normalized_ids = [int(template_id) for template_id in template_ids or [] if int(template_id or 0) > 0]
    if not normalized_ids:
        return
    db.query(TransitOrder).filter(TransitOrder.part_template_id.in_(normalized_ids)).update(
        {TransitOrder.part_template_id: None},
        synchronize_session=False,
    )


def delete_master_card_everywhere(db, part_number: str):
    normalized = normalize_text(part_number or "").strip().upper()
    if not normalized:
        return {"partsDeleted": 0, "templateDeleted": False}
    template = find_part_template(db, normalized)
    all_parts = db.query(Part).filter(Part.part_number == normalized).all()
    part_ids = [part.id for part in all_parts]
    clear_part_relationships(db, part_ids)
    if part_ids:
        db.query(Part).filter(Part.id.in_(part_ids)).delete(synchronize_session=False)
    template_deleted = False
    if template:
        clear_template_relationships(db, [template.id])
        db.delete(template)
        template_deleted = True
    return {"partsDeleted": len(part_ids), "templateDeleted": template_deleted}


@app.route("/admin/warehouses/delete/review", methods=["POST"])
@admin_required
def delete_warehouse_review():
    db = SessionLocal()
    try:
        warehouse_id = int(request.form.get("warehouse_id") or 0)
        delete_mode = normalize_text(request.form.get("delete_mode") or "inventory_only").strip().lower()
        confirm_text = (request.form.get("confirm_text") or "").strip().lower()
        if confirm_text not in {"так", "tak", "с‚р°рє"}:
            flash('Для видалення потрібно ввести "так"', "error")
            return redirect(url_for("admin_products"))
        if delete_mode not in {"inventory_only", "warehouse_with_goods"}:
            delete_mode = "inventory_only"
        warehouse = db.get(Warehouse, warehouse_id)
        if not warehouse:
            flash("Склад не знайдено", "error")
            return redirect(url_for("admin_products"))
        review_rows = warehouse_delete_review_rows(db, warehouse)
        return render_template(
            "admin_warehouse_delete_review.html",
            warehouse=warehouse,
            delete_mode=delete_mode,
            review_rows=review_rows,
            review_needs_actions=any(row["needsReview"] for row in review_rows),
            news=db.query(NewsFeed).order_by(desc(NewsFeed.created_at)).limit(12).all(),
        )
    except ValueError:
        flash("Оберіть склад для видалення", "error")
        return redirect(url_for("admin_products"))
    finally:
        db.close()


@app.route("/admin/warehouse/print")
@admin_required
def admin_warehouse_print():
    db = SessionLocal()
    try:
        warehouses = db.query(Warehouse).order_by(Warehouse.name.asc()).all()
        scope = normalize_text(request.args.get("scope") or "").strip().lower()
        if scope != "all":
            valid_ids = {str(item.id) for item in warehouses}
            scope = scope if scope in valid_ids else (str(warehouses[0].id) if warehouses else "all")
        query_text = request.args.get("q", "").strip()
        rows = build_warehouse_print_picker_rows(db, scope, query_text)
        scope_label = warehouse_print_scope_label(db, scope)
        news = db.query(NewsFeed).order_by(desc(NewsFeed.created_at)).limit(12).all()
        return render_template(
            "admin_warehouse_print.html",
            warehouses=warehouses,
            selected_scope=scope,
            scope_label=scope_label,
            rows=rows,
            q=query_text,
            news=news,
        )
    finally:
        db.close()


@app.route("/admin/transit")
@admin_required
def admin_transit():
    db = SessionLocal()
    try:
        if backfill_transit_batch_ids(db):
            db.commit()
        active_orders = (
            db.query(TransitOrder)
            .filter(TransitOrder.archived_at.is_(None))
            .order_by(desc(TransitOrder.created_at), desc(TransitOrder.id))
            .all()
        )
        archive_orders = (
            db.query(TransitOrder)
            .filter(TransitOrder.archived_at.is_not(None))
            .order_by(desc(TransitOrder.created_at), desc(TransitOrder.id))
            .all()
        )
        active_payload = [serialize_transit_order(db, order) for order in active_orders]
        archive_payload = [serialize_transit_order(db, order) for order in archive_orders]
        active_groups = group_transit_batches(active_payload)
        archive_groups = group_transit_batches(archive_payload)
        transit_form = build_transit_form_state()
        transit_draft_items = get_transit_draft_items()
        news = db.query(NewsFeed).order_by(desc(NewsFeed.created_at)).limit(12).all()
        return render_template(
            "admin_transit.html",
            transit_orders=active_payload,
            transit_archive=archive_payload,
            transit_groups=active_groups,
            transit_archive_groups=archive_groups,
            transit_active_count=sum(len(group["items"]) for group in active_groups),
            transit_archive_count=sum(len(group["items"]) for group in archive_groups),
            transit_form=transit_form,
            transit_draft_items=transit_draft_items,
            news=news,
        )
    finally:
        db.close()


@app.route("/admin/transit/draft-item", methods=["POST"])
@admin_required
def admin_transit_draft_add():
    db = SessionLocal()
    short_description = normalize_text(request.form.get("short_description") or "").strip()
    full_description = normalize_text(request.form.get("full_description") or "").strip()
    try:
        item = prepare_transit_item_input(
            db,
            part_number_value=request.form.get("part_number"),
            title_value=request.form.get("title"),
            service_info_value=request.form.get("service_info"),
            price_value=request.form.get("price_usd"),
            qty_value=request.form.get("qty"),
            existing_photo_url=request.form.get("existing_photo_url"),
            photo_file=request.files.get("photo"),
            upload_suffix=request.form.get("part_number") or "draft",
        )
        draft_items = get_transit_draft_items()
        draft_items.append(
            {
                "draftId": f"{int(time.time() * 1000)}-{random.randint(100, 999)}",
                **item,
            }
        )
        save_transit_draft(draft_items, short_description, full_description)
        db.commit()
        flash(f'Позицію {item["partNumber"]} додано до списку', "success")
        return redirect(url_for("admin_transit"))
    except ValueError:
        db.rollback()
        save_transit_draft(get_transit_draft_items(), short_description, full_description)
        flash("Вкажіть OEM номер, опис товару і кількість більше нуля", "error")
        return redirect(url_for("admin_transit"))
    finally:
        db.close()


@app.route("/admin/transit/draft-item/<draft_id>/delete", methods=["POST"])
@admin_required
def admin_transit_draft_delete(draft_id):
    draft_items = [item for item in get_transit_draft_items() if item.get("draftId") != draft_id]
    meta = get_transit_draft_meta()
    if draft_items:
        save_transit_draft(draft_items, meta["shortDescription"], meta["fullDescription"])
    else:
        clear_transit_draft()
    flash("Позицію прибрано зі списку", "success")
    return redirect(url_for("admin_transit"))


@app.route("/admin/transit/create", methods=["POST"])
@admin_required
def admin_transit_create():
    db = SessionLocal()
    try:
        meta = get_transit_draft_meta()
        short_description = normalize_text(request.form.get("short_description") or meta["shortDescription"]).strip()
        full_description = normalize_text(request.form.get("full_description") or meta["fullDescription"]).strip()
        draft_items = get_transit_draft_items()
        items_to_create = list(draft_items)

        current_part_number = normalize_text(request.form.get("part_number") or "").strip().upper()
        current_title = normalize_text(request.form.get("title") or "").strip()
        current_service_info = normalize_text(request.form.get("service_info") or "").strip()
        current_qty_raw = (request.form.get("qty") or "").strip()
        current_price_raw = (request.form.get("price_usd") or "").strip()
        has_current_photo = bool(getattr(request.files.get("photo"), "filename", "").strip())
        has_current_payload = any(
            [
                current_part_number,
                current_title,
                current_service_info,
                current_qty_raw not in {"", "1"},
                current_price_raw not in {"", "0", "0.0", "0.00"},
                has_current_photo,
            ]
        )

        if has_current_payload:
            items_to_create.append(
                {
                    "draftId": "",
                    **prepare_transit_item_input(
                        db,
                        part_number_value=request.form.get("part_number"),
                        title_value=request.form.get("title"),
                        service_info_value=request.form.get("service_info"),
                        price_value=request.form.get("price_usd"),
                        qty_value=request.form.get("qty"),
                        existing_photo_url=request.form.get("existing_photo_url"),
                        photo_file=request.files.get("photo"),
                        upload_suffix=request.form.get("part_number") or "single",
                    ),
                }
            )

        if not items_to_create:
            save_transit_draft(draft_items, short_description, full_description)
            flash("Додайте хоча б одну позицію до списку товарів у дорозі", "error")
            return redirect(url_for("admin_transit"))

        created_numbers = []
        batch_id = uuid4().hex
        for item in items_to_create:
            order_short_description = short_description or " / ".join(
                part for part in [item.get("partNumber") or "", item.get("title") or ""] if part
            )
            order = persist_transit_order(db, item, order_short_description, full_description, batch_id)
            created_numbers.append(order.part_number)
            flash_news(
                db,
                "transit",
                "Додано товар в дорозі",
                f"Замовлення {order.part_number} додано у вкладку товарів в дорозі.",
                "success",
            )
        db.commit()
        clear_transit_draft()
        flash(
            f"Збережено {len(created_numbers)} позицій у вкладку товарів в дорозі",
            "success",
        )
        return redirect(url_for("admin_transit"))
    except ValueError:
        db.rollback()
        save_transit_draft(draft_items, short_description, full_description)
        flash("Перевірте кількість і ціну товару", "error")
        return redirect(url_for("admin_transit"))
    finally:
        db.close()


@app.route("/admin/transit/<int:order_id>/update", methods=["POST"])
@admin_required
def admin_transit_update(order_id):
    db = SessionLocal()
    try:
        order = db.get(TransitOrder, order_id)
        if not order:
            flash("Замовлення в дорозі не знайдено", "error")
            return redirect(url_for("admin_transit"))

        part_number = normalize_text(request.form.get("part_number") or order.part_number).strip().upper()
        title = normalize_text(request.form.get("title") or "").strip()
        service_info = normalize_text(request.form.get("service_info") or "").strip()
        short_description = normalize_text(request.form.get("short_description") or "").strip()
        full_description = normalize_text(request.form.get("full_description") or "").strip()
        price_usd = float(request.form.get("price_usd") or 0)
        qty = max(int(request.form.get("qty") or 0), 0)
        existing_photo_url = normalize_text(request.form.get("existing_photo_url") or order.photo_urls or "").strip()
        uploaded_photo_url = save_upload(request.files.get("photo"), f"transit_{order_id}")
        source_part, template = resolve_transit_source(db, part_number)

        if source_part:
            if not title:
                title = normalize_text(source_part.name or "").strip()
            if not service_info:
                service_info = normalize_text(source_part.description or "").strip()
            if not price_usd:
                price_usd = float(source_part.price_usd or 0)
        elif template:
            if not title:
                title = normalize_text(template.name or "").strip()
            if not service_info:
                service_info = normalize_text(template.description or "").strip()
            if not price_usd:
                price_usd = float(template.price_usd or 0)

        photo_url = uploaded_photo_url or existing_photo_url
        if not photo_url and source_part:
            photo_url = primary_part_photo(source_part)
        if not photo_url and template:
            photo_url = primary_template_photo(template)

        if not short_description:
            short_description = " / ".join(part for part in [part_number, title] if part)
        if not part_number or not title or qty <= 0:
            flash("Вкажіть OEM номер, опис товару і кількість більше нуля", "error")
            return redirect(url_for("admin_transit"))

        order.part_number = part_number
        order.title = title
        order.service_info = service_info
        order.short_description = short_description
        order.full_description = full_description
        order.qty = qty
        order.price_usd = price_usd
        order.photo_urls = photo_url or ""
        order.has_photo = bool(photo_url)
        order.status = "in_transit"
        order.updated_at = now()

        if source_part:
            order.linked_part_id = source_part.id
            order.part_template_id = None
            order.barcode = source_part.barcode or ""
        else:
            template, created = upsert_part_template(
                db,
                part_number,
                {
                    "name": title,
                    "description": service_info,
                    "price_usd": price_usd,
                    "photo_urls": photo_url,
                    "showcase_photo_urls": [photo_url] if photo_url else [],
                },
            )
            order.linked_part_id = None
            order.part_template_id = template.id if template else None
            order.barcode = template.barcode if template else ""
            flash_news(
                db,
                "transit",
                "Оновлено шаблон товару",
                (
                    f"Створено шаблон {part_number}, який ще не додано до складу."
                    if created
                    else f"Шаблон {part_number} оновлено через вкладку товару в дорозі."
                ),
                "info",
            )

        flash_news(db, "transit", "Оновлено товар в дорозі", f"Замовлення {part_number} у вкладці товарів в дорозі оновлено.", "info")
        db.commit()
        flash("Замовлення в дорозі оновлено", "success")
        return redirect(url_for("admin_transit"))
    except ValueError:
        db.rollback()
        flash("Перевірте кількість і ціну товару", "error")
        return redirect(url_for("admin_transit"))
    finally:
        db.close()


@app.route("/admin/transit/<int:order_id>/delete", methods=["POST"])
@admin_required
def admin_transit_delete(order_id):
    db = SessionLocal()
    try:
        order = db.get(TransitOrder, order_id)
        if not order:
            flash("Замовлення в дорозі не знайдено", "error")
            return redirect(url_for("admin_transit"))
        confirm_text = (request.form.get("confirm_text") or "").strip().lower()
        if confirm_text not in {"так", "tak", "с‚р°рє"}:
            flash('Для видалення потрібно ввести "так"', "error")
            return redirect(url_for("admin_transit"))
        part_number = order.part_number or f"#{order.id}"
        db.delete(order)
        flash_news(db, "transit", "Видалено товар в дорозі", f"Замовлення {part_number} видалено з вкладки товарів в дорозі.", "info")
        db.commit()
        flash("Замовлення в дорозі видалено", "success")
        return redirect(url_for("admin_transit"))
    finally:
        db.close()


@app.route("/admin/transit/<int:order_id>/arrive", methods=["POST"])
@admin_required
def admin_transit_arrive(order_id):
    db = SessionLocal()
    try:
        order = db.get(TransitOrder, order_id)
        if not order:
            flash("Замовлення в дорозі не знайдено", "error")
            return redirect(url_for("admin_transit"))

        normalize_transit_order_progress(db, order)
        accepted_qty = max(int(order.accepted_qty or 0), 0)
        if accepted_qty <= 0:
            flash("Кнопка 'Товар прибув' стане активною після першого сканування в додатку", "error")
            db.rollback()
            return redirect(url_for("admin_transit"))

        should_print = (request.form.get("print_labels") or "").strip().lower() in {"1", "true", "yes", "так"}
        qty_override = parse_label_qty_override(request.form.get("label_qty"))
        if qty_override == -1:
            flash("Вкажіть коректну кількість етикеток", "error")
            return redirect(url_for("admin_transit"))
        qty = qty_override if qty_override is not None else max(int(order.qty or 0), 0)
        target_part, added_qty = apply_transit_arrival_to_part(db, order)
        order.archived_at = now()
        order.updated_at = now()
        arrival_note = ""
        if target_part and added_qty > 0:
            arrival_note = f" До позиції {target_part.part_number} додано {added_qty} шт."

        if should_print and qty > 0:
            barcode = ensure_transit_order_barcode(db, order)
            order.labels_printed_at = now()
            flash_news(
                db,
                "transit",
                "Товар прибув",
                f"Запис {order.part_number} переміщено в архів {format_dt(order.archived_at)} і штрихкоди підготовлено до друку.{arrival_note}",
                "success",
            )
            db.commit()
            labels = [
                build_print_label(
                    headline=order.part_number,
                    title=order.title,
                    description=order.service_info,
                    barcode=barcode,
                    context="Товар в дорозі",
                )
                for _ in range(qty)
            ]
            return render_template("print_labels.html", title=f"Етикетки в дорозі {order.part_number}", labels=labels)

        flash_news(
            db,
            "transit",
            "Товар прибув",
            f"Запис {order.part_number} переміщено в архів {format_dt(order.archived_at)}.{arrival_note}",
            "success",
        )
        db.commit()
        flash(
            f'Товар переміщено в архів{" і кількість оновлено" if added_qty > 0 else ""}',
            "success",
        )
        return redirect(url_for("admin_transit"))
    finally:
        db.close()


@app.route("/admin/barcodes/transit/batch/<batch_id>", methods=["POST"])
@admin_required
def print_transit_batch_barcodes(batch_id):
    db = SessionLocal()
    try:
        batch_key = normalize_text(batch_id or "").strip()
        qty_override = parse_label_qty_override(request.form.get("label_qty"))
        if qty_override == -1:
            flash("Вкажіть коректну кількість етикеток", "error")
            return redirect(url_for("admin_transit"))
        orders = (
            db.query(TransitOrder)
            .filter(TransitOrder.batch_id == batch_key)
            .order_by(TransitOrder.created_at.asc(), TransitOrder.id.asc())
            .all()
        )
        if not orders:
            flash("Замовлення в дорозі не знайдено", "error")
            return redirect(url_for("admin_transit"))

        labels = []
        printed_at = now()
        for order in orders:
            qty = qty_override if qty_override is not None else max(int(order.qty or 0), 0)
            if qty <= 0:
                continue
            barcode = ensure_transit_order_barcode(db, order)
            order.labels_printed_at = printed_at
            order.updated_at = printed_at
            labels.extend(
                [
                    build_print_label(
                        headline=order.part_number,
                        title=order.title,
                        description=order.service_info,
                        barcode=barcode,
                        context="Товар в дорозі",
                    )
                    for _ in range(qty)
                ]
            )

        if not labels:
            flash("У цьому замовленні немає позицій для друку штрихкодів", "error")
            return redirect(url_for("admin_transit"))

        flash_news(
            db,
            "transit",
            "Надруковано штрихкоди",
            f"Для замовлення в дорозі надруковано штрихкоди на {len(labels)} етикеток.",
            "info",
        )
        db.commit()
        title_source = normalize_text(orders[0].short_description or "").strip() or f"Партія {orders[0].part_number}"
        return render_template("print_labels.html", title=f"Етикетки в дорозі {title_source}", labels=labels)
    finally:
        db.close()


@app.route("/admin/barcodes/transit/<int:order_id>", methods=["POST"])
@admin_required
def print_transit_barcodes(order_id):
    db = SessionLocal()
    try:
        order = db.get(TransitOrder, order_id)
        if not order:
            flash("Замовлення в дорозі не знайдено", "error")
            return redirect(url_for("admin_transit"))
        qty = max(int(request.form.get("label_qty", order.qty or 0) or 0), 0)
        if qty <= 0:
            flash("Вкажіть кількість етикеток більше нуля", "error")
            return redirect(url_for("admin_transit"))

        barcode = ensure_transit_order_barcode(db, order)
        order.labels_printed_at = now()
        order.updated_at = now()
        db.commit()

        labels = [
            build_print_label(
                headline=order.part_number,
                title=order.title,
                description=order.service_info,
                barcode=barcode,
                context="Товар в дорозі",
            )
            for _ in range(qty)
        ]
        return render_template("print_labels.html", title=f"Етикетки в дорозі {order.part_number}", labels=labels)
    finally:
        db.close()


@app.route("/admin/warehouses/create", methods=["POST"])
@admin_required
def create_warehouse():
    db = SessionLocal()
    try:
        name = request.form.get("name", "").strip()
        markup = float(request.form.get("markup_percent", "0") or 0)
        if not name:
            flash("Р’РєР°Р¶С–С‚СЊ РЅР°Р·РІСѓ СЃРєР»Р°РґСѓ", "error")
            return redirect(url_for("admin_products"))
        db.add(Warehouse(name=name, markup_percent=markup, created_at=now(), updated_at=now()))
        flash_news(db, "warehouse", "РЎС‚РІРѕСЂРµРЅРѕ СЃРєР»Р°Рґ", f"РЎС‚РІРѕСЂРµРЅРѕ СЃРєР»Р°Рґ {name}.", "info")
        db.commit()
        flash("РЎРєР»Р°Рґ СЃС‚РІРѕСЂРµРЅРѕ", "success")
    except Exception as e:
        db.rollback()
        flash(f"РќРµ РІРґР°Р»РѕСЃСЏ СЃС‚РІРѕСЂРёС‚Рё СЃРєР»Р°Рґ: {e}", "error")
    finally:
        db.close()
    return redirect(url_for("admin_products"))


@app.route("/admin/warehouses/delete", methods=["POST"])
@admin_required
def delete_warehouse():
    db = SessionLocal()
    try:
        warehouse_id = int(request.form.get("warehouse_id") or 0)
        delete_mode = normalize_text(request.form.get("delete_mode") or "inventory_only").strip().lower()
        if delete_mode not in {"inventory_only", "warehouse_with_goods"}:
            delete_mode = "inventory_only"
        warehouse = db.get(Warehouse, warehouse_id)
        if not warehouse:
            flash("Склад не знайдено", "error")
            return redirect(url_for("admin_products"))
        review_rows = warehouse_delete_review_rows(db, warehouse)
        part_ids = [part_id for row in review_rows for part_id in row["warehousePartIds"]]
        import_session_ids = [row[0] for row in db.query(ImportSession.id).filter(ImportSession.warehouse_id == warehouse.id).all()]
        availability_request_ids = [row[0] for row in db.query(AvailabilityRequest.id).filter(AvailabilityRequest.warehouse_id == warehouse.id).all()]
        deleted_positions = 0
        deleted_master_cards = 0
        if delete_mode == "warehouse_with_goods":
            for row in review_rows:
                action = (request.form.get(f"action_{row['actionKey']}") or row["defaultAction"]).strip().lower()
                if action not in {"delete", "adjust", "leave"}:
                    action = row["defaultAction"]
                if action == "delete":
                    result = delete_master_card_everywhere(db, row["partNumber"])
                    deleted_positions += int(result["partsDeleted"] or 0)
                    deleted_master_cards += 1 if result["templateDeleted"] else 0
                    continue
                clear_part_relationships(db, row["warehousePartIds"])
                if row["warehousePartIds"]:
                    deleted_positions += db.query(Part).filter(Part.id.in_(row["warehousePartIds"])).delete(synchronize_session=False)
                template = find_part_template(db, row["partNumber"])
                if template and not row["hasOtherBindings"] and row["templateQty"] <= 0:
                    clear_template_relationships(db, [template.id])
                    db.delete(template)
                    deleted_master_cards += 1
                    continue
                if template and action == "adjust":
                    adjusted_qty = max(int(request.form.get(f"adjust_qty_{row['actionKey']}") or 0), 0)
                    template.unassigned_qty = adjusted_qty
                    template.updated_at = now()
        else:
            clear_part_relationships(db, part_ids)
            if part_ids:
                deleted_positions += db.query(Part).filter(Part.id.in_(part_ids)).delete(synchronize_session=False)
        if availability_request_ids:
            db.query(AvailabilityRequestItem).filter(AvailabilityRequestItem.request_id.in_(availability_request_ids)).delete(synchronize_session=False)
            db.query(AvailabilityRequest).filter(AvailabilityRequest.id.in_(availability_request_ids)).delete(synchronize_session=False)
        db.query(AppNotification).filter(AppNotification.warehouse_id == warehouse.id).delete(synchronize_session=False)
        db.query(ReceivingDraftItem).filter(ReceivingDraftItem.warehouse_id == warehouse.id).delete(synchronize_session=False)
        if import_session_ids:
            db.query(ImportChange).filter(ImportChange.import_session_id.in_(import_session_ids)).delete(synchronize_session=False)
        db.query(ImportSession).filter(ImportSession.warehouse_id == warehouse.id).delete(synchronize_session=False)
        warehouse_name = warehouse.name
        db.delete(warehouse)
        flash_news(
            db,
            "warehouse",
            "Видалено склад",
            (
                f"Склад {warehouse_name} видалено. Прибрано ярликів: {deleted_positions}. "
                f"Master-карток видалено: {deleted_master_cards}."
            ),
            "info",
        )
        db.commit()
        if delete_mode == "warehouse_with_goods":
            flash(f'Склад "{warehouse_name}" видалено. Ярлики прибрано, master-картки оброблено за вибраним сценарієм.', "success")
        else:
            flash(f'Склад "{warehouse_name}" видалено. Прибрано тільки наявність товару в межах цього складу.', "success")
    except ValueError:
        flash("Оберіть склад для видалення", "error")
    except Exception as e:
        db.rollback()
        flash(f"Не вдалося видалити склад: {e}", "error")
    finally:
        db.close()
    return redirect(url_for("admin_products"))



@app.route("/admin/warehouse/<int:warehouse_id>/update", methods=["POST"])
@admin_required
def update_warehouse(warehouse_id):
    db = SessionLocal()
    try:
        warehouse = db.get(Warehouse, warehouse_id)
        if not warehouse:
            flash("РЎРєР»Р°Рґ РЅРµ Р·РЅР°Р№РґРµРЅРѕ", "error")
            return redirect(url_for("admin_products"))
        warehouse.name = request.form.get("name", warehouse.name).strip() or warehouse.name
        warehouse.markup_percent = float(request.form.get("markup_percent", warehouse.markup_percent) or 0)
        warehouse.updated_at = now()
        flash_news(db, "warehouse", "РћРЅРѕРІР»РµРЅРѕ СЃРєР»Р°Рґ", f"РЎРєР»Р°Рґ {warehouse.name} РѕРЅРѕРІР»РµРЅРѕ.", "info")
        db.commit()
        flash("РЎРєР»Р°Рґ РѕРЅРѕРІР»РµРЅРѕ", "success")
        next_url = request.form.get("next") or ""
        return redirect(next_url or url_for("admin_parts", warehouse_id=warehouse.id))
    except Exception as e:
        db.rollback()
        flash(f"РџРѕРјРёР»РєР° РѕРЅРѕРІР»РµРЅРЅСЏ СЃРєР»Р°РґСѓ: {e}", "error")
        return redirect(url_for("admin_products"))
    finally:
        db.close()

@app.route("/admin/revision/<int:warehouse_id>/reset", methods=["POST"])
@admin_required
def revision_reset(warehouse_id):
    db = SessionLocal()
    try:
        warehouse = db.get(Warehouse, warehouse_id)
        if not warehouse:
            flash("РЎРєР»Р°Рґ РЅРµ Р·РЅР°Р№РґРµРЅРѕ", "error")
            return redirect(url_for("admin_products"))
        warehouse.revision_current_index = 0
        warehouse.revision_percent = 0
        warehouse.revision_status = "not_started"
        warehouse.revision_started_at = now()
        warehouse.revision_date = None
        warehouse.updated_at = now()
        db.commit()
        flash("Р РµРІС–Р·С–СЋ СЃРєРёРЅСѓС‚Рѕ. РџРѕС‡РёРЅР°С”РјРѕ СЃРїРѕС‡Р°С‚РєСѓ.", "success")
    finally:
        db.close()
    return redirect(url_for("revision", warehouse_id=warehouse_id, idx=0))

@app.route("/admin/warehouse/<int:warehouse_id>")
@admin_required
def warehouse_detail(warehouse_id):
    return redirect(
        url_for(
            "admin_parts",
            warehouse_id=warehouse_id,
            q=request.args.get("q", "").strip(),
        )
    )


@app.route("/admin/api/parts/prefill")
@admin_required
def admin_part_prefill():
    db = SessionLocal()
    try:
        warehouse_id = request.args.get("warehouse_id", type=int)
        part_number = request.args.get("part_number", "").strip().upper()
        if not part_number:
            return jsonify({"found": False})

        part = find_part_prefill(db, part_number, warehouse_id)
        if part:
            lookup = build_mobile_lookup_payload(db, part.part_number)
            warehouse_name = part.warehouse.name if part.warehouse else ""
            same_warehouse = bool(warehouse_id and part.warehouse_id == warehouse_id)
            return jsonify({
                "found": True,
                "sameWarehouse": same_warehouse,
                "part": {
                    "id": part.id,
                    "partNumber": part.part_number or "",
                    "brand": part.brand or "",
                    "producerType": part.producer_type or "OEM",
                    "name": part.name or "",
                    "description": part.description or "",
                    "priceUsd": float(part.price_usd or 0),
                    "qty": int(part.qty or 0),
                    "barcode": part.barcode or "",
                    "warehouseName": warehouse_name,
                    "photoUrl": primary_part_photo(part),
                    "hasPhoto": bool(part.has_photo and primary_part_photo(part)),
                    "isDeleted": bool(part.is_deleted),
                    "isTemplate": False,
                    "note": deleted_note(part),
                    "stocks": lookup.get("stocks", []),
                },
            })

        template = find_part_template(db, part_number)
        cross_match = False
        if not template:
            template = find_part_template_by_cross(db, part_number)
            cross_match = bool(template)
        if not template:
            return jsonify({"found": False, "partNumber": part_number})

        ensure_template_barcode(db, template)
        lookup = build_mobile_lookup_payload(db, template.part_number)
        db.commit()
        return jsonify({
            "found": True,
            "sameWarehouse": False,
            "crossMatch": cross_match,
            "requestedPartNumber": part_number,
            "part": {
                "id": None,
                "partNumber": template.part_number or "",
                "crossNumber": part_number if cross_match else "",
                "brand": template.brand or "",
                "producerType": template.producer_type or "OEM",
                "name": template.name or "",
                "description": template.description or "",
                "priceUsd": float(template.price_usd or 0),
                "qty": 0,
                "barcode": template.barcode or "",
                "warehouseName": "",
                "photoUrl": primary_template_photo(template),
                "hasPhoto": bool(template.has_photo and primary_template_photo(template)),
                "isDeleted": False,
                "isTemplate": True,
                "note": template_note(template),
                "stocks": lookup.get("stocks", []),
            },
        })
    finally:
        db.close()


@app.route("/admin/api/parts/search")
@admin_required
def admin_parts_search():
    db = SessionLocal()
    try:
        q = request.args.get("q", "").strip()
        warehouse_id = request.args.get("warehouse_id", type=int)
        parts = search_parts_for_picker(db, q, warehouse_id=warehouse_id)
        payload = [serialize_part_picker_card(db, part) for part in parts]
        db.commit()
        return jsonify({"items": payload})
    finally:
        db.close()


@app.route("/admin/api/parts/autosave", methods=["POST"])
@admin_required
def admin_part_autosave():
    db = SessionLocal()
    try:
        part_number = normalize_text(request.form.get("part_number") or "").strip().upper()
        if not part_number:
            return jsonify({"ok": False, "error": "part_number_required"}), 400

        def to_float(value, default=0.0):
            try:
                return float(str(value or default).replace(",", ".").strip() or default)
            except Exception:
                return default

        def to_int(value, default=0):
            try:
                return max(int(float(str(value or default).replace(",", ".").strip() or default)), 0)
            except Exception:
                return default

        warehouse_value = normalize_text(request.form.get("warehouse_id") or "all").strip().lower()
        raw_name = request.form.get("name")
        raw_description = request.form.get("description")
        raw_brand = request.form.get("brand")
        raw_type = request.form.get("producer_type")
        raw_price = request.form.get("price_usd")
        raw_youtube = request.form.get("youtube_url")
        raw_cross = request.form.get("cross_numbers") if "cross_numbers" in request.form else None
        name = normalize_text(raw_name or "").strip()
        description = normalize_text(raw_description or "").strip()
        brand = normalize_text(raw_brand or "").strip()
        producer_type = producer_type_label(raw_type or "OEM")
        price_usd = to_float(raw_price, 0)
        qty = to_int(request.form.get("qty"), 0)
        youtube_url = normalize_text(raw_youtube or "").strip()
        existing_template = find_part_template(db, part_number)
        cross_numbers = (
            cross_numbers_from_form(request.form, "cross_numbers", part_number)
            if raw_cross is not None
            else template_cross_numbers(existing_template)
        )

        before_template_qty = template_unassigned_qty(existing_template)
        exemplar = find_part_prefill(db, part_number, None)
        template_payload = {
            "brand": brand if raw_brand is not None else (existing_template.brand if existing_template else exemplar.brand if exemplar else ""),
            "producer_type": producer_type if raw_type is not None else (existing_template.producer_type if existing_template else exemplar.producer_type if exemplar else "OEM"),
            "name": name if raw_name is not None else (existing_template.name if existing_template else exemplar.name if exemplar else part_number),
            "description": description if raw_description is not None else (existing_template.description if existing_template else exemplar.description if exemplar else ""),
            "price_usd": price_usd if raw_price not in (None, "") else (existing_template.price_usd if existing_template else exemplar.price_usd if exemplar else 0),
            "unassigned_qty": qty if warehouse_value in {"", "all"} else template_unassigned_qty(existing_template),
            "photo_urls": existing_template.photo_urls if existing_template else (primary_part_photo(exemplar) if exemplar else ""),
            "showcase_photo_urls": template_gallery_urls(existing_template) if existing_template else (part_gallery_urls(exemplar) if exemplar else []),
            "youtube_url": youtube_url if raw_youtube is not None else (existing_template.youtube_url if existing_template else exemplar.youtube_url if exemplar else ""),
            "cross_numbers": cross_numbers,
        }
        template, _ = upsert_part_template(db, part_number, template_payload)
        ensure_template_barcode(db, template)
        template_qty_changed = False

        active_parts = (
            db.query(Part)
            .filter(Part.part_number == part_number, Part.is_deleted == False)
            .order_by(Part.id.asc())
            .all()
        )

        if warehouse_value in {"", "all"}:
            template.unassigned_qty = qty
            template.updated_at = now()
            template_qty_changed = True
        else:
            warehouse_id = int(warehouse_value)
            warehouse = db.get(Warehouse, warehouse_id)
            if not warehouse:
                return jsonify({"ok": False, "error": "warehouse_not_found"}), 404

            selected_part = (
                db.query(Part)
                .filter(Part.warehouse_id == warehouse_id, Part.part_number == part_number)
                .order_by(Part.id.asc())
                .first()
            )
            before_selected_qty = 0 if not selected_part or selected_part.is_deleted else int(selected_part.qty or 0)
            if not selected_part:
                selected_part = Part(
                    warehouse_id=warehouse_id,
                    part_number=part_number,
                    created_at=now(),
                    updated_at=now(),
            )
                db.add(selected_part)
                db.flush()

            rebalance_template_assignment_qty(template, before_selected_qty, qty)
            after_template_qty = template_unassigned_qty(template)
            template_qty_changed = after_template_qty != before_template_qty
            selected_part.qty = qty
            selected_part.in_stock = qty > 0
            selected_part.is_deleted = qty <= 0
            selected_part.deleted_at = now() if qty <= 0 else None
            selected_part.updated_at = now()
            apply_template_to_parts(db, template, only_parts=[selected_part])
            selected_part.brand_export = normalize_text(template.brand or "").strip()
            selected_part.part_number_export = part_number
            ensure_part_barcode(db, selected_part)
            part_delta = int(qty or 0) - int(before_selected_qty or 0)
            template_delta = int(after_template_qty or 0) - int(before_template_qty or 0)
            assigned_qty = min(max(part_delta, 0), max(-template_delta, 0))
            returned_qty = min(max(-part_delta, 0), max(template_delta, 0))
            if assigned_qty:
                queue_inventory_assignment_change(
                    db,
                    template=template,
                    warehouse_name=warehouse.name,
                    qty=assigned_qty,
                    to_warehouse=True,
                    warehouse_qty=int(qty or 0),
                    unassigned_qty=after_template_qty,
                    context_label=f"Картка товару → {warehouse.name}",
                    reason="Присвоєно склад через автозапис",
                )
            if returned_qty:
                queue_inventory_assignment_change(
                    db,
                    template=template,
                    warehouse_name=warehouse.name,
                    qty=returned_qty,
                    to_warehouse=False,
                    warehouse_qty=int(qty or 0),
                    unassigned_qty=after_template_qty,
                    context_label=f"Картка товару → Всі товари",
                    reason="Повернуто зі складу через автозапис",
                )
            actual_part_increase = max(part_delta - assigned_qty, 0)
            if actual_part_increase:
                queue_part_inventory_change(
                    db,
                    selected_part,
                    int(qty or 0) - actual_part_increase,
                    context_label=f"Картка товару → {warehouse.name}",
                    reason="Додано понад залишок Всі товари через автозапис",
                )
        if warehouse_value in {"", "all"}:
            queue_template_inventory_change(
                db,
                template,
                before_template_qty,
                context_label="Картка товару → Всі товари",
                reason="Збережено зміни через автозапис",
            )

        apply_template_to_parts(db, template)
        db.commit()

        cards = build_all_goods_cards(db, part_number)
        card = next((item for item in cards if item["partNumber"] == part_number), None)
        if card and warehouse_value not in {"", "all"}:
            selected_assignment = next(
                (
                    item
                    for item in card.get("assignedWarehouses", [])
                    if str(item.get("warehouseId")) == warehouse_value
                ),
                None,
            )
            if selected_assignment:
                card["primaryWarehouseId"] = warehouse_value
                card["primaryQty"] = int(selected_assignment.get("qty") or 0)
                card["primaryPartId"] = selected_assignment.get("partId")
        return jsonify(
            {
                "ok": True,
                "card": card or {},
                "message": "Картку товару збережено",
            }
        )
    finally:
        db.close()


@app.route("/admin/part/create", methods=["POST"])
@admin_required
def create_part():
    db = SessionLocal()
    try:
        warehouse_value = normalize_text(request.form.get("warehouse_id") or "").strip().lower()
        warehouse = None
        warehouse_id = None
        if warehouse_value and warehouse_value != "all":
            warehouse_id = int(warehouse_value)
            warehouse = db.get(Warehouse, warehouse_id)
            if not warehouse:
                flash("Склад не знайдено", "error")
                return redirect(url_for("admin_parts"))
        description = request.form.get("description", "").strip()
        part_number = request.form.get("part_number", "").strip().upper()
        name = request.form.get("name", "").strip()
        qty = int(request.form.get("qty", 0) or 0)
        price_usd = float(request.form.get("price_usd", 0) or 0)
        cross_numbers_submitted = bool(request.form.getlist("cross_numbers")) or ("cross_numbers" in request.form)
        cross_numbers = cross_numbers_from_form(request.form, "cross_numbers", part_number)
        template = find_part_template(db, part_number)
        cross_owner = find_part_template_by_cross(db, part_number)
        cross_action = normalize_text(request.form.get("cross_conflict_action") or "").strip()
        if cross_owner and (not template or cross_owner.id != template.id):
            if cross_action == "add_to_existing":
                ensure_template_barcode(db, cross_owner)
                before_template_qty = template_unassigned_qty(cross_owner)
                if warehouse_id is None:
                    cross_owner.unassigned_qty = before_template_qty + qty
                    cross_owner.updated_at = now()
                    queue_template_inventory_change(
                        db,
                        cross_owner,
                        before_template_qty,
                        context_label="Крос-номер → Всі товари",
                        reason=f"Кількість додано через крос {part_number}",
                    )
                else:
                    part = (
                        db.query(Part)
                        .filter(Part.warehouse_id == warehouse_id, Part.part_number == cross_owner.part_number)
                        .order_by(Part.id.asc())
                        .first()
                    )
                    before_part_qty = 0 if not part or part.is_deleted else int(part.qty or 0)
                    if not part:
                        part = Part(
                            warehouse_id=warehouse_id,
                            part_number=cross_owner.part_number,
                            created_at=now(),
                            updated_at=now(),
                        )
                        db.add(part)
                        db.flush()
                    new_qty = before_part_qty + qty
                    rebalance_template_assignment_qty(cross_owner, before_part_qty, new_qty)
                    after_template_qty = template_unassigned_qty(cross_owner)
                    part.qty = new_qty
                    part.in_stock = new_qty > 0
                    part.is_deleted = new_qty <= 0
                    part.deleted_at = now() if new_qty <= 0 else None
                    part.updated_at = now()
                    apply_template_to_parts(db, cross_owner, only_parts=[part])
                    part.brand_export = normalize_text(cross_owner.brand or "").strip()
                    part.part_number_export = cross_owner.part_number
                    ensure_part_barcode(db, part)
                    part_delta = int(new_qty or 0) - int(before_part_qty or 0)
                    template_delta = int(after_template_qty or 0) - int(before_template_qty or 0)
                    assigned_qty = min(max(part_delta, 0), max(-template_delta, 0))
                    if assigned_qty:
                        queue_inventory_assignment_change(
                            db,
                            template=cross_owner,
                            warehouse_name=warehouse.name if warehouse else "Склад",
                            qty=assigned_qty,
                            to_warehouse=True,
                            warehouse_qty=int(new_qty or 0),
                            unassigned_qty=after_template_qty,
                            context_label=f"Крос-номер → {warehouse.name if warehouse else 'Склад'}",
                            reason=f"Присвоєно склад через крос {part_number}",
                        )
                    actual_part_increase = max(part_delta - assigned_qty, 0)
                    if actual_part_increase:
                        queue_part_inventory_change(
                            db,
                            part,
                            int(new_qty or 0) - actual_part_increase,
                            context_label=f"Крос-номер → {warehouse.name if warehouse else 'Склад'}",
                            reason=f"Додано понад залишок Всі товари через крос {part_number}",
                        )
                flash_news(db, "parts", "Кількість додано через крос-номер", f"{part_number} → {cross_owner.part_number}: +{qty} шт.", "success")
                db.commit()
                flash(f"Кількість додано до основного товару {cross_owner.part_number}", "success")
                return redirect(url_for("admin_parts", warehouse_id=warehouse_id or "all"))
            if cross_action == "create_new_remove_cross":
                remove_cross_number_from_template(cross_owner, part_number)
                db.flush()
            else:
                flash(
                    f"OEM {part_number} вже є крос-номером товару {cross_owner.part_number}. "
                    'Оберіть: "добавити кількість до створеного товару" або "створити нову картку товару і видалити крос".',
                    "error",
                )
                return redirect(url_for("admin_parts", warehouse_id=warehouse_id or "all"))
        before_template_qty = template_unassigned_qty(template)
        if template:
            ensure_template_barcode(db, template)
            if not name:
                name = template.name or ""
            if not description:
                description = template.description or ""
            if not price_usd:
                price_usd = float(template.price_usd or 0)
        if not part_number or not name:
            flash("Вкажіть OEM номер і опис товару", "error")
            return redirect(url_for("admin_parts", warehouse_id=warehouse_id))

        existing_photo_url = (request.form.get("existing_photo_url") or "").strip()
        photo_urls, gallery_urls, has_photo = build_new_part_media(
            request.files.getlist("photos"),
            f"part_gallery_{part_number or 'new'}",
            request.form.get("export_photo_upload_index", ""),
            existing_photo_url,
        )
        if template and not photo_urls:
            photo_urls = template.photo_urls or ""
            gallery_urls = template.showcase_photo_urls or dump_media_urls([template.photo_urls] if template.photo_urls else [])
            has_photo = bool(primary_template_photo(template))

        requested_producer_type = normalize_text(request.form.get("producer_type", "")).strip()
        producer_type = producer_type_label(requested_producer_type or (template.producer_type if template else "OEM"))
        brand_value = normalize_text(request.form.get("brand", "").strip() or (template.brand if template else "")).strip()

        if warehouse_id is None:
            payload = {
                "brand": brand_value,
                "producer_type": producer_type,
                "name": name,
                "description": description,
                "price_usd": price_usd,
                "unassigned_qty": before_template_qty + qty,
                "photo_urls": photo_urls or (template.photo_urls if template else ""),
                "showcase_photo_urls": parse_media_urls(gallery_urls) or template_gallery_urls(template),
                "youtube_url": normalize_text(request.form.get("youtube_url", "").strip()),
            }
            if cross_numbers_submitted:
                payload["cross_numbers"] = cross_numbers
            template, _ = upsert_part_template(db, part_number, payload)
            ensure_template_barcode(db, template)
            apply_template_to_parts(db, template)
            queue_template_inventory_change(
                db,
                template,
                before_template_qty,
                context_label="Створення товару → Всі товари",
                reason="Збережено через форму додавання",
            )
            db.commit()
            flash("Товар додано в базу без прив'язки до складу", "success")
            return redirect(url_for("admin_parts", warehouse_id="all"))

        existing = (
            db.query(Part)
            .filter(Part.warehouse_id == warehouse_id, Part.part_number == part_number)
            .order_by(Part.id.asc())
            .first()
        )

        if existing and not existing.is_deleted:
            flash("Товар з таким OEM номером уже є в цьому складі. Відредагуйте існуючу картку.", "error")
            return redirect(url_for("admin_parts", warehouse_id=warehouse_id))

        if existing and existing.is_deleted:
            part = existing
            before_part_qty = 0
            part.is_deleted = qty <= 0
            part.deleted_at = now() if qty <= 0 else None
            part.name = name
            part.description = description
            part.price_usd = price_usd
            part.qty = qty
            part.in_stock = qty > 0
            part.photo_urls = photo_urls
            part.showcase_photo_urls = gallery_urls
            part.youtube_url = normalize_text(request.form.get("youtube_url", "").strip())
            part.has_photo = has_photo
            part.has_description = bool(description)
            part.brand = brand_value or part.brand or ""
            part.producer_type = producer_type
            part.brand_export = brand_value or part.brand or ""
            part.part_number_export = part_number
            if template and template.barcode:
                part.barcode = template.barcode
            part.updated_at = now()
            action_text = "відновлено"
        else:
            before_part_qty = 0
            part = Part(
                warehouse_id=warehouse_id,
                part_number=part_number,
                brand=brand_value,
                producer_type=producer_type,
                name=name,
                description=description,
                price_usd=price_usd,
                qty=qty,
                in_stock=qty > 0,
                photo_urls=photo_urls,
                showcase_photo_urls=gallery_urls,
                youtube_url=normalize_text(request.form.get("youtube_url", "").strip()),
                has_photo=has_photo,
                has_description=bool(description),
                brand_export=brand_value,
                part_number_export=part_number,
                created_at=now(),
                updated_at=now(),
            )
            db.add(part)
            action_text = "додано"
        template_payload = {
            "brand": brand_value,
            "producer_type": producer_type,
            "name": name,
            "description": description,
            "price_usd": price_usd,
            "photo_urls": photo_urls or (template.photo_urls if template else ""),
            "showcase_photo_urls": parse_media_urls(gallery_urls) or template_gallery_urls(template),
            "youtube_url": normalize_text(request.form.get("youtube_url", "").strip()),
        }
        if cross_numbers_submitted:
            template_payload["cross_numbers"] = cross_numbers
        template, _ = upsert_part_template(db, part_number, template_payload)
        rebalance_template_assignment_qty(template, 0, qty)
        after_template_qty = template_unassigned_qty(template)
        ensure_template_barcode(db, template)
        apply_template_to_parts(db, template, only_parts=[part])
        part.brand_export = normalize_text(template.brand or "").strip()
        part.part_number_export = part_number
        ensure_part_barcode(db, part)
        part_delta = int(qty or 0) - int(before_part_qty or 0)
        template_delta = int(after_template_qty or 0) - int(before_template_qty or 0)
        assigned_qty = min(max(part_delta, 0), max(-template_delta, 0))
        if assigned_qty:
            queue_inventory_assignment_change(
                db,
                template=template,
                warehouse_name=warehouse.name,
                qty=assigned_qty,
                to_warehouse=True,
                warehouse_qty=int(qty or 0),
                unassigned_qty=after_template_qty,
                context_label=f"Створення товару → {warehouse.name}",
                reason="Присвоєно склад через форму додавання",
            )
        actual_part_increase = max(part_delta - assigned_qty, 0)
        if actual_part_increase:
            queue_part_inventory_change(
                db,
                part,
                int(qty or 0) - actual_part_increase,
                context_label=f"Створення товару → {warehouse.name}",
                reason="Додано понад залишок Всі товари через форму додавання",
            )
        flash_news(db, "parts", "Оновлено картку товару", f"Товар {part.part_number} {action_text} у склад {warehouse.name}.", "success")
        db.commit()
        flash("Картку товару збережено", "success")
        return redirect(url_for("admin_parts", warehouse_id=warehouse_id))
    finally:
        db.close()


@app.route("/admin/part/<int:part_id>/update", methods=["POST"])
@admin_required
def update_part(part_id):
    db = SessionLocal()
    try:
        part = db.get(Part, part_id)
        if not part:
            flash("Товар не знайдено", "error")
            return redirect(url_for("admin_parts"))
        before_qty = int(part.qty or 0)
        before_part_number = normalize_text(part.part_number or "").strip().upper()

        new_part_number = request.form.get("part_number", part.part_number).strip().upper() or part.part_number
        duplicate = (
            db.query(Part)
            .filter(
                Part.warehouse_id == part.warehouse_id,
                Part.part_number == new_part_number,
                Part.id != part.id,
                Part.is_deleted == False,
            )
            .first()
        )
        if duplicate:
            flash("У цьому складі вже є інша активна картка з таким OEM номером.", "error")
            return redirect(url_for("admin_parts", warehouse_id=part.warehouse_id))

        existing_template = find_part_template(db, new_part_number)
        before_template_qty = template_unassigned_qty(existing_template)
        cross_numbers = (
            cross_numbers_from_form(request.form, "cross_numbers", new_part_number)
            if "cross_numbers" in request.form
            else template_cross_numbers(existing_template)
        )
        part.part_number = new_part_number
        part.part_number_export = new_part_number
        requested_name = request.form.get("name", part.name).strip()
        requested_brand = request.form.get("brand", part.brand).strip()
        requested_type = producer_type_label(request.form.get("producer_type", part.producer_type))
        requested_description = request.form.get("description", part.description).strip()
        requested_price = float(request.form.get("price_usd", part.price_usd) or 0)
        requested_qty = int(request.form.get("qty", part.qty) or 0)
        part.name = requested_name
        part.brand = requested_brand
        part.producer_type = requested_type
        part.description = requested_description
        part.price_usd = requested_price
        part.qty = requested_qty
        part.in_stock = part.qty > 0
        part.has_description = bool(part.description)
        part.photo_urls, part.showcase_photo_urls, part.has_photo = build_updated_part_media(
            part,
            request.files.getlist("photos"),
            f"part_gallery_{part.id}",
            request.form.get("export_photo_upload_index", ""),
            (request.form.get("export_photo_existing") or "").strip(),
            request.form.getlist("remove_photo_urls"),
        )
        part.youtube_url = normalize_text(request.form.get("youtube_url", part.youtube_url).strip())
        part.is_deleted = False
        part.deleted_at = None
        template_payload = {
            "brand": requested_brand,
            "producer_type": requested_type,
            "name": requested_name,
            "description": requested_description,
            "price_usd": requested_price,
            "photo_urls": part.photo_urls or (existing_template.photo_urls if existing_template else ""),
            "showcase_photo_urls": parse_media_urls(part.showcase_photo_urls) or template_gallery_urls(existing_template),
            "youtube_url": part.youtube_url or (existing_template.youtube_url if existing_template else ""),
            "cross_numbers": cross_numbers,
        }
        template, _ = upsert_part_template(db, new_part_number, template_payload)
        if before_part_number == new_part_number:
            rebalance_template_assignment_qty(template, before_qty, requested_qty)
        after_template_qty = template_unassigned_qty(template)
        ensure_template_barcode(db, template)
        apply_template_to_parts(db, template, only_parts=[part])
        part.brand_export = normalize_text(template.brand or "").strip()
        part.part_number_export = new_part_number
        ensure_part_barcode(db, part)
        part.updated_at = now()
        if before_part_number == new_part_number:
            part_delta = int(requested_qty or 0) - int(before_qty or 0)
            template_delta = int(after_template_qty or 0) - int(before_template_qty or 0)
            assigned_qty = min(max(part_delta, 0), max(-template_delta, 0))
            returned_qty = min(max(-part_delta, 0), max(template_delta, 0))
            if assigned_qty:
                queue_inventory_assignment_change(
                    db,
                    template=template,
                    warehouse_name=part.warehouse.name if part.warehouse else "Склад",
                    qty=assigned_qty,
                    to_warehouse=True,
                    warehouse_qty=int(requested_qty or 0),
                    unassigned_qty=after_template_qty,
                    context_label=f"Редагування товару → {part.warehouse.name if part.warehouse else 'Склад'}",
                    reason="Присвоєно склад через редагування",
                )
            if returned_qty:
                queue_inventory_assignment_change(
                    db,
                    template=template,
                    warehouse_name=part.warehouse.name if part.warehouse else "Склад",
                    qty=returned_qty,
                    to_warehouse=False,
                    warehouse_qty=int(requested_qty or 0),
                    unassigned_qty=after_template_qty,
                    context_label="Редагування товару → Всі товари",
                    reason="Повернуто зі складу через редагування",
                )
            actual_part_increase = max(part_delta - assigned_qty, 0)
            if actual_part_increase:
                queue_part_inventory_change(
                    db,
                    part,
                    int(requested_qty or 0) - actual_part_increase,
                    context_label=f"Редагування товару → {part.warehouse.name if part.warehouse else 'Без складу'}",
                    reason="Додано понад залишок Всі товари через редагування",
                )
        else:
            queue_part_inventory_change(
                db,
                part,
                before_qty,
                context_label=f"Редагування товару → {part.warehouse.name if part.warehouse else 'Без складу'}",
                reason="Збережено зміни у картці товару",
            )
        flash_news(db, "parts", "Оновлено товар", f"Товар {part.part_number} оновлено.", "info")
        db.commit()
        flash("Картку товару оновлено", "success")
        return redirect(url_for("admin_parts", warehouse_id=part.warehouse_id))
    finally:
        db.close()


@app.route("/admin/part/<int:part_id>/delete", methods=["POST"])
@admin_required
def delete_part(part_id):
    db = SessionLocal()
    try:
        part = db.get(Part, part_id)
        if not part:
            flash("Товар не знайдено", "error")
            return redirect(url_for("admin_parts"))
        confirm_text = (request.form.get("confirm_text") or "").strip().lower()
        if confirm_text not in {"так", "tak", "с‚р°рє"}:
            flash('Для видалення потрібно ввести "так"', "error")
            return redirect(url_for("admin_parts", warehouse_id=part.warehouse_id))
        before_qty = int(part.qty or 0)
        part.is_deleted = True
        part.deleted_at = now()
        part.qty = 0
        part.in_stock = False
        part.updated_at = now()
        queue_part_inventory_change(
            db,
            part,
            before_qty,
            context_label=f"Видалення товару → {part.warehouse.name if part.warehouse else 'Без складу'}",
            reason="Позицію прибрано зі складу без видалення master-картки",
        )
        flash_news(db, "parts", "Товар приховано", f"Товар {part.part_number} прибрано з активного складу, шаблон збережено.", "info")
        db.commit()
        flash("Товар приховано. Дані збережено як шаблон.", "success")
        return redirect(url_for("admin_parts", warehouse_id=part.warehouse_id))
    finally:
        db.close()


@app.route("/admin/barcodes/part/<int:part_id>", methods=["POST"])
@admin_required
def print_part_barcodes(part_id):
    db = SessionLocal()
    try:
        part = db.get(Part, part_id)
        if not part:
            flash("Товар не знайдено", "error")
            return redirect(url_for("admin_parts"))
        qty = max(int(request.form.get("label_qty", part.qty or 0) or 0), 0)
        if qty <= 0:
            flash("Вкажіть кількість етикеток більше нуля", "error")
            return redirect(url_for("admin_parts", warehouse_id=part.warehouse_id))
        ensure_part_barcode(db, part)
        warehouse = db.get(Warehouse, part.warehouse_id)
        db.commit()
        labels = [
            build_print_label(
                headline=part.part_number,
                title=part.name,
                description=part.description,
                barcode=part.barcode or "",
                context=warehouse.name if warehouse else "",
            )
            for _ in range(qty)
        ]
        return render_template("print_labels.html", title=f"Етикетки {part.part_number}", labels=labels)
    finally:
        db.close()


@app.route("/admin/cars")
@admin_required
def admin_cars():
    db = SessionLocal()
    try:
        cars = db.query(Car).order_by(desc(Car.created_at)).all()
        news = db.query(NewsFeed).order_by(desc(NewsFeed.created_at)).limit(12).all()
        return render_template("admin_cars.html", cars=cars, news=news, safe_photo=safe_photo)
    finally:
        db.close()


@app.route("/admin/cars/<int:car_id>/update", methods=["POST"])
@admin_required
def update_car(car_id):
    db = SessionLocal()
    try:
        car = db.get(Car, car_id)
        if not car:
            flash("Авто не знайдено", "error")
            return redirect(url_for("admin_cars"))
        car.brand = request.form.get("brand", car.brand).strip()
        car.model = request.form.get("model", car.model).strip()
        car.vin = request.form.get("vin", car.vin).strip()
        car.year = int(request.form.get("year") or 0) if request.form.get("year") else None
        car.mileage = int(request.form.get("mileage") or 0) if request.form.get("mileage") else None
        car.status = request.form.get("status", car.status).strip() or "in_stock"
        car.price_usd = float(request.form.get("price_usd", car.price_usd) or 0)
        car.description = request.form.get("description", car.description).strip()
        car.youtube_url = normalize_text(request.form.get("youtube_url", car.youtube_url).strip())

        current_images = [
            item for item in parse_media_urls(car.image_urls)
            if item not in set(request.form.getlist("remove_image_urls"))
        ]
        current_images.extend(save_uploads(request.files.getlist("images"), f"car_gallery_{car.id}"))
        car.image_urls = dump_media_urls(current_images)

        flash_news(db, "cars", "Оновлено авто", f"{car.brand} {car.model} оновлено.", "info")
        db.commit()
        flash("Картку авто оновлено", "success")
        return redirect(url_for("admin_cars"))
    finally:
        db.close()


@app.route("/admin/cars/delete", methods=["POST"])
@admin_required
def delete_car():
    db = SessionLocal()
    try:
        car_id = int(request.form.get("car_id") or 0)
        confirm_text = (request.form.get("confirm_text") or "").strip().lower()
        if confirm_text not in {"так", "tak", "с‚р°рє"}:
            flash('Р”Р»СЏ РІРёРґР°Р»РµРЅРЅСЏ РїРѕС‚СЂС–Р±РЅРѕ РІРІРµСЃС‚Рё "С‚Р°Рє"', "error")
            return redirect(url_for("admin_cars"))
        car = db.get(Car, car_id)
        if not car:
            flash("РђРІС‚Рѕ РЅРµ Р·РЅР°Р№РґРµРЅРѕ", "error")
            return redirect(url_for("admin_cars"))
        car_name = " ".join(x for x in [car.brand, car.model] if x).strip() or car.vin or f"РђРІС‚Рѕ #{car.id}"
        db.delete(car)
        flash_news(db, "cars", "Р’РёРґР°Р»РµРЅРѕ Р°РІС‚Рѕ", f"{car_name} РІРёРґР°Р»РµРЅРѕ Р· РІРєР»Р°РґРєРё Р°РІС‚Рѕ.", "info")
        db.commit()
        flash(f"{car_name} РІРёРґР°Р»РµРЅРѕ", "success")
    except ValueError:
        flash("РћР±РµСЂС–С‚СЊ Р°РІС‚Рѕ РґР»СЏ РІРёРґР°Р»РµРЅРЅСЏ", "error")
    except Exception as e:
        db.rollback()
        flash(f"РќРµ РІРґР°Р»РѕСЃСЏ РІРёРґР°Р»РёС‚Рё Р°РІС‚Рѕ: {e}", "error")
    finally:
        db.close()
    return redirect(url_for("admin_cars"))


@app.route("/admin/cars/wizard", methods=["GET", "POST"])
@admin_required
def car_wizard():
    step = int(request.args.get("step", "1"))
    car_draft = session.get("car_draft", {})
    if request.method == "POST":
        if step == 1:
            car_draft["brand"] = request.form.get("brand", "").strip()
            car_draft["model"] = request.form.get("model", "").strip()
            car_draft["vin"] = request.form.get("vin", "").strip()
            session["car_draft"] = car_draft
            return redirect(url_for("car_wizard", step=2))
        if step == 2:
            car_draft["year"] = request.form.get("year", "").strip()
            car_draft["mileage"] = request.form.get("mileage", "").strip()
            car_draft["status"] = request.form.get("status", "in_stock").strip()
            car_draft["price_usd"] = request.form.get("price_usd", "0").strip()
            session["car_draft"] = car_draft
            return redirect(url_for("car_wizard", step=3))
        if step == 3:
            car_draft["description"] = request.form.get("description", "").strip()
            current_images = parse_media_urls(car_draft.get("image_urls", ""))
            current_images.extend(save_uploads(request.files.getlist("images"), "car_showcase"))
            single_image = request.files.get("image")
            if single_image and single_image.filename:
                current_images.append(save_upload(single_image, "car_showcase_single"))
            car_draft["image_urls"] = dump_media_urls(current_images)
            car_draft["youtube_url"] = normalize_text(request.form.get("youtube_url", "").strip())
            session["car_draft"] = car_draft
            return redirect(url_for("car_wizard", step=4))
        if step == 4:
            db = SessionLocal()
            try:
                car = Car(
                    brand=car_draft.get("brand", ""),
                    model=car_draft.get("model", ""),
                    vin=car_draft.get("vin", ""),
                    year=int(car_draft.get("year") or 0) if car_draft.get("year") else None,
                    mileage=int(car_draft.get("mileage") or 0) if car_draft.get("mileage") else None,
                    status=car_draft.get("status", "in_stock"),
                    price_usd=float(car_draft.get("price_usd") or 0),
                    description=car_draft.get("description", ""),
                    image_urls=car_draft.get("image_urls", ""),
                    youtube_url=car_draft.get("youtube_url", ""),
                    created_at=now()
                )
                db.add(car)
                flash_news(db, "cars", "Р”РѕРґР°РЅРѕ Р°РІС‚Рѕ", f"{car.brand} {car.model} РґРѕРґР°РЅРѕ РІ РєР°С‚Р°Р»РѕРі Р°РІС‚Рѕ.", "success")
                db.commit()
            finally:
                db.close()
            session.pop("car_draft", None)
            flash("РђРІС‚Рѕ РґРѕРґР°РЅРѕ РґРѕ РєР°С‚Р°Р»РѕРіСѓ", "success")
            return redirect(url_for("admin_cars"))
    db = SessionLocal()
    try:
        news = db.query(NewsFeed).order_by(desc(NewsFeed.created_at)).limit(12).all()
    finally:
        db.close()
    return render_template("car_wizard.html", step=step, car_draft=car_draft, news=news)



@app.route("/admin/orders")
@admin_required
def admin_orders():
    db = SessionLocal()
    try:
        selected_group = request.args.get("group", "new").strip() or "new"
        orders = db.query(Order).order_by(desc(Order.created_at)).all()
        order_cards = [serialize_admin_order(db, order) for order in orders]
        counts = {
            "new": sum(1 for order in order_cards if order["group"] == "new"),
            "active": sum(1 for order in order_cards if order["group"] == "active"),
            "history": sum(1 for order in order_cards if order["group"] == "history"),
        }
        if selected_group not in counts:
            selected_group = "new"
        packing_requests = (
            db.query(PackingRequest)
            .filter(PackingRequest.status != "deleted", PackingRequest.source_type == "manual")
            .order_by(desc(PackingRequest.created_at))
            .all()
        )
        news = db.query(NewsFeed).order_by(desc(NewsFeed.created_at)).limit(12).all()
        return render_template(
            "admin_orders.html",
            orders=[item for item in order_cards if item["group"] == selected_group],
            order_counts=counts,
            selected_group=selected_group,
            packing_requests=[serialize_packing_request(db, item) for item in packing_requests],
            news=news,
        )
    finally:
        db.close()


@app.route("/admin/packing/create", methods=["POST"])
@admin_required
def admin_packing_create():
    db = SessionLocal()
    try:
        items_payload = json_loads_safe(request.form.get("items_json"), [])
        delivery_type = request.form.get("delivery_type", "pickup")
        np_payload = packing_np_payload_from_form(request.form)
        control_payment_raw = request.form.get("control_payment_uah", "")
        is_nova_poshta = normalized_delivery_type(delivery_type) == "nova_poshta"
        control_payment_uah = 0.0
        if is_nova_poshta and not normalize_text(control_payment_raw or "").strip():
            flash('Для Нової пошти вкажіть суму в полі "Контроль оплати".', "error")
            return redirect(url_for("admin_orders"))
        if normalize_text(control_payment_raw or "").strip():
            control_payment_uah = parse_control_payment_amount(control_payment_raw)
            if control_payment_uah is None:
                flash('Некоректна сума у полі "Контроль оплати".', "error")
                return redirect(url_for("admin_orders"))
        request_obj = create_packing_request_from_payload(
            db,
            items_payload=items_payload,
            delivery_type=delivery_type,
            source_type="manual",
            customer_name=request.form.get("customer_name", ""),
            phone=request.form.get("phone", ""),
            comment=request.form.get("comment", ""),
            control_payment_uah=control_payment_uah,
            np_payload=np_payload,
        )
        if not request_obj.items:
            db.rollback()
            flash("Додайте товари для збирання", "error")
            return redirect(url_for("admin_orders"))
        flash_news(
            db,
            "packing",
            "Створено заявку на пакування",
            f"Заявка #{request_obj.id}: {len(request_obj.items)} позицій, спосіб доставки — {'Нова пошта' if request_obj.delivery_type == 'nova_poshta' else 'Самовивіз'}.",
            "info",
        )
        db.commit()
        flash("Заявку відправлено в додаток", "success")
        return redirect(url_for("admin_orders"))
    finally:
        db.close()


@app.route("/admin/orders/<int:order_id>/packing", methods=["POST"])
@admin_required
def admin_order_send_to_packing(order_id):
    db = SessionLocal()
    try:
        order = db.get(Order, order_id)
        if not order:
            flash("Замовлення не знайдено", "error")
            return redirect(url_for("admin_orders"))
        existing_request = latest_packing_request_for_order(db, order.id)
        if existing_request:
            flash("Це замовлення вже передано в додаток на збірку", "error")
            return redirect(url_for("admin_orders", group=order_group_for_status(order.status)))
        items_payload = []
        for item in order.items:
            items_payload.append({
                "partId": item.part_id,
                "partNumber": item.part_number,
                "qty": int(item.qty or 0),
            })
        request_obj = create_packing_request_from_payload(
            db,
            items_payload=items_payload,
            delivery_type=infer_order_delivery_type(order),
            source_type="order",
            source_order_id=order.id,
            customer_name=order.customer_name or "",
            phone=order.phone or "",
            city=order.city or "",
            comment=f"Замовлення #{order.id}",
            control_payment_uah=float(order.prepayment_usd or 0),
            np_payload=order_np_payload(order),
        )
        if not request_obj.items:
            db.rollback()
            flash("У замовленні немає позицій для збирання", "error")
            return redirect(url_for("admin_orders"))
        order.status = "processing"
        order.is_processing = True
        order.updated_at = now()
        flash_news(db, "packing", "Замовлення відправлено на пакування", f"Замовлення #{order.id} передано в додаток.", "info")
        db.commit()
        flash("Замовлення передано на пакування", "success")
        return redirect(url_for("admin_orders", group="active"))
    finally:
        db.close()


@app.route("/admin/packing/<int:request_id>/apply-stock", methods=["POST"])
@admin_required
def admin_packing_apply_stock(request_id):
    db = SessionLocal()
    try:
        request_obj = db.get(PackingRequest, request_id)
        if not request_obj:
            flash("Заявку не знайдено", "error")
            return redirect(url_for("admin_orders"))
        changed = 0
        for item in request_obj.items:
            if item.status not in {"missing", "partial"}:
                continue
            missing_qty = max(int(item.missing_qty or 0), 0)
            if missing_qty <= 0:
                continue
            part = db.get(Part, item.part_id) if item.part_id else None
            if part:
                before_qty = int(part.qty or 0)
                part.qty = max(int(part.qty or 0) - missing_qty, 0)
                part.in_stock = part.qty > 0
                part.stock_checked_at = now()
                part.stock_check_status = "updated"
                part.stock_check_note = f"Під час збирання заявки #{request_obj.id} не знайдено {missing_qty} шт."
                part.updated_at = now()
                queue_part_inventory_change(
                    db,
                    part,
                    before_qty,
                    context_label=f"Стоїть на видачі → Заявка #{request_obj.id}",
                    reason=f"Пересорт / не знайдено {missing_qty} шт.",
                )
                changed += 1
                continue
            template = resolve_packing_request_template(db, item)
            if not template:
                continue
            before_qty = template_unassigned_qty(template)
            template.unassigned_qty = max(template_unassigned_qty(template) - missing_qty, 0)
            template.updated_at = now()
            queue_template_inventory_change(
                db,
                template,
                before_qty,
                context_label=f"Стоїть на видачі → Заявка #{request_obj.id}",
                reason=f"Пересорт / не знайдено {missing_qty} шт.",
            )
            changed += 1
        request_obj.status = "applied"
        request_obj.updated_at = now()
        order = db.get(Order, request_obj.source_order_id) if request_obj.source_order_id else None
        if order:
            keyed_items = {
                (int(order_item.part_id or 0), normalize_text(order_item.part_number or "").strip().upper()): order_item
                for order_item in order.items
            }
            total_usd = 0.0
            for request_item in request_obj.items:
                key = (int(request_item.part_id or 0), normalize_text(request_item.part_number or "").strip().upper())
                order_item = keyed_items.get(key)
                if not order_item:
                    continue
                adjusted_qty = max(int(request_item.expected_qty or 0) - max(int(request_item.missing_qty or 0), 0), 0)
                order_item.qty = adjusted_qty
                total_usd += float(order_item.price_usd or 0) * adjusted_qty
            order.total_usd = total_usd
            order.updated_at = now()
        flash_news(db, "packing", "Склад скориговано", f"По заявці #{request_obj.id} скориговано {changed} позицій.", "success")
        db.commit()
        flash("Залишки на складі оновлено", "success")
        return redirect(url_for("admin_orders"))
    finally:
        db.close()


@app.route("/admin/packing/<int:request_id>/edit", methods=["POST"])
@admin_required
def admin_packing_edit(request_id):
    db = SessionLocal()
    try:
        request_obj = db.get(PackingRequest, request_id)
        if not request_obj:
            flash("Заявку не знайдено", "error")
            return redirect(url_for("admin_orders"))
        items_payload = json_loads_safe(request.form.get("items_json"), [])
        delivery_type = request.form.get("delivery_type", request_obj.delivery_type)
        control_payment_raw = request.form.get("control_payment_uah", "")
        is_nova_poshta = normalized_delivery_type(delivery_type) == "nova_poshta"
        control_payment_uah = None
        if is_nova_poshta and not normalize_text(control_payment_raw or "").strip():
            flash('Для Нової пошти вкажіть суму в полі "Контроль оплати".', "error")
            return redirect(url_for("admin_orders"))
        if normalize_text(control_payment_raw or "").strip():
            control_payment_uah = parse_control_payment_amount(control_payment_raw)
            if control_payment_uah is None:
                flash('Некоректна сума у полі "Контроль оплати".', "error")
                return redirect(url_for("admin_orders"))
        elif is_nova_poshta:
            control_payment_uah = 0.0
        update_packing_request_items(
            db,
            request_obj,
            items_payload,
            delivery_type,
            customer_name=request.form.get("customer_name", request_obj.customer_name),
            phone=request.form.get("phone", request_obj.phone),
            comment=request.form.get("comment", request_obj.comment),
            control_payment_uah=control_payment_uah,
            np_payload=packing_np_payload_from_form(request.form),
        )
        flash_news(db, "packing", "Змінено заявку на пакування", f"Заявку #{request_obj.id} оновлено і знову відправлено в додаток.", "info")
        db.commit()
        flash("Заявку оновлено", "success")
        return redirect(url_for("admin_orders"))
    finally:
        db.close()


@app.route("/admin/packing/<int:request_id>/delete", methods=["POST"])
@admin_required
def admin_packing_delete(request_id):
    db = SessionLocal()
    try:
        request_obj = db.get(PackingRequest, request_id)
        if not request_obj:
            flash("Заявку не знайдено", "error")
            return redirect(url_for("admin_orders"))
        request_obj.status = "deleted"
        request_obj.updated_at = now()
        flash_news(db, "packing", "Заявку видалено", f"Заявку #{request_obj.id} видалено.", "info")
        db.commit()
        flash("Заявку видалено", "success")
        return redirect(url_for("admin_orders"))
    finally:
        db.close()


@app.route("/admin/packing/<int:request_id>/writeoff", methods=["POST"])
@admin_required
def admin_packing_writeoff(request_id):
    db = SessionLocal()
    try:
        request_obj = db.get(PackingRequest, request_id)
        if not request_obj:
            flash("Заявку не знайдено", "error")
            return redirect(url_for("admin_orders"))
        if (request_obj.source_type or "manual") != "manual":
            flash("Списати можна лише ручну видачу", "error")
            return redirect(url_for("admin_orders"))
        if request_obj.status == "deleted":
            flash("Заявку вже закрито", "error")
            return redirect(url_for("admin_orders"))

        for packing_item in request_obj.items or []:
            if find_part_template(db, packing_item.part_number or ""):
                continue
            part = db.get(Part, packing_item.part_id) if packing_item.part_id else None
            if part:
                sync_template_from_part(db, part)

        total_found = sum(max(int(item.found_qty or 0), 0) for item in request_obj.items or [])
        confirm_text = normalize_text(request.form.get("confirm_text", "")).strip().lower()
        force_full = confirm_text in {"так", "tak"}
        if total_found <= 0 and not force_full:
            flash('Товар не скановано. Для списання введіть "так".', "error")
            return redirect(url_for("admin_orders"))

        items_payload = packing_request_writeoff_payload(request_obj, force_full=force_full)
        if not items_payload:
            flash("Немає позицій для списання", "error")
            return redirect(url_for("admin_orders"))

        try:
            order = create_manual_issue_order(
                db,
                destination=normalize_text(request_obj.customer_name or request_obj.comment or f"Видача #{request_obj.id}").strip(),
                items_payload=items_payload,
            )
        except ValueError as exc:
            db.rollback()
            code = str(exc)
            if code.startswith("item_not_found:"):
                item_ref = code.split(":", 1)[1]
                flash(f"Позицію {item_ref} не знайдено у базі.", "error")
                return redirect(url_for("admin_orders"))
            if code.startswith("not_enough:"):
                _, part_number, available_qty = code.split(":", 2)
                flash(f"Для {part_number} доступно лише {available_qty} шт. у наявності.", "error")
                return redirect(url_for("admin_orders"))
            raise

        request_obj.status = "deleted"
        request_obj.updated_at = now()
        if not normalize_text(order.comment or "").strip():
            order.comment = "Видано вручну"
        order.updated_at = now()
        db.commit()
        flash("Товар списано і перенесено в історію", "success")
        return redirect(url_for("admin_orders", group="history"))
    finally:
        db.close()


@app.route("/admin/packing/<int:request_id>/send-to-order", methods=["POST"])
@admin_required
def admin_packing_send_to_order(request_id):
    db = SessionLocal()
    try:
        request_obj = db.get(PackingRequest, request_id)
        if not request_obj:
            flash("Заявку не знайдено", "error")
            return redirect(url_for("admin_orders"))
        if request_obj.status != "packed":
            flash("Спочатку завершіть пакування в додатку", "error")
            return redirect(url_for("admin_orders"))

        order = db.get(Order, request_obj.source_order_id) if request_obj.source_order_id else None
        effective_delivery_type = effective_packing_delivery_type(request_obj, order)
        request_obj.delivery_type = effective_delivery_type
        if effective_delivery_type == "nova_poshta" and order:
            fallback_np_payload = order_np_payload(order)
            if not normalize_text(request_obj.city or "").strip():
                request_obj.city = fallback_np_payload.get("city_name", "")
            if not normalize_text(request_obj.np_city_ref or "").strip():
                request_obj.np_city_ref = fallback_np_payload.get("city_ref", "")
            if not normalize_text(request_obj.np_warehouse_ref or "").strip():
                request_obj.np_warehouse_ref = fallback_np_payload.get("warehouse_ref", "")
            if not normalize_text(request_obj.np_warehouse_label or "").strip():
                request_obj.np_warehouse_label = fallback_np_payload.get("warehouse_label", "")
            if not normalize_text(request_obj.np_street_ref or "").strip():
                request_obj.np_street_ref = fallback_np_payload.get("street_ref", "")
            if not normalize_text(request_obj.np_street_name or "").strip():
                request_obj.np_street_name = fallback_np_payload.get("street_name", "")
            if not normalize_text(request_obj.np_house or "").strip():
                request_obj.np_house = fallback_np_payload.get("house", "")
            request_obj.np_service_type = normalized_np_service_type(request_obj.np_service_type or fallback_np_payload.get("service_type", "warehouse"))
        control_payment_uah = None
        if effective_delivery_type == "nova_poshta":
            control_payment_uah = parse_control_payment_amount(request.form.get("control_payment_uah"))
            if control_payment_uah is None:
                control_payment_uah = parse_control_payment_amount(str(request_obj.control_payment_uah or ""))
            if control_payment_uah is None:
                flash('Вкажіть суму в полі "Контроль оплати".', "error")
                return redirect(url_for("admin_orders", group="active"))
            request_obj.control_payment_uah = control_payment_uah
        else:
            request_obj.control_payment_uah = 0
        if order and not order.stock_reserved:
            try:
                reserve_packing_request_inventory(db, request_obj)
            except ValueError as exc:
                db.rollback()
                code = str(exc)
                if code.startswith("item_not_found:"):
                    item_ref = code.split(":", 1)[1]
                    flash(f"Позицію {item_ref} не знайдено у базі.", "error")
                    return redirect(url_for("admin_orders"))
                if code.startswith("not_enough:"):
                    _, part_number, available_qty = code.split(":", 2)
                    flash(f"Для {part_number} доступно лише {available_qty} шт. у наявності.", "error")
                    return redirect(url_for("admin_orders"))
                raise
            order = db.get(Order, request_obj.source_order_id) if request_obj.source_order_id else None
            request_obj = db.get(PackingRequest, request_id)
            request_obj.delivery_type = effective_delivery_type
            order.stock_reserved = True
            order.updated_at = now()
        if order:
            for item in list(order.items):
                db.delete(item)
            db.flush()
        else:
            order = Order(
                customer_name=request_obj.customer_name or "Клієнт",
                phone=request_obj.phone or "",
                city=request_obj.city or "",
                comment=packing_request_order_comment(request_obj) or request_obj.comment or "",
                total_usd=0,
                status="awaiting_shipment",
                is_processing=True,
                prepayment_usd=control_payment_uah or 0,
                ttn="",
                ttn_status="",
                cancel_reason="",
                stock_reserved=True,
                external_source="",
                external_order_id="",
                external_status="",
                created_at=now(),
                updated_at=now(),
            )
            apply_np_payload_to_order(order, request_obj.delivery_type or "nova_poshta", packing_request_np_payload(request_obj))
            db.add(order)
            db.flush()

        total_usd = 0.0
        for item in request_obj.items:
            part = db.get(Part, item.part_id) if item.part_id else None
            price = float(part.price_usd or 0) if part else 0.0
            qty = max(int(item.expected_qty or 0), 0)
            total_usd += price * qty
            order.items.append(
                OrderItem(
                    part_id=item.part_id,
                    part_number=item.part_number or "",
                    name=item.title or item.part_number or "Товар",
                    qty=qty,
                    price_usd=price,
                )
            )
        order.total_usd = total_usd
        order.status = "awaiting_shipment"
        order.is_processing = True
        order.city = normalize_text(request_obj.city or order.city or "")
        order.comment = normalize_text(packing_request_order_comment(request_obj) or order.comment or "")
        apply_np_payload_to_order(order, effective_delivery_type, packing_request_np_payload(request_obj))
        if effective_delivery_type == "nova_poshta":
            order.prepayment_usd = control_payment_uah or 0
        else:
            order.prepayment_usd = 0
        order.updated_at = now()
        order.stock_reserved = True

        request_obj.source_order_id = order.id
        request_obj.status = "awaiting_shipment"
        request_obj.updated_at = now()
        flash_news(
            db,
            "orders",
            "Замовлення чекає відправки",
            (
                f"Замовлення #{order.id} передано у відправку Новою поштою."
                if effective_delivery_type == "nova_poshta"
                else f"Замовлення #{order.id} готове до видачі."
            ),
            "success",
        )
        db.commit()
        flash(
            "Замовлення передано у список відправки" if effective_delivery_type == "nova_poshta" else "Замовлення позначено як готове до видачі",
            "success",
        )
        return redirect(url_for("admin_orders", group="active"))
    finally:
        db.close()


@app.route("/admin/orders/<int:order_id>/edit-items", methods=["POST"])
@admin_required
def admin_order_edit(order_id):
    db = SessionLocal()
    try:
        order = db.get(Order, order_id)
        if not order:
            flash("Замовлення не знайдено", "error")
            return redirect(url_for("admin_orders"))
        if order.status in {"done", "cancelled"}:
            flash("Редагування доступне лише для активних замовлень", "error")
            return redirect(url_for("admin_orders", group=order_group_for_status(order.status)))
        packing_request = latest_packing_request_for_order(db, order.id)
        capabilities = admin_order_capabilities(order, packing_request)
        if not capabilities["canEditItems"]:
            flash("Після підтвердження в додатку змінювати склад замовлення вже не можна.", "error")
            return redirect(url_for("admin_orders", group=order_group_for_status(order.status)))

        items_payload = json_loads_safe(request.form.get("items_json"), [])
        if not items_payload:
            flash("Додайте хоча б одну позицію", "error")
            return redirect(url_for("admin_orders", group=order_group_for_status(order.status)))

        had_reserved_stock = bool(order.stock_reserved)
        if had_reserved_stock:
            release_order_inventory(db, order)

        for item in list(order.items):
            db.delete(item)
        db.flush()

        total_usd = 0.0
        normalized_items = []
        for raw in items_payload:
            try:
                part_id = int(raw.get("partId") or 0)
                qty = max(int(raw.get("qty") or 0), 0)
            except Exception:
                continue
            if not part_id or qty <= 0:
                continue
            part = db.get(Part, part_id)
            if not part or part.is_deleted:
                continue
            price = float(raw.get("priceUsd") or part.price_usd or 0)
            total_usd += price * qty
            normalized_items.append({"partId": part.id, "qty": qty})
            order.items.append(
                OrderItem(
                    part_id=part.id,
                    part_number=part.part_number or "",
                    name=part.name or "",
                    qty=qty,
                    price_usd=price,
                )
            )

        if not order.items:
            flash("Не вдалося сформувати жодної позиції для замовлення", "error")
            return redirect(url_for("admin_orders", group=order_group_for_status(order.status)))

        order.total_usd = total_usd
        order.updated_at = now()
        if order.status == "new":
            order.status = "processing"
            order.is_processing = True

        packing_request = latest_packing_request_for_order(db, order.id)
        if packing_request and packing_request.status not in {"packed", "awaiting_shipment", "shipped", "deleted"}:
            update_packing_request_items(db, packing_request, normalized_items, packing_request.delivery_type)
            flash_news(db, "orders", "Замовлення оновлено", f"Замовлення #{order.id} і повторний запит на збірку оновлено.", "info")
        else:
            flash_news(db, "orders", "Замовлення оновлено", f"Замовлення #{order.id} відредаговано.", "info")

        if had_reserved_stock:
            try:
                reserve_order_inventory(db, order)
            except ValueError as exc:
                db.rollback()
                flash(inventory_reserve_error_message(str(exc)), "error")
                return redirect(url_for("admin_orders", group=order_group_for_status(order.status)))
        else:
            order.stock_reserved = False

        db.commit()
        flash("Замовлення оновлено", "success")
        return redirect(url_for("admin_orders", group=order_group_for_status(order.status)))
    finally:
        db.close()


@app.route("/admin/orders/<int:order_id>/complete", methods=["POST"])
@admin_required
def admin_order_complete(order_id):
    db = SessionLocal()
    try:
        order = db.get(Order, order_id)
        if not order:
            flash("Замовлення не знайдено", "error")
            return redirect(url_for("admin_orders"))
        packing_request = latest_packing_request_for_order(db, order.id)
        capabilities = admin_order_capabilities(order, packing_request)
        if not capabilities["canComplete"]:
            flash("Замовлення ще не готове до завершення за поточним сценарієм.", "error")
            return redirect(url_for("admin_orders", group=order_group_for_status(order.status)))
        confirm_text = (request.form.get("confirm_text") or "").strip().lower()
        if confirm_text not in {"так", "tak", "с‚р°рє"}:
            flash('Для завершення потрібно ввести "так"', "error")
            return redirect(url_for("admin_orders", group=order_group_for_status(order.status)))
        if order.status == "cancelled":
            flash("Скасоване замовлення не можна завершити", "error")
            return redirect(url_for("admin_orders", group="history"))
        if not order.stock_reserved:
            try:
                reserve_order_inventory(db, order)
            except ValueError as exc:
                db.rollback()
                flash(inventory_reserve_error_message(str(exc)), "error")
                return redirect(url_for("admin_orders", group=order_group_for_status(order.status)))
        order.status = "done"
        order.is_processing = False
        order.updated_at = now()
        flash_news(db, "orders", "Замовлення виконано", f"Замовлення #{order.id} виконано.", "success")
        db.commit()
        flash("Замовлення переведено у виконані", "success")
        return redirect(url_for("admin_orders", group="history"))
    finally:
        db.close()


@app.route("/admin/orders/<int:order_id>/update", methods=["POST"])
@admin_required
def admin_order_update(order_id):
    db = SessionLocal()
    try:
        order = db.get(Order, order_id)
        if not order:
            flash("Замовлення не знайдено", "error")
            return redirect(url_for("admin_orders"))
        packing_request = latest_packing_request_for_order(db, order.id)
        capabilities = admin_order_capabilities(order, packing_request)
        order.is_processing = request.form.get("is_processing") == "on"
        control_payment_raw = request.form.get("prepayment_usd", "")
        if normalize_text(control_payment_raw or "").strip():
            control_payment_uah = parse_control_payment_amount(control_payment_raw)
            if control_payment_uah is None:
                flash('Некоректна сума у полі "Контроль оплати".', "error")
                return redirect(url_for("admin_orders", group=order_group_for_status(order.status)))
            order.prepayment_usd = control_payment_uah
        else:
            order.prepayment_usd = 0
        new_ttn = request.form.get("ttn", "").strip()
        previous_ttn = normalize_text(order.ttn or "").strip()
        ttn_changed = bool(new_ttn and new_ttn != previous_ttn)
        if ttn_changed and not capabilities["canDirectTtnEdit"]:
            flash("ТТН можна вносити лише після натискання \"Відправити замовлення\".", "error")
            return redirect(url_for("admin_orders", group=order_group_for_status(order.status)))
        if order.status == "cancelled":
            order.is_processing = False
        elif ttn_changed:
            order.ttn = new_ttn
            order.status = "shipped"
            order.is_processing = True
            order.ttn_status = "Очікує оновлення"
            queue_telegram_message(db, build_order_ttn_telegram_message(order, new_ttn))
            flash_news(db, "telegram", "ТТН передано в Telegram", f"Замовлення #{order.id}: {new_ttn}.", "info")

        if order.status == "cancelled":
            order.is_processing = False
        elif order.status == "done":
            order.is_processing = False
        elif order.status == "awaiting_shipment" and not order.ttn:
            order.is_processing = True
        elif order.ttn:
            order.status = "shipped"
            order.is_processing = True
        elif order.is_processing and order.status != "done":
            order.status = "processing"
        elif order.status not in ("processing", "done", "awaiting_shipment", "shipped"):
            order.status = "new"

        order.updated_at = now()
        flash_news(db, "orders", "Оновлено замовлення", f"Замовлення #{order.id} оновлено.", "info")
        db.commit()
        flash("Замовлення оновлено", "success")
        return redirect(url_for("admin_orders", group=order_group_for_status(order.status)))
    finally:
        db.close()


@app.route("/admin/orders/<int:order_id>/cancel", methods=["POST"])
@admin_required
def admin_order_cancel(order_id):
    db = SessionLocal()
    try:
        order = db.get(Order, order_id)
        if not order:
            flash("Замовлення не знайдено", "error")
            return redirect(url_for("admin_orders"))
        cancel_reason = (request.form.get("cancel_reason") or "").strip()
        if not cancel_reason:
            flash("Вкажіть причину відмови", "error")
            return redirect(url_for("admin_orders"))

        returned_units = 0
        if order.status != "cancelled":
            returned_units = release_order_inventory(db, order)
        order.status = "cancelled"
        order.cancel_reason = cancel_reason
        order.is_processing = False
        order.updated_at = now()
        flash_news(
            db,
            "orders",
            "Замовлення скасовано",
            f"Замовлення #{order.id} скасовано. Причина: {cancel_reason}. Повернуто на склад: {returned_units} шт.",
            "info",
        )
        db.commit()
        flash(f"Замовлення #{order.id} скасовано", "success")
        return redirect(url_for("admin_orders", group="history"))
    finally:
        db.close()


@app.route("/admin/orders/<int:order_id>/refresh-ttn", methods=["POST"])
@admin_required
def admin_order_refresh_ttn(order_id):
    db = SessionLocal()
    try:
        order = db.get(Order, order_id)
        if not order or not order.ttn:
            flash("TTN не вказано", "error")
            return redirect(url_for("admin_orders"))
        if order.status == "cancelled":
            flash("Скасоване замовлення не оновлює статус ТТН", "error")
            return redirect(url_for("admin_orders"))
        try:
            status = refresh_order_ttn_status_from_np(db, order)
        except ValueError as exc:
            db.rollback()
            flash(inventory_reserve_error_message(str(exc)), "error")
            return redirect(url_for("admin_orders", group=order_group_for_status(order.status)))
        if status:
            flash_news(db, "nova_poshta", "Оновлено статус ТТН", f"Замовлення #{order.id}: {status}", "info")
            db.commit()
            flash("Статус ТТН оновлено", "success")
        else:
            flash("Статус ТТН не знайдено", "error")
        return redirect(url_for("admin_orders", group=order_group_for_status(order.status)))
    except Exception as e:
        flash(f"Помилка оновлення ТТН: {e}", "error")
        return redirect(url_for("admin_orders"))
    finally:
        db.close()


@app.route("/admin/orders/sync/autopro", methods=["POST"])
@admin_required
def admin_sync_autopro_orders():
    flash("Інтеграцію Autopro вимкнено в кабінеті.", "error")
    return redirect(url_for("admin_orders"))


@app.route("/admin/orders/sync/prom", methods=["POST"])
@admin_required
def admin_sync_prom_orders():
    flash("Інтеграцію Prom.ua вимкнено в кабінеті.", "error")
    return redirect(url_for("admin_orders"))



@app.route("/admin/receiving/legacy", methods=["GET", "POST"])
@admin_required
def admin_receiving_legacy():
    db = SessionLocal()
    try:
        warehouses = db.query(Warehouse).order_by(Warehouse.name.asc()).all()
        draft = session.get("mobile_receiving_draft", [])
        selected_warehouse_id = session.get("mobile_receiving_selected_warehouse_id")
        if request.method == "POST":
            selected_warehouse_id = int(request.form.get("warehouse_id") or 0)
            session["mobile_receiving_selected_warehouse_id"] = selected_warehouse_id
            warehouse = db.get(Warehouse, selected_warehouse_id)
            if not warehouse:
                flash("РЎРєР»Р°Рґ РЅРµ Р·РЅР°Р№РґРµРЅРѕ", "error")
                return redirect(url_for("admin_receiving"))

            imported = 0
            updated = 0
            for item in draft:
                part_number = (item.get("partNumber") or "").strip()
                existing = db.query(Part).filter(Part.part_number == part_number).order_by(Part.id.asc()).first()
                if existing:
                    existing.qty = int(existing.qty or 0) + int(item.get("qty") or 0)
                    existing.in_stock = existing.qty > 0
                    if item.get("description"):
                        existing.description = ((existing.description or "").strip() + "\\n" + item.get("description", "").strip()).strip()
                        existing.has_description = bool(existing.description)
                    existing.producer_type = producer_type_label(existing.producer_type or "OEM")
                    existing.updated_at = now()
                    sync_template_from_part(db, existing)
                    updated += 1
                else:
                    new_part = Part(
                        warehouse_id=selected_warehouse_id,
                        part_number=part_number,
                        brand="",
                        producer_type=producer_type_label("OEM"),
                        name=(item.get("title") or "").strip(),
                        description=(item.get("description") or "").strip(),
                        price_usd=float(item.get("priceUsd") or 0),
                        qty=int(item.get("qty") or 0),
                        in_stock=int(item.get("qty") or 0) > 0,
                        photo_urls="",
                        has_photo=False,
                        has_description=bool((item.get("description") or "").strip()),
                        brand_export="",
                        part_number_export=part_number,
                        created_at=now(),
                        updated_at=now(),
                    )
                    db.add(new_part)
                    ensure_part_barcode(db, new_part)
                    sync_template_from_part(db, new_part)
                    imported += 1
            flash_news(db, "receiving", "РџСЂРёР№РѕРј С‚РѕРІР°СЂСѓ", f"Р†РјРїРѕСЂС‚РѕРІР°РЅРѕ: {imported}, РѕРЅРѕРІР»РµРЅРѕ: {updated}.", "success")
            db.commit()
            session["mobile_receiving_draft"] = []
            flash("РџСЂРёР№РѕРј С‚РѕРІР°СЂСѓ С–РјРїРѕСЂС‚РѕРІР°РЅРѕ РІ СЃРєР»Р°Рґ", "success")
            draft = []
        return render_template("admin_receiving.html", warehouses=warehouses, draft=draft, selected_warehouse_id=selected_warehouse_id, news=db.query(NewsFeed).order_by(desc(NewsFeed.created_at)).limit(12).all())
    finally:
        db.close()

@app.route("/admin/receiving")
@admin_required
def admin_receiving():
    db = SessionLocal()
    try:
        warehouses = db.query(Warehouse).order_by(Warehouse.name.asc()).all()
        selected_scope, selected_warehouse_id = parse_receiving_scope(request.args.get("warehouse_id"))
        draft_items = [serialize_receiving_item(db, item) for item in get_receiving_draft_items_for_scope(db, selected_scope)]
        return render_template(
            "admin_receiving.html",
            warehouses=warehouses,
            draft=draft_items,
            selected_scope=selected_scope,
            selected_warehouse_id=selected_warehouse_id,
            news=db.query(NewsFeed).order_by(desc(NewsFeed.created_at)).limit(12).all(),
        )
    finally:
        db.close()


@app.route("/admin/receiving/import", methods=["POST"])
@admin_required
def admin_receiving_import():
    db = SessionLocal()
    try:
        source_scope, _ = parse_receiving_scope(request.form.get("draft_scope"))
        target_scope, warehouse_id = parse_receiving_scope(request.form.get("warehouse_id"))
        items = get_receiving_draft_items_for_scope(db, source_scope)
        if not items:
            flash("Чернетка прийому товару порожня.", "error")
            return redirect(url_for("admin_receiving", warehouse_id=source_scope))

        imported, updated, warehouse, template_created, template_updated = import_receiving_items(db, items, warehouse_id)
        for item in items:
            db.delete(item)
        db.commit()
        if warehouse:
            flash(
                f"Прийомку збережено у базу та імпортовано у склад {warehouse.name}. "
                f"База: створено {template_created}, оновлено {template_updated}. "
                f"Склад: створено {imported}, оновлено {updated}.",
                "success",
            )
            return redirect(url_for("admin_receiving", warehouse_id=target_scope))
        flash(
            f"Позиції збережено у Всі товари. Створено {template_created}, оновлено {template_updated}.",
            "success",
        )
        return redirect(url_for("admin_receiving", warehouse_id="all"))
    except ValueError:
        flash("Склад не знайдено.", "error")
        return redirect(url_for("admin_receiving"))
    finally:
        db.close()


@app.route("/admin/receiving/<int:item_id>/update", methods=["POST"])
@admin_required
def admin_receiving_update(item_id):
    db = SessionLocal()
    try:
        item = db.get(ReceivingDraftItem, item_id)
        if not item:
            flash("Позицію чернетки не знайдено.", "error")
            return redirect(url_for("admin_receiving"))

        return_scope, _ = parse_receiving_scope(request.form.get("return_scope"))
        target_scope, warehouse_id = parse_receiving_scope(request.form.get("warehouse_id"))
        if target_scope not in {"all", "unassigned"}:
            warehouse = db.get(Warehouse, warehouse_id)
            if not warehouse:
                flash("Склад не знайдено.", "error")
                return redirect(url_for("admin_receiving", warehouse_id=return_scope))
        else:
            warehouse_id = None

        item.warehouse_id = warehouse_id
        item.part_number = (request.form.get("part_number") or item.part_number).strip().upper()
        item.title = (request.form.get("title") or item.title).strip()
        item.qty = int(request.form.get("qty") or item.qty or 1)
        item.price_usd = float(request.form.get("price_usd") or item.price_usd or 0)
        item.description = (request.form.get("description") or item.description).strip()
        current_gallery = parse_media_urls(item.photo_urls)
        uploaded_urls = save_uploads(request.files.getlist("photos"), f"receiving_{item.id}")
        gallery = parse_media_urls(current_gallery + uploaded_urls)
        selected_existing = normalize_text(request.form.get("export_photo_existing") or "").strip()
        selected_upload_url = ""
        if uploaded_urls:
            try:
                selected_upload_url = uploaded_urls[int(request.form.get("export_photo_upload_index") or 0)]
            except Exception:
                selected_upload_url = uploaded_urls[0]
        if len(gallery) == 1:
            primary_url = gallery[0]
        elif selected_existing and selected_existing in gallery:
            primary_url = selected_existing
        elif selected_upload_url and selected_upload_url in gallery:
            primary_url = selected_upload_url
        else:
            primary_url = gallery[0] if gallery else ""
        gallery = reorder_media_with_primary(gallery, primary_url)
        item.photo_urls = dump_media_urls(gallery)
        item.has_photo = bool(gallery)
        ensure_draft_barcode(db, item)
        lookup = build_mobile_lookup_payload(db, item.part_number)
        item.existing_stocks_json = json.dumps(lookup.get("stocks", []), ensure_ascii=False)
        item.updated_at = now()
        db.commit()
        flash("Позицію чернетки оновлено.", "success")
        return redirect(url_for("admin_receiving", warehouse_id=return_scope))
    finally:
        db.close()


@app.route("/admin/receiving/<int:item_id>/delete", methods=["POST"])
@admin_required
def admin_receiving_delete(item_id):
    db = SessionLocal()
    try:
        item = db.get(ReceivingDraftItem, item_id)
        if not item:
            flash("Позицію чернетки не знайдено.", "error")
            return redirect(url_for("admin_receiving"))
        return_scope, _ = parse_receiving_scope(request.form.get("return_scope"))
        db.delete(item)
        db.commit()
        flash("Позицію чернетки видалено.", "success")
        return redirect(url_for("admin_receiving", warehouse_id=return_scope))
    finally:
        db.close()


@app.route("/admin/api", methods=["GET", "POST"])
@admin_required
def admin_api():
    db = SessionLocal()
    try:
        if request.method == "POST":
            for key in ["nova_poshta_api_key", "telegram_bot_token", "telegram_chat_id"]:
                value = request.form.get(key, "").strip()
                row = db.query(ApiSetting).filter(ApiSetting.setting_key == key).one_or_none()
                if row:
                    row.setting_value = value
                    row.updated_at = now()
                else:
                    db.add(ApiSetting(setting_key=key, setting_value=value, updated_at=now()))
            flash_news(db, "api", "Налаштування API оновлено", "Оновлено ключ Nova Poshta і параметри Telegram.", "info")
            db.commit()
            flash("Налаштування API збережені.", "success")
            return redirect(url_for("admin_api"))
        settings = get_api_settings_map(db)
        telegram_status = telegram_connection_status(db)
        recent_telegram_chats = telegram_recent_chats(db)
        news = db.query(NewsFeed).order_by(desc(NewsFeed.created_at)).limit(12).all()
        return render_template(
            "admin_api.html",
            settings=settings,
            news=news,
            telegram_status=telegram_status,
            recent_telegram_chats=recent_telegram_chats,
        )
    finally:
        db.close()


@app.route("/admin/application/request/create", methods=["POST"])
@admin_required
def admin_application_request_create():
    db = SessionLocal()
    try:
        warehouse_id = int(request.form.get("warehouse_id") or 0)
        items_payload = json_loads_safe(request.form.get("items_json"), [])
        title = request.form.get("title", "").strip()
        warehouse = db.get(Warehouse, warehouse_id)
        if not warehouse:
            flash("Оберіть склад для запиту", "error")
            return redirect(url_for("admin_application"))
        request_obj = create_availability_request_from_payload(db, warehouse_id, title, items_payload)
        if not request_obj.items:
            db.rollback()
            flash("Додайте хоча б одну позицію до запиту", "error")
            return redirect(url_for("admin_application"))
        flash_news(
            db,
            "application",
            "Створено запит на наявність",
            f"Запит #{request_obj.id} для складу {warehouse.name}: {len(request_obj.items)} позицій.",
            "info",
        )
        db.commit()
        flash("Запит на наявність створено", "success")
        return redirect(url_for("admin_application"))
    finally:
        db.close()


@app.route("/admin/application")
@admin_required
def admin_application():
    db = SessionLocal()
    try:
        notifications = db.query(AppNotification).filter(AppNotification.status == "open").order_by(desc(AppNotification.created_at)).all()
        cards = []
        for item in notifications:
            part = db.get(Part, item.part_id)
            warehouse = db.get(Warehouse, item.warehouse_id)
            cards.append({
                "id": item.id,
                "reason": normalize_text(item.reason),
                "currentQty": int(item.current_qty or 0),
                "enteredQty": int(item.entered_qty or 0),
                "createdAt": format_dt(item.created_at),
                "part": serialize_part_card(db, part) if part else None,
                "warehouseName": warehouse.name if warehouse else "",
            })
        requests_list = (
            db.query(AvailabilityRequest)
            .filter(AvailabilityRequest.status != "deleted")
            .order_by(desc(AvailabilityRequest.created_at))
            .all()
        )
        request_cards = [serialize_availability_request(db, item) for item in requests_list]
        warehouses = db.query(Warehouse).order_by(Warehouse.name.asc()).all()
        news = db.query(NewsFeed).order_by(desc(NewsFeed.created_at)).limit(12).all()
        return render_template(
            "admin_application.html",
            notifications=cards,
            availability_requests=request_cards,
            warehouses=warehouses,
            news=news,
        )
    finally:
        db.close()


@app.route("/admin/application/<int:notification_id>/resolve", methods=["POST"])
@admin_required
def admin_application_resolve(notification_id):
    db = SessionLocal()
    try:
        notification = db.get(AppNotification, notification_id)
        if not notification:
            flash("Повідомлення не знайдено", "error")
            return redirect(url_for("admin_application"))
        part = db.get(Part, notification.part_id)
        if not part:
            db.delete(notification)
            db.commit()
            flash("Товар уже відсутній, повідомлення прибрано", "success")
            return redirect(url_for("admin_application"))
        qty = int(request.form.get("qty") or part.qty or 0)
        before_qty = int(part.qty or 0)
        part.qty = qty
        part.in_stock = qty > 0
        part.stock_checked_at = now()
        part.stock_check_status = "updated" if qty != int(notification.current_qty or 0) else "checked_ok"
        part.stock_check_note = ""
        part.updated_at = now()
        clear_part_notifications(db, part.id)
        queue_part_inventory_change(
            db,
            part,
            before_qty,
            context_label="Питання наявності",
            reason="Уточнення залишку через повідомлення",
        )
        flash_news(db, "application", "Уточнення залишку", f"{part.part_number}: кількість збережено як {qty}.", "success")
        db.commit()
        flash("Зміни збережено, повідомлення закрито", "success")
        return redirect(url_for("admin_application"))
    finally:
        db.close()


@app.route("/admin/application/request/<int:request_id>/restart", methods=["POST"])
@admin_required
def admin_application_request_restart(request_id):
    db = SessionLocal()
    try:
        request_obj = db.get(AvailabilityRequest, request_id)
        if not request_obj:
            flash("Запит не знайдено", "error")
            return redirect(url_for("admin_application"))
        for item in request_obj.items:
            item.checked_qty = None
            item.status = "pending"
            item.note = ""
            item.updated_at = now()
        request_obj.status = "open"
        request_obj.completed_at = None
        recalc_availability_request(request_obj)
        flash_news(db, "application", "Запит перезапущено", f"Запит #{request_obj.id} відкрито повторно.", "info")
        db.commit()
        flash("Запит знову відправлено в додаток", "success")
        return redirect(url_for("admin_application"))
    finally:
        db.close()


@app.route("/admin/application/request/<int:request_id>/apply", methods=["POST"])
@admin_required
def admin_application_request_apply(request_id):
    db = SessionLocal()
    try:
        request_obj = db.get(AvailabilityRequest, request_id)
        if not request_obj:
            flash("Запит не знайдено", "error")
            return redirect(url_for("admin_application"))
        updated_parts = 0
        for item in request_obj.items:
            if item.checked_qty is None:
                continue
            part = db.get(Part, item.part_id)
            if not part:
                continue
            qty = max(int(item.checked_qty or 0), 0)
            before_qty = int(part.qty or 0)
            part.qty = qty
            part.in_stock = qty > 0
            part.stock_checked_at = now()
            part.stock_check_status = "updated" if qty != int(item.expected_qty or 0) else "checked_ok"
            part.stock_check_note = normalize_text(item.note or "")
            part.updated_at = now()
            clear_part_notifications(db, part.id)
            queue_part_inventory_change(
                db,
                part,
                before_qty,
                context_label=f"Питання наявності → Запит #{request_obj.id}",
                reason="Застосовано перевірений залишок",
            )
            updated_parts += 1
        request_obj.status = "applied"
        request_obj.updated_at = now()
        request_obj.completed_at = request_obj.completed_at or now()
        flash_news(db, "application", "Оновлено залишки", f"По запиту #{request_obj.id} оновлено {updated_parts} позицій.", "success")
        db.commit()
        flash("Зміни по складу застосовано", "success")
        return redirect(url_for("admin_application"))
    finally:
        db.close()


@app.route("/admin/application/request/<int:request_id>/delete", methods=["POST"])
@admin_required
def admin_application_request_delete(request_id):
    db = SessionLocal()
    try:
        request_obj = db.get(AvailabilityRequest, request_id)
        if not request_obj:
            flash("Запит не знайдено", "error")
            return redirect(url_for("admin_application"))
        title = request_obj.title or f"Запит #{request_obj.id}"
        db.delete(request_obj)
        flash_news(db, "application", "Видалено запит", f"{title} видалено.", "info")
        db.commit()
        flash("Запит видалено", "success")
        return redirect(url_for("admin_application"))
    finally:
        db.close()


@app.route("/admin/barcodes/warehouse/<int:warehouse_id>", methods=["POST"])
@admin_required
def print_warehouse_barcodes(warehouse_id):
    db = SessionLocal()
    try:
        warehouse = db.get(Warehouse, warehouse_id)
        if not warehouse:
            flash("Склад не знайдено", "error")
            return redirect(url_for("admin_products"))
        qty_override = parse_label_qty_override(request.form.get("label_qty"))
        if qty_override == -1:
            flash("Вкажіть коректну кількість етикеток", "error")
            return redirect(url_for("warehouse_detail", warehouse_id=warehouse_id))
        selected_ids = [int(x) for x in request.form.getlist("part_ids") if str(x).isdigit()]
        query = (
            db.query(Part)
            .filter(Part.warehouse_id == warehouse_id, Part.is_deleted == False)
            .order_by(Part.part_number.asc())
        )
        if selected_ids:
            query = query.filter(Part.id.in_(selected_ids))
        parts = query.all()
        for part in parts:
            ensure_part_barcode(db, part)
        db.commit()
        labels = []
        for part in parts:
            copies = qty_override if qty_override is not None else label_copies(part.qty)
            for _ in range(copies):
                labels.append(
                    build_print_label(
                        headline=part.part_number,
                        title=part.name,
                        description=part.description,
                        barcode=part.barcode or "",
                        context=warehouse.name,
                    )
                )
        return render_template("print_labels.html", title=f"Р•С‚РёРєРµС‚РєРё СЃРєР»Р°РґСѓ {warehouse.name}", labels=labels)
    finally:
        db.close()


@app.route("/admin/barcodes/receiving", methods=["POST"])
@admin_required
def print_receiving_barcodes():
    db = SessionLocal()
    try:
        qty_override = parse_label_qty_override(request.form.get("label_qty"))
        if qty_override == -1:
            flash("Вкажіть коректну кількість етикеток", "error")
            return redirect(url_for("admin_receiving"))
        selected_ids = [int(x) for x in request.form.getlist("item_ids") if str(x).isdigit()]
        query = db.query(ReceivingDraftItem).order_by(desc(ReceivingDraftItem.updated_at), desc(ReceivingDraftItem.id))
        if selected_ids:
            query = query.filter(ReceivingDraftItem.id.in_(selected_ids))
        items = query.all()
        for item in items:
            ensure_draft_barcode(db, item)
        db.commit()
        labels = []
        for item in items:
            copies = qty_override if qty_override is not None else label_copies(item.qty)
            for _ in range(copies):
                labels.append(
                    build_print_label(
                        headline=item.part_number,
                        title=item.title,
                        description=item.description,
                        barcode=item.barcode or "",
                        context="Прийомка",
                    )
                )
        return render_template("print_labels.html", title="Етикетки прийомки", labels=labels)
    finally:
        db.close()


@app.route("/admin/import/preview", methods=["POST"])
@admin_required
def import_preview():
    file = request.files.get("file")
    warehouse_id = int(request.form.get("warehouse_id") or 0)
    if not file or not warehouse_id:
        flash("РћР±РµСЂС–С‚СЊ СЃРєР»Р°Рґ С– CSV С„Р°Р№Р»", "error")
        return redirect(url_for("admin_products"))
    db = SessionLocal()
    try:
        warehouse = db.get(Warehouse, warehouse_id)
        if not warehouse:
            flash("РЎРєР»Р°Рґ РЅРµ Р·РЅР°Р№РґРµРЅРѕ", "error")
            return redirect(url_for("admin_products"))
        rows = parse_avtopro_csv(file)
        session_row = ImportSession(warehouse_id=warehouse_id, file_name=file.filename or "import.csv", created_at=now())
        db.add(session_row)
        db.commit()
        db.refresh(session_row)
        existing = {p.part_number: p for p in db.query(Part).filter(Part.warehouse_id == warehouse_id).all()}
        new_rows = changed_rows = same_rows = 0
        for item in rows:
            prev = existing.get(item["part_number"])
            if not prev:
                ctype = "new"
                apply_change = True
                new_rows += 1
                before_price = before_qty = before_stock = None
            else:
                before_price = float(prev.price_usd)
                before_qty = int(prev.qty)
                before_stock = bool(prev.in_stock)
                changed = before_price != item["price_usd"] or before_qty != item["qty"] or before_stock != item["in_stock"]
                ctype = "changed" if changed else "same"
                apply_change = changed
                changed_rows += int(changed)
                same_rows += int(not changed)
            db.add(ImportChange(
                import_session_id=session_row.id,
                part_number=item["part_number"],
                change_type=ctype,
                before_price=before_price,
                after_price=item["price_usd"],
                before_qty=before_qty,
                after_qty=item["qty"],
                before_stock=before_stock,
                after_stock=item["in_stock"],
                apply_change=apply_change,
                payload_json=json.dumps(item, ensure_ascii=False),
                created_at=now()
            ))
        session_row.total_rows = len(rows)
        session_row.new_rows = new_rows
        session_row.changed_rows = changed_rows
        session_row.same_rows = same_rows
        flash_news(db, "import", "Сформовано preview імпорту", f"{warehouse.name}: нових {new_rows}, змінених {changed_rows}.", "info")
        db.commit()
        return redirect(url_for("import_review", session_id=session_row.id))
    except Exception as e:
        db.rollback()
        flash(f"РџРѕРјРёР»РєР° РїРѕСЂС–РІРЅСЏРЅРЅСЏ С–РјРїРѕСЂС‚Сѓ: {e}", "error")
        return redirect(url_for("admin_products"))
    finally:
        db.close()


@app.route("/admin/import/review/<int:session_id>")
@admin_required
def import_review(session_id):
    db = SessionLocal()
    try:
        only = request.args.get("only", "diff")
        session_row = db.get(ImportSession, session_id)
        changes = db.query(ImportChange).filter(ImportChange.import_session_id == session_id).all()
        if only == "diff":
            changes = [c for c in changes if c.change_type in ("changed", "new")]
        elif only == "new":
            changes = [c for c in changes if c.change_type == "new"]
        elif only == "changed":
            changes = [c for c in changes if c.change_type == "changed"]
        elif only == "same":
            changes = [c for c in changes if c.change_type == "same"]
        priority = {"changed": 0, "new": 1, "same": 2}
        changes = sorted(changes, key=lambda c: (priority.get(c.change_type, 9), c.part_number or ""))
        all_changes = db.query(ImportChange).filter(ImportChange.import_session_id == session_id).all()
        summary = {
            "all": len(all_changes),
            "new": len([c for c in all_changes if c.change_type == "new"]),
            "changed": len([c for c in all_changes if c.change_type == "changed"]),
            "same": len([c for c in all_changes if c.change_type == "same"]),
            "selected": len([c for c in all_changes if c.apply_change]),
        }
        news = db.query(NewsFeed).order_by(desc(NewsFeed.created_at)).limit(12).all()
        return render_template("import_review.html", session=session_row, changes=changes, only=only, summary=summary, news=news)
    finally:
        db.close()


@app.route("/admin/import/toggle-ajax/<int:change_id>", methods=["POST"])
@admin_required
def toggle_import_change_ajax(change_id):
    db = SessionLocal()
    try:
        change = db.get(ImportChange, change_id)
        if not change:
            return jsonify({"ok": False, "error": "Change not found"}), 404
        payload = request.get_json(silent=True) or {}
        change.apply_change = bool(payload.get("apply"))
        db.commit()
        all_changes = db.query(ImportChange).filter(ImportChange.import_session_id == change.import_session_id).all()
        summary = {
            "all": len(all_changes),
            "new": len([c for c in all_changes if c.change_type == "new"]),
            "changed": len([c for c in all_changes if c.change_type == "changed"]),
            "same": len([c for c in all_changes if c.change_type == "same"]),
            "selected": len([c for c in all_changes if c.apply_change]),
        }
        return jsonify({"ok": True, "apply_change": change.apply_change, "summary": summary})
    finally:
        db.close()


@app.route("/admin/import/bulk-ajax/<int:session_id>", methods=["POST"])
@admin_required
def bulk_import_action_ajax(session_id):
    payload = request.get_json(silent=True) or {}
    action = payload.get("action", "")
    db = SessionLocal()
    try:
        changes = db.query(ImportChange).filter(ImportChange.import_session_id == session_id).all()
        if action == "select_changed":
            for c in changes:
                if c.change_type == "changed":
                    c.apply_change = True
        elif action == "select_new":
            for c in changes:
                if c.change_type == "new":
                    c.apply_change = True
        elif action == "select_diff":
            for c in changes:
                if c.change_type in ("changed", "new"):
                    c.apply_change = True
        elif action == "clear_same":
            for c in changes:
                if c.change_type == "same":
                    c.apply_change = False
        elif action == "clear_all":
            for c in changes:
                c.apply_change = False
        db.commit()
        summary = {
            "all": len(changes),
            "new": len([c for c in changes if c.change_type == "new"]),
            "changed": len([c for c in changes if c.change_type == "changed"]),
            "same": len([c for c in changes if c.change_type == "same"]),
            "selected": len([c for c in changes if c.apply_change]),
        }
        changed_map = {c.id: c.apply_change for c in changes}
        return jsonify({"ok": True, "summary": summary, "changed_map": changed_map})
    finally:
        db.close()


@app.route("/admin/import/confirm/<int:session_id>", methods=["POST"])
@admin_required
def confirm_import(session_id):
    action = request.form.get("action", "selected")
    db = SessionLocal()
    try:
        session_row = db.get(ImportSession, session_id)
        changes = db.query(ImportChange).filter(ImportChange.import_session_id == session_id).all()
        for c in changes:
            qty_key = f"after_qty_{c.id}"
            if qty_key in request.form:
                try:
                    qty_value = max(int(float(request.form.get(qty_key) or 0)), 0)
                except Exception:
                    qty_value = int(c.after_qty or 0)
                c.after_qty = qty_value
                c.after_stock = qty_value > 0
                payload = json.loads(c.payload_json)
                payload["qty"] = qty_value
                payload["in_stock"] = qty_value > 0
                c.payload_json = json.dumps(payload, ensure_ascii=False)
        if action == "all":
            for c in changes:
                if c.change_type != "same":
                    c.apply_change = True
        elif action == "skip_all":
            for c in changes:
                c.apply_change = False
        applied = 0
        for c in changes:
            if not c.apply_change:
                continue
            payload = json.loads(c.payload_json)
            part = db.query(Part).filter(Part.warehouse_id == session_row.warehouse_id, Part.part_number == c.part_number).one_or_none()
            if part:
                part.is_deleted = False
                part.deleted_at = None
                part.brand = payload["brand"]
                part.producer_type = producer_type_label(payload["producer_type"])
                part.name = payload["name"]
                part.price_usd = payload["price_usd"]
                part.qty = payload["qty"]
                part.in_stock = payload["in_stock"]
                gallery_payload = parse_media_urls(payload.get("showcase_photo_urls") or [])
                if payload.get("photo_urls"):
                    part.photo_urls = payload["photo_urls"]
                if gallery_payload:
                    part.showcase_photo_urls = dump_media_urls(gallery_payload)
                elif payload.get("photo_urls"):
                    part.showcase_photo_urls = dump_media_urls(part_gallery_urls(part) + [payload["photo_urls"]])
                part.has_photo = bool(part_gallery_urls(part))
                part.brand_export = payload["brand_export"]
                part.part_number_export = payload["part_number_export"]
                part.avtopro_flag_1 = payload["avtopro_flag_1"]
                part.avtopro_flag_2 = payload["avtopro_flag_2"]
                part.avtopro_flag_3 = payload["avtopro_flag_3"]
                part.avtopro_flag_4 = payload["avtopro_flag_4"]
                part.raw_import_row = payload["raw_import_row"]
                ensure_part_barcode(db, part)
                sync_template_from_part(db, part)
                part.updated_at = now()
            else:
                new_part = Part(
                    warehouse_id=session_row.warehouse_id,
                    part_number=payload["part_number"],
                    brand=payload["brand"],
                    producer_type=producer_type_label(payload["producer_type"]),
                    name=payload["name"],
                    description="",
                    price_usd=payload["price_usd"],
                    qty=payload["qty"],
                    in_stock=payload["in_stock"],
                    photo_urls=payload["photo_urls"],
                    showcase_photo_urls=payload.get("showcase_photo_urls") or dump_media_urls([payload["photo_urls"]] if payload["photo_urls"] else []),
                    has_photo=payload.get("has_photo", False),
                    has_description=False,
                    views_24h=payload["views_24h"],
                    views_168h=payload["views_168h"],
                    brand_export=payload["brand_export"],
                    part_number_export=payload["part_number_export"],
                    avtopro_flag_1=payload["avtopro_flag_1"],
                    avtopro_flag_2=payload["avtopro_flag_2"],
                    avtopro_flag_3=payload["avtopro_flag_3"],
                    avtopro_flag_4=payload["avtopro_flag_4"],
                    raw_import_row=payload["raw_import_row"],
                    created_at=now(),
                    updated_at=now(),
                )
                db.add(new_part)
                ensure_part_barcode(db, new_part)
                sync_template_from_part(db, new_part)
            applied += 1
        session_row.status = "confirmed"
        warehouse = db.get(Warehouse, session_row.warehouse_id)
        flash_news(db, "import", "Прийнято зміни на склад", f"{warehouse.name}: застосовано {applied} змін.", "success")
        db.commit()
        flash(f"РџСЂРёР№РЅСЏС‚Рѕ РЅР° СЃРєР»Р°Рґ: {applied} Р·РјС–РЅ", "success")
        return redirect(url_for("warehouse_detail", warehouse_id=session_row.warehouse_id))
    finally:
        db.close()


@app.route("/admin/export/<int:warehouse_id>")
@admin_required
def export_warehouse(warehouse_id):
    db = SessionLocal()
    try:
        warehouse = db.get(Warehouse, warehouse_id)
        output = io.StringIO()
        writer = csv.writer(output, delimiter=";")
        for p in (
            db.query(Part)
            .filter(Part.warehouse_id == warehouse_id, Part.is_deleted == False)
            .order_by(Part.part_number.asc())
            .all()
        ):
            template = find_part_template(db, p.part_number)
            export_brand = (template.brand if template and template.brand else p.brand) or ""
            export_name = (template.name if template and template.name else p.name) or ""
            export_price = float(template.price_usd if template and template.price_usd is not None else p.price_usd or 0)
            export_photo = (template.photo_urls if template and template.photo_urls else p.photo_urls) or ""
            export_has_photo = "1" if export_photo else "0"
            writer.writerow([
                export_brand,
                p.part_number or "",
                export_name,
                f"{export_price:.0f}",
                p.qty or 0,
                p.avtopro_flag_1 or "0",
                export_photo,
                p.brand_export or export_brand,
                p.part_number_export or p.part_number or "",
                "",
                p.avtopro_flag_2 or "1",
                "",
                "",
                "",
                "",
                p.avtopro_flag_3 or "0",
                p.avtopro_flag_4 or export_has_photo,
            ])
        mem = io.BytesIO(output.getvalue().encode("utf-8-sig"))
        return send_file(mem, mimetype="text/csv", as_attachment=True, download_name=f"{warehouse.name}-export.csv")
    finally:
        db.close()


@app.route("/admin/warehouse/<int:warehouse_id>/print-list")
@admin_required
def print_warehouse_list(warehouse_id):
    db = SessionLocal()
    try:
        warehouse = db.get(Warehouse, warehouse_id)
        if not warehouse:
            flash("Склад не знайдено", "error")
            return redirect(url_for("admin_products"))
        scope = str(warehouse_id)
        rows = build_warehouse_print_picker_rows(db, scope)
        printed_at = now()
        touch_warehouse_print_marks(db, scope, [row["part_number"] for row in rows], printed_at=printed_at)
        db.commit()

        return render_template(
            "print_warehouse_list.html",
            scope_title=warehouse.name,
            rows=rows,
            generated_at=format_dt(printed_at),
        )
    finally:
        db.close()


@app.route("/admin/warehouse/print-list", methods=["POST"])
@admin_required
def print_selected_warehouse_list():
    db = SessionLocal()
    try:
        scope = normalize_text(request.form.get("scope") or "").strip().lower()
        query_text = request.form.get("q", "").strip()
        selected_numbers = request.form.getlist("part_numbers")
        if scope != "all":
            warehouses = db.query(Warehouse).order_by(Warehouse.name.asc()).all()
            valid_ids = {str(item.id) for item in warehouses}
            scope = scope if scope in valid_ids else (str(warehouses[0].id) if warehouses else "all")

        if not selected_numbers:
            flash("Оберіть хоча б одну позицію для друку", "error")
            return redirect(url_for("admin_warehouse_print", scope=scope, q=query_text))

        rows = build_warehouse_print_picker_rows(db, scope, "", selected_numbers=selected_numbers)
        if not rows:
            flash("Обрані позиції не знайдено", "error")
            return redirect(url_for("admin_warehouse_print", scope=scope, q=query_text))

        printed_at = now()
        touch_warehouse_print_marks(db, scope, [row["part_number"] for row in rows], printed_at=printed_at)
        db.commit()
        return render_template(
            "print_warehouse_list.html",
            scope_title=warehouse_print_scope_label(db, scope),
            rows=rows,
            generated_at=format_dt(printed_at),
        )
    finally:
        db.close()


@app.route("/admin/revision/<int:warehouse_id>", methods=["GET", "POST"])
@admin_required
def revision(warehouse_id):
    db = SessionLocal()
    try:
        warehouse = db.get(Warehouse, warehouse_id)
        parts = (
            db.query(Part)
            .filter(Part.warehouse_id == warehouse_id, Part.is_deleted == False)
            .order_by(Part.id.asc())
            .all()
        )

        if not parts:
            flash("РЈ СЃРєР»Р°РґС– РЅРµРјР°С” С‚РѕРІР°СЂС–РІ", "error")
            return redirect(url_for("warehouse_detail", warehouse_id=warehouse_id))

        total = len(parts)
        action = (request.args.get("action") or "").strip()
        if action == "restart":
            warehouse.revision_current_index = 0
            warehouse.revision_percent = 0
            warehouse.revision_status = "not_started"
            warehouse.revision_started_at = now()
            warehouse.revision_date = None
            warehouse.updated_at = now()
            db.commit()
            return redirect(url_for("revision", warehouse_id=warehouse_id, idx=0))
        if action == "continue":
            next_idx = min(int(warehouse.revision_current_index or 0), max(total - 1, 0))
            return redirect(url_for("revision", warehouse_id=warehouse_id, idx=next_idx, flow=1))

        if request.method == "POST":
            idx = int(request.args.get("idx", 0))
            part = parts[idx]

            availability = request.form.get("availability")
            before_qty = int(part.qty or 0)
            if availability == "no":
                part.qty = 0
                part.in_stock = False
            else:
                part.qty = int(request.form.get("qty", 0))
                part.in_stock = part.qty > 0

            upload_url = save_upload(request.files.get("photo"), f"revision_{part.id}")
            if upload_url:
                part.photo_urls = upload_url
                part.has_photo = True

            part.updated_at = now()
            queue_part_inventory_change(
                db,
                part,
                before_qty,
                context_label=f"Ревізія → {warehouse.name}",
                reason="Оновлено під час ревізії складу",
            )

            next_idx = idx + 1

            # Р·Р°РІРµСЂС€РµРЅРѕ
            if next_idx >= total:
                warehouse.revision_status = "completed"
                warehouse.revision_percent = 100
                warehouse.revision_date = now()
                db.commit()
                flash("Р РµРІС–Р·С–СЋ Р·Р°РІРµСЂС€РµРЅРѕ", "success")
                return redirect(url_for("warehouse_detail", warehouse_id=warehouse_id))

            # РїРµСЂРµС…С–Рґ РґРѕ РЅР°СЃС‚СѓРїРЅРѕРіРѕ
            warehouse.revision_status = "in_progress"
            warehouse.revision_percent = int((next_idx / total) * 100)
            db.commit()

            return redirect(url_for("revision", warehouse_id=warehouse_id, idx=next_idx, flow=1))

        idx = int(request.args.get("idx", 0))
        part = parts[idx]

        show_resume_prompt = False  # Р’Р†Р”РљР›Р®Р§РђР„РњРћ Р“Р›Р®Рљ

        return render_template(
            "revision.html",
            warehouse=warehouse,
            part=part,
            idx=idx,
            total=total,
            progress=warehouse.revision_percent or 0,
            safe_photo=safe_photo,
            display_usd=display_usd,
            display_uah=display_uah,
            show_resume_prompt=(
                warehouse.revision_status == "in_progress"
                and not request.args.get("flow")
                and int(warehouse.revision_current_index or 0) > 0
            )
        )
    finally:
        db.close()


@app.route("/api/mobile/status")
def api_mobile_status():
    db = SessionLocal()
    try:
        warehouses_total = db.query(Warehouse).count()
        draft_total = db.query(ReceivingDraftItem).count()
        transit_open_count = len(mobile_transit_orders(db))
        availability_open_count = (
            db.query(AvailabilityRequest)
            .filter(AvailabilityRequest.status.in_(["open", "in_progress"]))
            .count()
        )
        packing_open_count = (
            db.query(PackingRequest)
            .filter(PackingRequest.status.in_(["open", "in_progress", "ready", "issue", "packed"]))
            .count()
        )
        return jsonify({
            "ok": True,
            "api": "up",
            "warehousesTotal": warehouses_total,
            "draftTotal": draft_total,
            "transitOpenCount": transit_open_count,
            "availabilityOpenCount": availability_open_count,
            "packingOpenCount": packing_open_count,
            "timestamp": now().isoformat(),
        })
    finally:
        db.close()


@app.route("/api/mobile/warehouses")
def api_mobile_warehouses():
    db = SessionLocal()
    try:
        warehouses = db.query(Warehouse).order_by(Warehouse.name.asc()).all()
        payload = []
        for w in warehouses:
            stats = recalc_warehouse_stats(w)
            payload.append({
                "id": w.id,
                "name": w.name,
                "qtyTotal": stats["total"],
                "revisionStatus": w.revision_status,
                "revisionPercent": int(w.revision_percent or 0),
            })
        db.commit()
        return jsonify(payload)
    finally:
        db.close()

@app.route("/api/mobile/parts/lookup")
def api_mobile_part_lookup():
    db = SessionLocal()
    try:
        part_number = request.args.get("partNumber", "").strip()
        payload = build_mobile_lookup_payload(db, part_number)
        db.commit()
        return jsonify(payload)
    finally:
        db.close()

@app.route("/api/mobile/receiving/draft-legacy")
def api_mobile_receiving_draft_legacy():
    draft = session.get("mobile_receiving_draft", [])
    selected_warehouse_id = session.get("mobile_receiving_selected_warehouse_id")
    return jsonify({
        "selectedWarehouseId": selected_warehouse_id,
        "items": draft,
    })

@app.route("/api/mobile/receiving/items-legacy", methods=["POST"])
def api_mobile_receiving_add_item_legacy():
    db = SessionLocal()
    try:
        payload = request.get_json(silent=True) or {}
        draft = session.get("mobile_receiving_draft", [])
        lookup = mobile_lookup_part_stock(db, payload.get("partNumber", ""))
        next_id = max([int(x.get("id", 0)) for x in draft], default=0) + 1
        item = {
            "id": next_id,
            "partNumber": (payload.get("partNumber") or "").strip().upper(),
            "title": (payload.get("title") or "").strip(),
            "qty": int(payload.get("qty") or 1),
            "priceUsd": float(payload.get("priceUsd") or 0),
            "description": (payload.get("description") or "").strip(),
            "photoUri": payload.get("photoUri"),
            "existingStocks": lookup.get("stocks", []),
        }
        draft.append(item)
        session["mobile_receiving_draft"] = draft
        return jsonify({
            "selectedWarehouseId": session.get("mobile_receiving_selected_warehouse_id"),
            "items": draft,
        })
    finally:
        db.close()

@app.route("/api/mobile/receiving/import-legacy", methods=["POST"])
def api_mobile_receiving_import_legacy():
    db = SessionLocal()
    try:
        warehouse_id = int(request.args.get("warehouseId") or 0)
        warehouse = db.get(Warehouse, warehouse_id)
        if not warehouse:
            return jsonify({"error": "warehouse_not_found"}), 404

        draft = session.get("mobile_receiving_draft", [])
        imported = 0
        updated = 0

        for item in draft:
            part_number = (item.get("partNumber") or "").strip()
            existing = db.query(Part).filter(Part.part_number == part_number).order_by(Part.id.asc()).first()
            if existing:
                existing.qty = int(existing.qty or 0) + int(item.get("qty") or 0)
                existing.in_stock = existing.qty > 0
                if item.get("description"):
                    existing.description = ((existing.description or "").strip() + "\\n" + item.get("description", "").strip()).strip()
                    existing.has_description = bool(existing.description)
                existing.producer_type = producer_type_label(existing.producer_type or "OEM")
                existing.updated_at = now()
                sync_template_from_part(db, existing)
                updated += 1
            else:
                new_part = Part(
                    warehouse_id=warehouse_id,
                    part_number=part_number,
                    brand="",
                    producer_type=producer_type_label("OEM"),
                    name=(item.get("title") or "").strip(),
                    description=(item.get("description") or "").strip(),
                    price_usd=float(item.get("priceUsd") or 0),
                    qty=int(item.get("qty") or 0),
                    in_stock=int(item.get("qty") or 0) > 0,
                    photo_urls="",
                    has_photo=False,
                    has_description=bool((item.get("description") or "").strip()),
                    brand_export="",
                    part_number_export=part_number,
                    created_at=now(),
                    updated_at=now(),
                )
                db.add(new_part)
                ensure_part_barcode(db, new_part)
                sync_template_from_part(db, new_part)
                imported += 1

        flash_news(db, "receiving", "РџСЂРёР№РѕРј С‚РѕРІР°СЂСѓ", f"Р†РјРїРѕСЂС‚РѕРІР°РЅРѕ: {imported}, РѕРЅРѕРІР»РµРЅРѕ: {updated} Сѓ СЃРєР»Р°Рґ {warehouse.name}.", "success")
        db.commit()
        session["mobile_receiving_draft"] = []
        session["mobile_receiving_selected_warehouse_id"] = warehouse_id
        return jsonify({
            "selectedWarehouseId": warehouse_id,
            "items": [],
            "imported": imported,
            "updated": updated,
        })
    finally:
        db.close()

@app.route("/api/mobile/receiving/draft")
def api_mobile_receiving_draft():
    db = SessionLocal()
    try:
        warehouse_id = int(request.args.get("warehouseId") or 0)
        items = get_receiving_draft_items(db, warehouse_id or None)
        payload = [serialize_receiving_item(db, item) for item in items]
        return jsonify({
            "selectedWarehouseId": warehouse_id or (payload[0]["warehouseId"] if payload else None),
            "items": payload,
        })
    finally:
        db.close()


@app.route("/api/mobile/receiving/items", methods=["POST"])
def api_mobile_receiving_add_item():
    db = SessionLocal()
    try:
        payload = request.get_json(silent=True) or {}
        form = request.form if request.form else payload
        warehouse_id = int(form.get("warehouseId") or 0) or None
        warehouse = db.get(Warehouse, warehouse_id) if warehouse_id else None
        if form.get("warehouseId") and warehouse_id and not warehouse:
            return jsonify({"error": "warehouse_not_found"}), 404

        part_number = (form.get("partNumber") or "").strip().upper()
        if not part_number:
            return jsonify({"error": "part_number_required"}), 400

        lookup = build_mobile_lookup_payload(db, part_number)
        title = (form.get("title") or lookup.get("suggestedTitle") or part_number).strip()
        qty_value = max(int(form.get("qty") or 1), 1)
        price_value = float(form.get("priceUsd") or 0)
        description_value = (form.get("description") or "").strip()

        duplicate = find_recent_mobile_draft_duplicate(
            db=db,
            warehouse_id=warehouse_id,
            part_number=part_number,
            title=title,
            qty=qty_value,
            description=description_value,
        )
        if duplicate:
            duplicate.existing_stocks_json = json.dumps(lookup.get("stocks", []), ensure_ascii=False)
            duplicate.updated_at = now()
            ensure_draft_barcode(db, duplicate)
            db.commit()
            items = [serialize_receiving_item(db, row) for row in get_receiving_draft_items(db, warehouse_id)]
            return jsonify({
                "selectedWarehouseId": warehouse_id,
                "items": items,
            })

        upload_urls = save_uploads(request.files.getlist("photo"), f"receiving_mobile_{warehouse_id or 'draft'}")
        upload_payload = dump_media_urls(upload_urls)
        existing_query = db.query(Part).filter(Part.part_number == part_number, Part.is_deleted == False)
        if warehouse_id:
            existing_query = existing_query.filter(Part.warehouse_id == warehouse_id)
        existing_part = existing_query.order_by(desc(Part.updated_at), Part.id.asc()).first()
        item = ReceivingDraftItem(
            warehouse_id=warehouse_id,
            part_number=part_number,
            barcode=(existing_part.barcode if existing_part else ""),
            title=title,
            qty=qty_value,
            price_usd=price_value,
            description=description_value,
            photo_urls=upload_payload,
            has_photo=bool(upload_urls),
            existing_stocks_json=json.dumps(lookup.get("stocks", []), ensure_ascii=False),
            source="mobile",
            created_at=now(),
            updated_at=now(),
        )
        db.add(item)
        ensure_draft_barcode(db, item)
        db.commit()
        items = [serialize_receiving_item(db, row) for row in get_receiving_draft_items(db, warehouse_id)]
        return jsonify({
            "selectedWarehouseId": warehouse_id,
            "items": items,
        })
    finally:
        db.close()


@app.route("/api/mobile/receiving/items/<int:item_id>", methods=["DELETE", "POST"])
def api_mobile_receiving_delete_item(item_id):
    db = SessionLocal()
    try:
        item = db.get(ReceivingDraftItem, item_id)
        if not item:
            return jsonify({"error": "draft_item_not_found"}), 404
        warehouse_id = item.warehouse_id
        db.delete(item)
        db.commit()
        items = [serialize_receiving_item(db, row) for row in get_receiving_draft_items(db, warehouse_id)]
        return jsonify({
            "selectedWarehouseId": warehouse_id,
            "items": items,
        })
    finally:
        db.close()


@app.route("/api/mobile/receiving/import", methods=["POST"])
def api_mobile_receiving_import():
    db = SessionLocal()
    try:
        warehouse_id = int(request.args.get("warehouseId") or 0)
        items = get_receiving_draft_items(db, warehouse_id or None)
        if warehouse_id:
            items = [item for item in items if item.warehouse_id == warehouse_id]
        if not items:
            return jsonify({"error": "draft_empty"}), 404

        imported, updated, warehouse, template_created, template_updated = import_receiving_items(db, items, warehouse_id or None)
        for item in items:
            db.delete(item)
        db.commit()
        return jsonify({
            "selectedWarehouseId": warehouse.id if warehouse else None,
            "items": [],
            "imported": imported,
            "updated": updated,
            "templateCreated": template_created,
            "templateUpdated": template_updated,
        })
    except ValueError:
        return jsonify({"error": "warehouse_not_found"}), 404
    finally:
        db.close()


@app.route("/api/mobile/transit/current")
def api_mobile_transit_current():
    db = SessionLocal()
    try:
        payload = mobile_transit_payload(db)
        db.commit()
        return jsonify(payload)
    finally:
        db.close()


@app.route("/api/mobile/transit/scan", methods=["POST"])
def api_mobile_transit_scan():
    db = SessionLocal()
    try:
        payload = request.get_json(silent=True) or {}
        barcode = re.sub(r"\D", "", payload.get("barcode", "") or "")
        if len(barcode) != 8:
            return jsonify({"error": "barcode_required"}), 400

        orders = mobile_transit_orders(db)
        matched = next(
            (
                order
                for order in orders
                if (order.barcode or "").strip() == barcode and int(order.accepted_qty or 0) < int(order.qty or 0)
            ),
            None,
        )
        if not matched:
            already_done = next(
                (order for order in orders if (order.barcode or "").strip() == barcode),
                None,
            )
            if already_done:
                payload = mobile_transit_payload(db)
                return jsonify({
                    **payload,
                    "found": True,
                    "message": "Цю позицію вже прийнято повністю.",
                    "item": serialize_transit_order(db, already_done),
                })
            payload = mobile_transit_payload(db)
            return jsonify({
                **payload,
                "found": False,
                "message": "Товар не знайдено.",
                "item": None,
            })

        matched.accepted_qty = min(int(matched.qty or 0), int(matched.accepted_qty or 0) + 1)
        normalize_transit_order_progress(db, matched)
        maybe_queue_transit_arrival_message(db, orders)
        db.commit()

        payload = mobile_transit_payload(db)
        return jsonify({
            **payload,
            "found": True,
            "message": f"Прийнято {matched.accepted_qty}/{int(matched.qty or 0)}",
            "item": serialize_transit_order(db, matched),
        })
    finally:
        db.close()


@app.route("/api/mobile/transit/finish", methods=["POST"])
def api_mobile_transit_finish():
    db = SessionLocal()
    try:
        orders = mobile_transit_orders(db)
        newly_accepted = [
            order for order in orders
            if max(int(order.accepted_qty or 0), 0) > max(int(order.arrival_notified_qty or 0), 0)
        ]
        if newly_accepted:
            if not maybe_queue_transit_arrival_message(db, orders):
                queue_telegram_message(db, build_transit_arrival_telegram_message(newly_accepted))
                for order in newly_accepted:
                    order.arrival_notified_qty = max(int(order.accepted_qty or 0), 0)
                    order.updated_at = now()
            message = "❗ Товар прибув, додайте в склад в кабінеті!!!"
        else:
            message = "Немає нових прийнятих позицій."
        db.commit()
        payload = mobile_transit_payload(db)
        return jsonify({
            **payload,
            "found": bool(newly_accepted),
            "message": message,
            "item": None,
        })
    finally:
        db.close()


@app.route("/api/mobile/revision/<int:warehouse_id>")
def api_mobile_revision_status(warehouse_id):
    db = SessionLocal()
    try:
        warehouse = db.get(Warehouse, warehouse_id)
        if not warehouse:
            return jsonify({"error": "warehouse_not_found"}), 404
        ask_resume = warehouse.revision_status == "in_progress" and int(warehouse.revision_current_index or 0) > 0
        payload = mobile_revision_payload(db, warehouse_id, ask_resume=ask_resume)
        db.commit()
        return jsonify(payload)
    finally:
        db.close()

@app.route("/api/mobile/revision/<int:warehouse_id>/resume", methods=["POST"])
def api_mobile_revision_resume(warehouse_id):
    db = SessionLocal()
    try:
        return jsonify(mobile_revision_payload(db, warehouse_id, ask_resume=False))
    finally:
        db.close()

@app.route("/api/mobile/revision/<int:warehouse_id>/restart", methods=["POST"])
def api_mobile_revision_restart(warehouse_id):
    db = SessionLocal()
    try:
        warehouse = db.get(Warehouse, warehouse_id)
        if not warehouse:
            return jsonify({"error": "warehouse_not_found"}), 404
        warehouse.revision_current_index = 0
        warehouse.revision_percent = 0
        warehouse.revision_status = "not_started"
        warehouse.revision_started_at = now()
        warehouse.revision_date = None
        warehouse.updated_at = now()
        db.commit()
        return jsonify(mobile_revision_payload(db, warehouse_id, ask_resume=False))
    finally:
        db.close()

@app.route("/api/mobile/revision/<int:warehouse_id>/submit", methods=["POST"])
def api_mobile_revision_submit(warehouse_id):
    db = SessionLocal()
    try:
        warehouse = db.get(Warehouse, warehouse_id)
        if not warehouse:
            return jsonify({"error": "warehouse_not_found"}), 404
        payload = request.get_json(silent=True) or {}
        form = request.form if request.form else payload
        parts = (
            db.query(Part)
            .filter(Part.warehouse_id == warehouse_id, Part.is_deleted == False)
            .order_by(Part.id.asc())
            .all()
        )
        if not parts:
            return jsonify({"error": "no_parts"}), 404

        part_id = int(form.get("partId") or 0)
        current = next((p for p in parts if p.id == part_id), None)
        if not current:
            return jsonify({"error": "part_not_found"}), 404

        raw_available = form.get("isAvailable")
        is_available = str(raw_available).lower() in ("1", "true", "yes", "on")
        qty = int(form.get("qty") or 0)
        if not is_available:
            current.qty = 0
            current.in_stock = False
        else:
            current.qty = qty
            current.in_stock = qty > 0
        upload_url = save_upload(request.files.get("photo"), f"revision_mobile_{current.id}")
        if upload_url:
            current.photo_urls = upload_url
            current.has_photo = True
        ensure_part_barcode(db, current)
        current.updated_at = now()

        current_idx = 0
        for idx, p in enumerate(parts):
            if p.id == current.id:
                current_idx = idx
                break

        next_idx = current_idx + 1
        warehouse.revision_started_at = warehouse.revision_started_at or now()
        warehouse.revision_current_index = min(next_idx, len(parts))
        warehouse.revision_percent = int((warehouse.revision_current_index / len(parts)) * 100) if parts else 0
        warehouse.revision_date = now()
        warehouse.revision_status = "completed" if next_idx >= len(parts) else "in_progress"
        warehouse.updated_at = now()
        db.commit()
        return jsonify(mobile_revision_payload(db, warehouse_id, ask_resume=False))
    finally:
        db.close()


@app.route("/api/mobile/verification/lookup")
def api_mobile_verification_lookup():
    db = SessionLocal()
    try:
        barcode = re.sub(r"\D", "", request.args.get("barcode", "") or "")
        if len(barcode) != 8:
            return jsonify({"error": "barcode_required"}), 400
        part = db.query(Part).filter(Part.barcode == barcode).one_or_none()
        if not part:
            return jsonify({"error": "barcode_not_found"}), 404
        ensure_part_barcode(db, part)
        db.commit()
        return jsonify(serialize_part_card(db, part))
    finally:
        db.close()


@app.route("/api/mobile/verification/check", methods=["POST"])
def api_mobile_verification_check():
    db = SessionLocal()
    try:
        payload = request.get_json(silent=True) or {}
        barcode = re.sub(r"\D", "", payload.get("barcode", "") or "")
        action = (payload.get("action") or "").strip()
        entered_qty = int(payload.get("qty") or 0)
        if len(barcode) != 8:
            return jsonify({"error": "barcode_required"}), 400
        part = db.query(Part).filter(Part.barcode == barcode).one_or_none()
        if not part:
            return jsonify({"error": "barcode_not_found"}), 404

        current_qty = int(part.qty or 0)
        if entered_qty == current_qty:
            action = "match"

        if action == "match":
            part.stock_checked_at = now()
            part.stock_check_status = "checked_ok"
            part.stock_check_note = ""
            part.updated_at = now()
            clear_part_notifications(db, part.id)
            message = "Кількість збігається зі складом"
        elif action == "accept_new":
            before_qty = int(part.qty or 0)
            part.qty = entered_qty
            part.in_stock = entered_qty > 0
            part.stock_checked_at = now()
            part.stock_check_status = "updated"
            part.stock_check_note = ""
            part.updated_at = now()
            clear_part_notifications(db, part.id)
            queue_part_inventory_change(
                db,
                part,
                before_qty,
                context_label=f"Мобільна звірка → {part.warehouse.name if part.warehouse else 'Без складу'}",
                reason="Прийнято нову кількість після сканування",
            )
            message = f"Нову кількість {entered_qty} записано в склад"
        elif action == "leave_unchanged":
            part.stock_checked_at = now()
            part.stock_check_status = "needs_clarification"
            part.stock_check_note = f"На складі {current_qty}, під час звірки введено {entered_qty}"
            part.updated_at = now()
            create_app_notification(
                db,
                part,
                entered_qty=entered_qty,
                reason=f"На складі {current_qty}, під час звірки введено {entered_qty}. Потрібне уточнення.",
            )
            message = f"Залишок лишився {current_qty}, створено повідомлення для уточнення"
        else:
            return jsonify({
                "error": "action_required",
                "expectedQty": current_qty,
                "enteredQty": entered_qty,
            }), 400

        db.commit()
        return jsonify({
            "ok": True,
            "action": action,
            "message": message,
            "part": serialize_part_card(db, part),
        })
    finally:
        db.close()


@app.route("/api/mobile/availability/current")
def api_mobile_availability_current():
    db = SessionLocal()
    try:
        request_obj = (
            db.query(AvailabilityRequest)
            .filter(AvailabilityRequest.status.in_(["open", "in_progress"]))
            .order_by(AvailabilityRequest.created_at.asc(), AvailabilityRequest.id.asc())
            .first()
        )
        if not request_obj:
            return jsonify({"ok": True, "hasRequest": False})
        payload = availability_mobile_payload(db, request_obj)
        db.commit()
        return jsonify({"ok": True, "hasRequest": True, **payload})
    finally:
        db.close()


@app.route("/api/mobile/availability/<int:request_id>/check", methods=["POST"])
def api_mobile_availability_check(request_id):
    db = SessionLocal()
    try:
        request_obj = db.get(AvailabilityRequest, request_id)
        if not request_obj:
            return jsonify({"error": "request_not_found"}), 404
        payload = request.get_json(silent=True) or request.form
        item_id = int(payload.get("itemId") or 0)
        qty = max(int(payload.get("qty") or 0), 0)
        item = db.get(AvailabilityRequestItem, item_id)
        if not item or item.request_id != request_obj.id:
            return jsonify({"error": "item_not_found"}), 404

        item.checked_qty = qty
        if qty == int(item.expected_qty or 0):
            item.status = "found"
            item.note = ""
        elif qty <= 0:
            item.status = "missing"
            item.note = f"Позицію {item.part_number} не знайдено."
        else:
            item.status = "mismatch"
            item.note = f"Очікувалось {int(item.expected_qty or 0)}, фактично знайдено {qty}."
        item.updated_at = now()

        part = db.get(Part, item.part_id)
        if part:
            part.stock_checked_at = now()
            if item.status == "found":
                part.stock_check_status = "checked_ok"
                part.stock_check_note = ""
            else:
                part.stock_check_status = "needs_clarification"
                part.stock_check_note = normalize_text(item.note or "")
            part.updated_at = now()

        recalc_availability_request(request_obj)
        db.commit()
        return jsonify({"ok": True, **availability_mobile_payload(db, request_obj)})
    finally:
        db.close()


@app.route("/api/mobile/availability/<int:request_id>/restart", methods=["POST"])
def api_mobile_availability_restart(request_id):
    db = SessionLocal()
    try:
        request_obj = db.get(AvailabilityRequest, request_id)
        if not request_obj:
            return jsonify({"error": "request_not_found"}), 404
        for item in request_obj.items:
            item.checked_qty = None
            item.status = "pending"
            item.note = ""
            item.updated_at = now()
        request_obj.status = "open"
        request_obj.completed_at = None
        recalc_availability_request(request_obj)
        db.commit()
        return jsonify({"ok": True, **availability_mobile_payload(db, request_obj)})
    finally:
        db.close()


@app.route("/api/mobile/packing/current")
def api_mobile_packing_current():
    db = SessionLocal()
    try:
        request_obj = (
            db.query(PackingRequest)
            .filter(PackingRequest.status.in_(["open", "in_progress", "ready"]))
            .order_by(PackingRequest.created_at.asc(), PackingRequest.id.asc())
            .first()
        )
        if not request_obj:
            return jsonify({"ok": True, "hasRequest": False})
        payload = packing_mobile_payload(db, request_obj)
        db.commit()
        return jsonify({"ok": True, "hasRequest": True, **payload})
    finally:
        db.close()


@app.route("/api/mobile/issue/list")
def api_mobile_issue_list():
    db = SessionLocal()
    try:
        payload = mobile_issue_payload(db)
        db.commit()
        return jsonify(payload)
    finally:
        db.close()


@app.route("/api/mobile/issue/<int:request_id>/scan", methods=["POST"])
def api_mobile_issue_scan(request_id):
    db = SessionLocal()
    try:
        request_obj = db.get(PackingRequest, request_id)
        if not request_obj or request_obj.status == "deleted":
            return jsonify({"error": "request_not_found"}), 404

        payload = request.get_json(silent=True) or {}
        barcode = re.sub(r"\D", "", payload.get("barcode", "") or "")
        if len(barcode) != 8:
            return jsonify({"error": "barcode_required"}), 400

        request_items = list(request_obj.items or [])
        matched = next(
            (
                item
                for item in request_items
                if packing_item_barcode(db, item) == barcode
                and int(item.found_qty or 0) < int(item.expected_qty or 0)
            ),
            None,
        )
        if not matched:
            already_done = next(
                (item for item in request_items if packing_item_barcode(db, item) == barcode),
                None,
            )
            base_payload = mobile_issue_payload(db)
            request_payload = serialize_issue_request(db, request_obj)
            if already_done:
                return jsonify({
                    **base_payload,
                    "found": True,
                    "message": "Цю позицію вже підтверджено повністю.",
                    "request": request_payload,
                    "item": serialize_packing_item(db, already_done),
                })
            return jsonify({
                **base_payload,
                "found": False,
                "message": "Товар не знайдено у вибраній заявці.",
                "request": request_payload,
                "item": None,
            })

        expected_qty = max(int(matched.expected_qty or 0), 0)
        matched.found_qty = min(expected_qty, int(matched.found_qty or 0) + 1)
        matched.missing_qty = max(expected_qty - int(matched.found_qty or 0), 0)
        matched.status = "found" if int(matched.found_qty or 0) >= expected_qty else "partial"
        matched.updated_at = now()
        recalc_packing_request(request_obj)
        db.commit()

        base_payload = mobile_issue_payload(db)
        return jsonify({
            **base_payload,
            "found": True,
            "message": f"Підтверджено {int(matched.found_qty or 0)}/{expected_qty}",
            "request": serialize_issue_request(db, request_obj),
            "item": serialize_packing_item(db, matched),
        })
    finally:
        db.close()


@app.route("/api/mobile/issue/<int:request_id>/complete", methods=["POST"])
def api_mobile_issue_complete(request_id):
    db = SessionLocal()
    try:
        request_obj = db.get(PackingRequest, request_id)
        if not request_obj or request_obj.status == "deleted":
            return jsonify({"error": "request_not_found", "message": "Заявку не знайдено."}), 404
        try:
            order = complete_issue_request(db, request_obj)
        except ValueError as exc:
            db.rollback()
            code = str(exc)
            if code == "nothing_scanned":
                return jsonify({"error": "nothing_scanned", "message": "Спочатку відскануйте хоча б одну позицію."}), 400
            if code == "not_fully_scanned":
                return jsonify({"error": "not_fully_scanned", "message": "Спочатку відскануйте всі позиції у заявці."}), 400
            if code.startswith("item_not_found:"):
                item_ref = code.split(":", 1)[1]
                return jsonify({"error": "item_not_found", "message": f"Позицію {item_ref} не знайдено у базі."}), 400
            if code.startswith("not_enough:"):
                _, part_number, available_qty = code.split(":", 2)
                return jsonify({
                    "error": "not_enough",
                    "message": f"Для {part_number} доступно лише {available_qty} шт. у \"Всі товари\".",
                }), 400
            raise
        db.commit()
        payload = mobile_issue_payload(db)
        return jsonify({
            **payload,
            "ok": True,
            "message": "Готово до пакування.",
            "orderId": order.id if order else 0,
        })
    finally:
        db.close()


@app.route("/api/mobile/shipment/list")
def api_mobile_shipment_list():
    db = SessionLocal()
    try:
        payload = mobile_shipment_payload(db)
        db.commit()
        return jsonify(payload)
    finally:
        db.close()


@app.route("/api/mobile/shipment/<int:request_id>/ttn", methods=["POST"])
def api_mobile_shipment_submit_ttn(request_id):
    db = SessionLocal()
    try:
        request_obj = db.get(PackingRequest, request_id)
        if not request_obj or request_obj.status == "deleted":
            return jsonify({"error": "request_not_found", "message": "Заявку не знайдено."}), 404
        if request_obj.status != "awaiting_shipment" or (request_obj.delivery_type or "pickup") != "nova_poshta":
            return jsonify({"error": "request_not_ready", "message": "Заявка ще не готова до відправки."}), 400

        payload = request.get_json(silent=True) or {}
        ttn = normalize_text(payload.get("ttn", "") or "").strip()
        if not ttn:
            return jsonify({"error": "ttn_required", "message": "Вкажіть ТТН Нової пошти."}), 400

        order = db.get(Order, request_obj.source_order_id) if request_obj.source_order_id else None
        if not order:
            order = Order(
                customer_name=normalize_text(request_obj.customer_name or "").strip() or "Клієнт",
                phone=normalize_text(request_obj.phone or "").strip(),
                city=normalize_text(request_obj.city or "").strip(),
                comment=packing_request_order_comment(request_obj) or request_obj.comment or "",
                total_usd=0,
                status="awaiting_shipment",
                is_processing=True,
                prepayment_usd=float(request_obj.control_payment_uah or 0),
                ttn="",
                ttn_status="",
                cancel_reason="",
                stock_reserved=False,
                external_source="",
                external_order_id="",
                external_status="",
                created_at=now(),
                updated_at=now(),
            )
            db.add(order)
            db.flush()
            total_usd = 0.0
            for item in request_obj.items or []:
                part = db.get(Part, item.part_id) if item.part_id else None
                price = float(part.price_usd or 0) if part else 0.0
                qty = max(int(item.expected_qty or 0), 0)
                total_usd += price * qty
                order.items.append(
                    OrderItem(
                        part_id=item.part_id,
                        part_number=item.part_number or "",
                        name=item.title or item.part_number or "Товар",
                        qty=qty,
                        price_usd=price,
                    )
                )
            order.total_usd = total_usd
            request_obj.source_order_id = order.id

        order.ttn = ttn
        order.status = "shipped"
        order.is_processing = True
        order.ttn_status = "Очікує оновлення"
        order.updated_at = now()
        request_obj.status = "shipped"
        request_obj.updated_at = now()

        flash_news(
            db,
            "nova_poshta",
            "Замовлення відправлено",
            f"Замовлення #{order.id} відправлено. ТТН: {ttn}.",
            "success",
        )
        queue_telegram_message(db, build_order_ttn_telegram_message(order, ttn))
        flash_news(db, "telegram", "ТТН передано в Telegram", f"Замовлення #{order.id}: {ttn}.", "info")
        db.commit()
        result_payload = mobile_shipment_payload(db)
        return jsonify({
            **result_payload,
            "ok": True,
            "message": f"ТТН {ttn} збережено.",
            "orderId": order.id,
        })
    finally:
        db.close()


@app.route("/api/mobile/issue/manual/scan", methods=["POST"])
def api_mobile_manual_issue_scan():
    db = SessionLocal()
    try:
        payload = request.get_json(silent=True) or {}
        scan_code = normalize_text(payload.get("barcode", "") or "").strip()
        if not scan_code:
            return jsonify({"error": "barcode_required"}), 400

        template = find_manual_issue_template_by_code(db, scan_code)
        if not template:
            return jsonify({
                "ok": True,
                "found": False,
                "message": "ПЕРЕСОРТ!",
                "item": None,
            })

        available_qty = manual_issue_available_qty(db, template)
        if available_qty <= 0:
            db.commit()
            return jsonify({
                "ok": True,
                "found": True,
                "message": "ПЕРЕСОРТ!",
                "item": serialize_manual_issue_item(db, template),
            })

        db.commit()
        return jsonify({
            "ok": True,
            "found": True,
            "message": f"Доступно {available_qty} шт.",
            "item": serialize_manual_issue_item(db, template),
        })
    finally:
        db.close()


@app.route("/api/mobile/issue/manual", methods=["POST"])
def api_mobile_manual_issue_submit():
    db = SessionLocal()
    try:
        payload = request.get_json(silent=True) or {}
        destination = payload.get("destination", "") or ""
        items_payload = payload.get("items") or []
        try:
            request_obj = create_manual_issue_request(db, destination, items_payload)
        except ValueError as exc:
            db.rollback()
            code = str(exc)
            if code == "destination_required":
                return jsonify({"error": "destination_required", "message": "Вкажіть, куди видано товар."}), 400
            if code == "items_required":
                return jsonify({"error": "items_required", "message": "Додайте хоча б одну позицію."}), 400
            if code.startswith("item_not_found:"):
                item_ref = code.split(":", 1)[1]
                return jsonify({"error": "item_not_found", "message": f"Позицію {item_ref} не знайдено у базі."}), 400
            if code.startswith("not_enough:"):
                _, part_number, available_qty = code.split(":", 2)
                return jsonify({
                    "error": "not_enough",
                    "message": f"Для {part_number} доступно лише {available_qty} шт. у наявності.",
                }), 400
            raise

        db.commit()
        return jsonify({
            "ok": True,
            "orderId": request_obj.id,
            "requestId": request_obj.id,
            "message": f"Заявку #{request_obj.id} створено у \"Стоїть на видачі\".",
        })
    finally:
        db.close()


@app.route("/api/mobile/packing/<int:request_id>/item", methods=["POST"])
def api_mobile_packing_item(request_id):
    db = SessionLocal()
    try:
        request_obj = db.get(PackingRequest, request_id)
        if not request_obj:
            return jsonify({"error": "request_not_found"}), 404
        item_id = int(request.form.get("itemId") or 0)
        found_qty = max(int(request.form.get("foundQty") or 0), 0)
        item = db.get(PackingRequestItem, item_id)
        if not item or item.request_id != request_obj.id:
            return jsonify({"error": "item_not_found"}), 404
        expected_qty = max(int(item.expected_qty or 0), 0)
        item.found_qty = min(found_qty, expected_qty)
        item.missing_qty = max(expected_qty - item.found_qty, 0)
        if item.found_qty == expected_qty:
            item.status = "found"
        elif item.found_qty <= 0:
            item.status = "missing"
        else:
            item.status = "partial"
        existing_photos = parse_media_urls(item.photos_json)
        new_photos = save_uploads(request.files.getlist("photo"), f"pack_{request_id}_{item.id}")
        item.photos_json = dump_media_urls(existing_photos + new_photos)
        item.updated_at = now()
        recalc_packing_request(request_obj)
        db.commit()
        return jsonify({"ok": True, **packing_mobile_payload(db, request_obj)})
    finally:
        db.close()


@app.route("/api/mobile/packing/<int:request_id>/packed", methods=["POST"])
def api_mobile_packing_packed(request_id):
    db = SessionLocal()
    try:
        request_obj = db.get(PackingRequest, request_id)
        if not request_obj:
            return jsonify({"error": "request_not_found"}), 404
        recalc_packing_request(request_obj)
        if request_obj.status != "ready":
            return jsonify({"error": "packing_not_ready"}), 400
        request_obj.status = "packed"
        request_obj.updated_at = now()
        flash_news(db, "packing", "Заявку упаковано", f"Заявку #{request_obj.id} підтверджено в додатку.", "success")
        db.commit()
        return jsonify({"ok": True, **packing_mobile_payload(db, request_obj)})
    finally:
        db.close()

@app.route("/uploads/<path:filename>")
def uploaded_file(filename):
    return send_file(UPLOAD_DIR / filename)


@app.route("/google6a2f4ce00040ba4f.html")
def google_site_verification():
    return send_file(BASE_DIR / "google6a2f4ce00040ba4f.html", mimetype="text/html")


if __name__ == "__main__":
    wait_for_db()
    seed_if_empty()
    start_backup_scheduler()
    app.run(host="0.0.0.0", port=8080, debug=False)


