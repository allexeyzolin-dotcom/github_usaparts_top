"""Durable, bounded Merchant API delivery of published revision cards.

The queue is independent of Google approval. Never treat a successful insert as
approval, invent manufacturer identifiers, or send a placeholder as a photo.
"""
from datetime import datetime, timedelta
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import unquote, urlsplit
import json
import os
import secrets
import threading
import time

import requests
from PIL import Image, ImageOps, UnidentifiedImageError
from flask import abort, flash, jsonify, redirect, render_template, request, session, url_for
from sqlalchemy import Column, DateTime, Integer, String, Text, case, or_, text

SITE = None
MODEL = None
LOCK = threading.Lock()
STARTED = False
MAX_BYTES = 16 * 1024 * 1024
LABELS = {"pending": "У черзі", "retry": "Повторна спроба", "blocked": "Потрібне виправлення",
          "synced": "Передано API", "skipped": "Не відправляється"}
GOOGLE_LABELS = {"approved": "Схвалено", "pending": "Google перевіряє",
                 "disapproved": "Відхилено", "unknown": "Статус ще не отримано"}


def digest(value):
    return sha256(json.dumps(value, ensure_ascii=True, sort_keys=True, default=str).encode()).hexdigest()


def options(settings):
    return {"enabled": SITE.parse_backup_bool(settings.get("merchant_revision_enabled")),
            "batch": SITE.parse_backup_int(settings.get("merchant_revision_batch"), 20, 1, 100),
            "interval": SITE.parse_backup_int(settings.get("merchant_revision_interval"), 300, 60, 86400)}


def destination(config):
    return digest([config["account_id"], SITE.google_merchant_data_source_name(config),
                   config["content_language"], config["feed_label"]])


def checked_image_url(url):
    """Only fetch known product-image origins; never follow arbitrary redirects."""
    parsed = urlsplit(url)
    own = urlsplit(SITE.public_site_base_url()).hostname
    if (parsed.scheme != "https" or parsed.hostname not in {own, "usaparts.top", "www.usaparts.top", "i.avto.pro"}
            or parsed.username or parsed.password or parsed.port not in (None, 443)
            or "placeholder" in parsed.path.lower()):
        raise ValueError("Неприпустиме джерело фото або фото-заглушка")
    return parsed


def image_bytes(url):
    parsed = checked_image_url(url)
    if parsed.hostname in {urlsplit(SITE.public_site_base_url()).hostname, "usaparts.top", "www.usaparts.top"}:
        path = unquote(parsed.path)
        if not path.startswith("/uploads/"):
            raise ValueError("Власне фото має бути в /uploads/")
        candidate = (SITE.UPLOAD_DIR / path[len("/uploads/"):]).resolve()
        if not candidate.is_relative_to(SITE.UPLOAD_DIR.resolve()) or not candidate.is_file():
            raise ValueError("Файл фото не знайдено")
        if candidate.stat().st_size > MAX_BYTES:
            raise ValueError("Фото перевищує 16 МБ")
        return candidate.read_bytes()
    current = url
    for _ in range(4):
        checked_image_url(current)
        with requests.get(current, timeout=(8, 20), stream=True, allow_redirects=False,
                          headers={"User-Agent": "USAparts-Merchant-Images/1.0"}) as response:
            if response.is_redirect:
                from urllib.parse import urljoin
                current = urljoin(current, response.headers.get("Location", ""))
                continue
            response.raise_for_status()
            data = bytearray()
            for chunk in response.iter_content(65536):
                data.extend(chunk)
                if len(data) > MAX_BYTES:
                    raise ValueError("Фото перевищує 16 МБ")
            return bytes(data)
    raise ValueError("Забагато перенаправлень фото")


def cache_image(url, force=False):
    """Decode real bytes and serve a matching JPEG from the persistent uploads volume."""
    checked_image_url(url)
    folder = SITE.UPLOAD_DIR / "merchant"
    folder.mkdir(exist_ok=True)
    manifest = folder / (sha256(url.encode()).hexdigest() + ".json")
    if manifest.is_file() and not force:
        try:
            cached = json.loads(manifest.read_text("utf-8"))
            if (datetime.fromisoformat(cached["checked"]) > datetime.utcnow() - timedelta(days=7)
                    and (folder / cached["file"]).is_file()):
                return SITE.absolute_public_url("/uploads/merchant/" + cached["file"]), cached["warning"]
        except (ValueError, KeyError, OSError):
            pass
    raw = image_bytes(url)
    try:
        with Image.open(BytesIO(raw)) as source:
            if source.format not in {"JPEG", "PNG", "WEBP", "GIF", "TIFF", "BMP"}:
                raise ValueError("Непідтримуваний формат фото")
            width, height = source.size
            if width * height > 64000000 or min(width, height) < 100:
                raise ValueError(f"Непридатний розмір фото: {width}×{height}")
            source.load()
            oriented = ImageOps.exif_transpose(source).convert("RGBA")
            canvas = Image.new("RGB", oriented.size, "white")
            canvas.paste(oriented, mask=oriented.getchannel("A"))
            canvas.thumbnail((2400, 2400), Image.Resampling.LANCZOS)
            output = BytesIO()
            canvas.save(output, "JPEG", quality=94, optimize=True)
    except (UnidentifiedImageError, OSError, Image.DecompressionBombError) as exc:
        raise ValueError("Файл не є справним зображенням") from exc
    content = output.getvalue()
    filename = sha256(content).hexdigest() + ".jpg"
    target = folder / filename
    if not target.exists():
        temporary = folder / (secrets.token_hex(12) + ".tmp")
        temporary.write_bytes(content)
        os.replace(temporary, target)
    warning = f"Фото {width}×{height}: до 31.01.2027 потрібен оригінал щонайменше 500×500" if min(width, height) < 500 else ""
    value = {"file": filename, "checked": datetime.utcnow().isoformat(), "warning": warning}
    temporary = manifest.with_suffix("." + secrets.token_hex(6) + ".tmp")
    temporary.write_text(json.dumps(value), "utf-8")
    os.replace(temporary, manifest)
    return SITE.absolute_public_url("/uploads/merchant/" + filename), warning


def prepare_payload(part, config, revision=True):
    # Reuse the storefront's exact price, condition, title and canonical URL.
    proxy = SimpleNamespace(**{column.name: getattr(part, column.name) for column in SITE.Part.__table__.columns})
    proxy.warehouse = part.warehouse
    proxy.is_deleted, proxy.in_stock, proxy.qty = False, True, 1
    gallery = list(dict.fromkeys(SITE.parse_media_urls(part.showcase_photo_urls) + SITE.parse_media_urls(part.photo_urls)))
    # A stale showcase URL must not hide a real original upload on the same card.
    gallery.sort(key=lambda url: not urlsplit(url).path.startswith("/uploads/"))
    proxy.showcase_photo_urls = json.dumps(gallery)
    payload, reason = SITE.google_merchant_part_payload(proxy, {**config, "allow_placeholder_images": False})
    if payload is None:
        raise ValueError(reason)
    attrs = payload["productAttributes"]
    attrs["title"] = SITE.clean_search_text(attrs["title"])
    attrs["description"] = SITE.compact_meta_text(attrs["title"],
        SITE.clean_search_text(part.description or ""), f"Виробник: {part.brand or part.brand_export or ''}.", limit=5000)
    attrs["availability"] = "IN_STOCK" if not part.is_deleted and part.in_stock and part.qty > 0 else "OUT_OF_STOCK"
    brand = SITE.seo_clean_label(part.brand or part.brand_export or "")
    if not brand or brand.casefold() in {"oem", "oem бв", "замінник", "usaparts.top"}:
        raise ValueError("Не вказано справжній бренд виробника")
    attrs["brand"] = brand[:70]
    if not (part.part_number or "").strip():
        raise ValueError("Не вказано номер виробника (MPN)")
    attrs["googleProductCategory"] = "899"  # Vehicles & Parts > Vehicle Parts & Accessories > Motor Vehicle Parts
    categories = SITE.seo_part_categories(part)
    attrs["productTypes"] = ["Автозапчастини" + (" > " + categories[0]["label"] if categories else "")]
    if revision:
        attrs["customLabel0"] = "Перевірена наявність"
        attrs["customLabel1"] = "Ревізія"
    attrs["shipping"] = [{"country": "UA", "service": "Нова Пошта", "price": {
        "amountMicros": "300000000", "currencyCode": "UAH"}, "minHandlingTime": "0", "maxHandlingTime": "1",
        "minTransitTime": "1", "maxTransitTime": "3", "handlingCutoffTime": "1200", "handlingCutoffTimezone": "Europe/Kyiv"}]
    return payload


def prepare_images(payload):
    attrs = payload["productAttributes"]
    urls = [attrs["imageLink"], *attrs.pop("additionalImageLinks", [])]
    images, warnings, failures = [], [], []
    for url in dict.fromkeys(urls):
        try:
            converted, warning = cache_image(url)
            if converted not in images:
                images.append(converted)
            if warning:
                warnings.append(warning)
        except (ValueError, requests.RequestException) as exc:
            # Do not echo external URLs, credentials or request internals into the cabinet.
            failures.append(str(exc) if isinstance(exc, ValueError) else "Не вдалося завантажити оригінал фото")
    if not images:
        raise ValueError("; ".join(dict.fromkeys(failures)) or "Немає реального фото")
    attrs["imageLink"] = images[0]
    if images[1:]:
        attrs["additionalImageLinks"] = images[1:11]
    return "; ".join(dict.fromkeys(warnings + failures))[:2000]


def discover(db, config, now):
    """Checkpoint source snapshots. Unchanged blocked cards retry at most daily."""
    dest = destination(config)
    existing = {row.revision_id: row for row in db.query(MODEL).filter_by(destination=dest)}
    rows = db.query(SITE.RevisionPart, SITE.Part).join(
        SITE.RevisionPublication, SITE.RevisionPublication.revision_id == SITE.RevisionPart.id).join(
        SITE.Part, SITE.Part.id == SITE.RevisionPublication.part_id).order_by(SITE.RevisionPart.id).all()
    # Negative IDs are existing Google offers explicitly imported for repair.
    # Never create new offers for the remainder of the unverified catalog.
    legacy_ids = [-key for key in existing if key < 0]
    for part in db.query(SITE.Part).filter(SITE.Part.id.in_(legacy_ids)).all() if legacy_ids else []:
        rows.append((SimpleNamespace(id=-part.id, version=part.updated_at or part.created_at), part))
    offers = {}
    for revision, part in rows:
        if part.in_stock and not part.is_deleted and part.qty > 0:
            offers.setdefault(SITE.google_merchant_offer_id(part), []).append((revision, part))
    canonical = {}
    for offer, candidates in offers.items():
        identities = {(SITE.seo_clean_label(part.part_number).upper(), SITE.seo_clean_label(part.brand or part.brand_export).upper(),
                       SITE.part_condition_code(part)) for _, part in candidates}
        if len(candidates) > 1 and len(identities) == 1:
            # The public catalog already selects one card per OEM. Use that exact
            # card/price rather than letting duplicate cards overwrite one offer.
            preferred = SITE.best_unique_public_parts([part for _, part in candidates])[0]
            canonical[offer] = next(revision.id for revision, part in candidates if part.id == preferred.id)
    present = set()
    for revision, part in rows:
        present.add(revision.id)
        job = existing.get(revision.id)
        if job is None:
            job = MODEL(revision_id=revision.id, destination=dest, state="pending", attempts=0)
            db.add(job)
        job.part_id, job.offer_id, job.title = part.id, SITE.google_merchant_offer_id(part), part.name
        reason, payload, duplicate = "", None, False
        try:
            payload = prepare_payload(part, config, revision=revision.id > 0)
        except ValueError as exc:
            reason = str(exc)
        if len(offers.get(job.offer_id, [])) > 1:
            preferred = canonical.get(job.offer_id)
            if preferred and revision.id != preferred:
                duplicate, reason = True, f"Однаковий товар: передається публічна картка #{preferred} з актуальною ціною сайту"
            elif not preferred:
                reason = "Однаковий Merchant ID у картках: " + ", ".join(str(row.id) for row, _ in offers[job.offer_id])
        signature = digest([payload, reason, revision.version])
        changed = signature != job.source_hash
        if changed:
            job.source_hash, job.source_json = signature, json.dumps(payload, ensure_ascii=False)
            job.attempts, job.next_at = 0, now
            job.state, job.error = ("blocked", reason) if reason else ("pending", "")
        if duplicate:
            job.state, job.error = "skipped", reason
        if not part.in_stock or part.is_deleted or part.qty <= 0:
            if not job.sent_at and not job.product_name:
                job.state, job.error = "skipped", "Нульовий залишок; товар ще не відправлявся"
            elif job.state == "skipped" and payload and not reason:
                job.state, job.error, job.next_at = "pending", "", now
            elif reason and job.sent_json:
                # Stock withdrawal must still work if the photo/price was removed meanwhile.
                previous = json.loads(job.sent_json)
                previous["productAttributes"]["availability"] = "OUT_OF_STOCK"
                job.source_json = json.dumps(previous, ensure_ascii=False)
                job.source_hash = digest(previous)
                job.state, job.error = ("pending", "") if job.sent_hash != job.source_hash else ("synced", "")
        if job.state == "synced" and job.sent_at and job.sent_at < now - timedelta(days=14):
            job.state, job.next_at = "pending", now
        if job.state == "blocked" and not changed and job.next_at and job.next_at <= now:
            job.state = "pending" if not reason else "blocked"
            job.next_at = now + timedelta(days=1)
    # Removed revision cards are withdrawn without deleting the Google offer.
    for revision_id, job in existing.items():
        if revision_id not in present and job.sent_json:
            payload = json.loads(job.sent_json)
            payload["productAttributes"]["availability"] = "OUT_OF_STOCK"
            signature = digest(payload)
            if signature != job.sent_hash:
                job.source_json, job.source_hash = json.dumps(payload), signature
                job.state, job.next_at = "pending", now
    db.commit()


def import_existing(db, config, headers, now):
    """Read one page of this exact data source; queue only already-existing offers."""
    settings = SITE.get_api_settings_map(db)
    if settings.get("merchant_revision_import_active") != "1":
        return
    dest = destination(config)
    cursor = settings.get("merchant_revision_import_cursor", "") if settings.get("merchant_revision_import_destination") == dest else ""
    response = requests.get(f"{SITE.GOOGLE_MERCHANT_API_BASE}/products/v1/accounts/{config['account_id']}/products",
                            params={"pageSize": 100, **({"pageToken": cursor} if cursor else {})}, headers=headers, timeout=35)
    if not response.ok:
        SITE.set_setting(db, "merchant_revision_import_message", SITE.google_merchant_response_summary(response))
        db.commit()
        return
    result = response.json()
    parts = SITE.best_unique_public_parts(db.query(SITE.Part).filter(SITE.Part.is_deleted.is_(False)).all())
    by_offer = {SITE.google_merchant_offer_id(part): part for part in parts}
    jobs = {job.offer_id: job for job in db.query(MODEL).filter_by(destination=dest)}
    count = 0
    for product in result.get("products", []):
        if (product.get("dataSource") != SITE.google_merchant_data_source_name(config)
                or product.get("contentLanguage") != config["content_language"] or product.get("feedLabel") != config["feed_label"]
                or product.get("legacyLocal") or product.get("archived")):
            continue
        offer = product.get("offerId")
        part = by_offer.get(offer)
        if not part:
            continue
        job = jobs.get(offer)
        if not job:
            job = MODEL(revision_id=-part.id, destination=dest, part_id=part.id, offer_id=offer,
                        title=part.name, state="pending", attempts=0)
            db.add(job)
            jobs[offer] = job
        job.product_name = product.get("base64EncodedName") or product["name"]
        job.google_state, job.issues = approval(product)
        job.checked_at = now
        count += 1
    cursor = result.get("nextPageToken", "")
    SITE.set_setting(db, "merchant_revision_import_cursor", cursor)
    SITE.set_setting(db, "merchant_revision_import_destination", dest)
    SITE.set_setting(db, "merchant_revision_import_active", "1" if cursor else "0")
    SITE.set_setting(db, "merchant_revision_import_message", f"Прочитано сторінку Google: зіставлено {count} товарів. " + ("Імпорт триває порціями." if cursor else "Зіставлення існуючих товарів завершено."))
    db.commit()
    discover(db, config, now)


def approval(product):
    status = product.get("productStatus", {})
    destinations = status.get("destinationStatuses", [])
    relevant = [item for item in destinations if item.get("reportingContext") == "FREE_LISTINGS"] or destinations
    if any("UA" in item.get("disapprovedCountries", []) for item in relevant):
        state = "disapproved"
    elif any("UA" in item.get("pendingCountries", []) for item in relevant):
        state = "pending"
    elif any("UA" in item.get("approvedCountries", []) for item in relevant):
        state = "approved"
    else:
        state = "unknown"
    issues = [{key: str(issue.get(key, ""))[:1500] for key in ("code", "severity", "attribute", "description", "detail")}
              for issue in status.get("itemLevelIssues", [])]
    return state, json.dumps(issues, ensure_ascii=False)


def retry_delay(response, attempts):
    try:
        hint = int(response.headers.get("Retry-After", "0")) if response is not None else 0
    except ValueError:
        hint = 0
    return max(hint, min(86400, 60 * 2 ** min(attempts, 10)))


def run_batch(force=False):
    if not LOCK.acquire(blocking=False):
        return
    connection = None
    advisory = False
    try:
        connection = SITE.engine.connect()
        if connection.dialect.name == "postgresql":
            if not connection.execute(text("SELECT pg_try_advisory_lock(726576660009)")).scalar():
                return
            advisory = True
            connection.commit()
        with SITE.SessionLocal() as db:
            settings = SITE.get_api_settings_map(db)
            config, opt = SITE.google_merchant_settings_from_map(settings), options(settings)
            now = datetime.utcnow()
            if not config["enabled"] or (not force and not opt["enabled"]):
                return
            if SITE.google_merchant_required_missing(config):
                SITE.set_setting(db, "merchant_revision_message", "Заповніть Merchant account, API data source та облікові дані в налаштуваннях API")
                db.commit()
                return
            due = settings.get("merchant_revision_next_at", "")
            if not force and due and datetime.fromisoformat(due) > now:
                return
            discover(db, config, now)
            SITE.set_setting(db, "merchant_revision_last_at", now.isoformat(timespec="seconds"))
            SITE.set_setting(db, "merchant_revision_next_at", (now + timedelta(seconds=opt["interval"])).isoformat())
            SITE.set_setting(db, "merchant_revision_message", "Виконується порція. Завершені товари зберігаються по одному; наступний запуск продовжить чергу.")
            db.commit()
            headers = SITE.google_merchant_request_headers(config)
            import_existing(db, config, headers, now)
            dest = destination(config)
            jobs = db.query(MODEL).filter(MODEL.destination == dest, MODEL.state.in_(["pending", "retry"]),
                or_(MODEL.next_at.is_(None), MODEL.next_at <= now)).order_by(
                    case((MODEL.revision_id > 0, 0), else_=1), MODEL.sent_at.desc().nullslast(), MODEL.id).limit(opt["batch"]).all()
            sent, errors = 0, 0
            started_at = time.monotonic()
            for job in jobs:
                if time.monotonic() - started_at > 120:
                    break
                snapshot = job.source_hash
                payload = json.loads(job.source_json)
                if not payload:
                    job.state, job.error = "blocked", "Неповна картка товару"
                    db.commit()
                    continue
                response = None
                try:
                    job.warning = prepare_images(payload)
                    # Retire an earlier SKU before assigning the changed card to another ID.
                    previous = json.loads(job.sent_json) if job.sent_json else None
                    if previous and previous["offerId"] != payload["offerId"]:
                        previous["productAttributes"]["availability"] = "OUT_OF_STOCK"
                        retired = requests.post(f"{SITE.GOOGLE_MERCHANT_API_BASE}/products/v1/accounts/{config['account_id']}/productInputs:insert",
                            params={"dataSource": SITE.google_merchant_data_source_name(config)}, headers=headers, json=previous, timeout=35)
                        retired.raise_for_status()
                    response = requests.post(f"{SITE.GOOGLE_MERCHANT_API_BASE}/products/v1/accounts/{config['account_id']}/productInputs:insert",
                        params={"dataSource": SITE.google_merchant_data_source_name(config)}, headers=headers, json=payload, timeout=35)
                    if not response.ok:
                        job.error = SITE.google_merchant_response_summary(response)
                        job.attempts += 1
                        transient = response.status_code in {408, 409, 429} or response.status_code >= 500
                        job.state = "retry" if transient or response.status_code in {401, 403} else "blocked"
                        job.next_at = now + (timedelta(seconds=retry_delay(response, job.attempts)) if job.state == "retry" else timedelta(days=1))
                        errors += 1
                        if response.status_code in {401, 403, 429}:
                            SITE.set_setting(db, "merchant_revision_message", job.error)
                            SITE.set_setting(db, "merchant_revision_next_at", job.next_at.isoformat())
                            db.commit()
                            break
                    else:
                        result = response.json()
                        job.product_name = result.get("product") or f"accounts/{config['account_id']}/products/{payload['contentLanguage']}~{payload['feedLabel']}~{payload['offerId']}"
                        job.state, job.error, job.attempts = "synced", "", 0
                        job.sent_hash, job.sent_json, job.sent_at = snapshot, json.dumps(payload, ensure_ascii=False), now
                        job.google_state, job.issues, job.checked_at = "pending", "[]", None
                        sent += 1
                except ValueError as exc:
                    job.state, job.error = "blocked", str(exc)[:2000]
                    job.next_at = now + timedelta(days=1)
                    errors += 1
                except requests.RequestException:
                    job.state, job.error = "retry", "Помилка з’єднання з Google або сервером фото"
                    job.attempts += 1
                    job.next_at = now + timedelta(seconds=retry_delay(response, job.attempts))
                    errors += 1
                db.commit()  # Each item survives a restart; never restart from the first SKU.
            # Approval is asynchronous. Poll a separate bounded slice, oldest checks first.
            checks = db.query(MODEL).filter(MODEL.destination == dest, MODEL.sent_at.isnot(None),
                or_(MODEL.checked_at.is_(None), MODEL.checked_at < now - timedelta(hours=6),
                    (MODEL.google_state == "pending") & (MODEL.checked_at < now - timedelta(minutes=15)))).order_by(
                    MODEL.checked_at.asc().nullsfirst(), MODEL.id).limit(opt["batch"]).all()
            for job in checks:
                try:
                    response = requests.get(f"{SITE.GOOGLE_MERCHANT_API_BASE}/products/v1/{job.product_name}", headers=headers, timeout=25)
                    if response.ok:
                        job.google_state, job.issues = approval(response.json())
                        job.status_error = ""
                    elif response.status_code != 404:
                        job.status_error = SITE.google_merchant_response_summary(response)
                    job.checked_at = now
                    db.commit()
                    if response.status_code in {401, 403, 429}:
                        break
                except requests.RequestException:
                    break
            if sent or not errors:
                SITE.set_setting(db, "merchant_revision_message", f"Порцію завершено: передано {sent}, помилок {errors}. Схвалення перевіряється окремо.")
            db.commit()
    except Exception:
        # Never put service-account JSON or request headers into a user-visible error.
        SITE.app.logger.exception("Merchant revision worker failed")
        with SITE.SessionLocal() as db:
            SITE.set_setting(db, "merchant_revision_message", "Не вдалося завершити порцію. Перевірте доступ до Merchant API в налаштуваннях API.")
            SITE.set_setting(db, "merchant_revision_next_at", (datetime.utcnow() + timedelta(minutes=15)).isoformat())
            db.commit()
    finally:
        try:
            if connection is not None:
                if advisory:
                    connection.execute(text("SELECT pg_advisory_unlock(726576660009)"))
                    connection.commit()
                connection.close()
        finally:
            LOCK.release()


def start_worker():
    global STARTED
    if STARTED:
        return
    STARTED = True
    def loop():
        while True:
            run_batch()
            threading.Event().wait(30)
    threading.Thread(target=loop, name="merchant-revision", daemon=True).start()


def install(site):
    global SITE, MODEL
    SITE = site
    class MerchantRevisionJob(site.Base):
        __tablename__ = "merchant_revision_jobs"
        id = Column(Integer, primary_key=True)
        revision_id = Column(Integer, nullable=False, index=True)
        destination = Column(String(64), nullable=False, index=True)
        part_id = Column(Integer)
        offer_id = Column(String(50), default="")
        title = Column(Text, default="")
        state = Column(String(20), default="pending", index=True)
        source_hash = Column(String(64), default="")
        sent_hash = Column(String(64), default="")
        source_json = Column(Text, default="null")
        sent_json = Column(Text, default="")
        product_name = Column(Text, default="")
        attempts = Column(Integer, default=0)
        next_at = Column(DateTime)
        sent_at = Column(DateTime)
        checked_at = Column(DateTime)
        google_state = Column(String(20), default="unknown")
        issues = Column(Text, default="[]")
        error = Column(Text, default="")
        warning = Column(Text, default="")
        status_error = Column(Text, default="")
        from sqlalchemy import UniqueConstraint
        __table_args__ = (UniqueConstraint("destination", "revision_id"),)
    MODEL = MerchantRevisionJob
    site.MerchantRevisionJob = MODEL

    @site.app.route("/admin/revision/google", methods=["GET", "POST"])
    @site.admin_required
    def admin_revision_google():
        token = session.setdefault("merchant_revision_csrf", secrets.token_urlsafe(32))
        with site.SessionLocal() as db:
            if request.method == "POST":
                if not secrets.compare_digest(token, request.form.get("csrf_token", "")):
                    abort(403)
                action = request.form.get("action")
                if action == "settings":
                    site.set_setting(db, "merchant_revision_enabled", "1" if request.form.get("enabled") else "0")
                    site.set_setting(db, "merchant_revision_batch", str(site.parse_backup_int(request.form.get("batch"), 20, 1, 100)))
                    site.set_setting(db, "merchant_revision_interval", str(site.parse_backup_int(request.form.get("interval"), 300, 60, 86400)))
                    # The same already-configured Merchant credentials and data source are used.
                    if request.form.get("enabled"):
                        site.set_setting(db, "google_merchant_enabled", "1")
                    db.commit()
                    flash("Налаштування автоматичної передачі збережено", "success")
                elif action == "retry":
                    config = site.google_merchant_settings_from_map(site.get_api_settings_map(db))
                    for job in db.query(MODEL).filter_by(destination=destination(config)).filter(MODEL.state.in_(["blocked", "retry"])):
                        job.state, job.next_at, job.attempts = "pending", datetime.utcnow(), 0
                    db.commit()
                elif action == "import":
                    site.set_setting(db, "merchant_revision_import_active", "1")
                    site.set_setting(db, "merchant_revision_import_cursor", "")
                    db.commit()
                    flash("Існуючі товари цього API-джерела будуть зіставлені й оновлені порціями. Нові товари додаються лише з ревізії.", "success")
                elif action == "run":
                    threading.Thread(target=run_batch, kwargs={"force": True}, daemon=True).start()
                    flash("Запущено одну порцію у фоні", "success")
                return redirect(url_for("admin_revision_google"))
            settings = site.get_api_settings_map(db)
            config = site.google_merchant_settings_from_map(settings)
            jobs = db.query(MODEL).filter_by(destination=destination(config)).order_by(MODEL.id).all()
            counts = {state: sum(j.state == state for j in jobs) for state in LABELS}
            google = {state: sum(j.google_state == state and bool(j.product_name) for j in jobs) for state in GOOGLE_LABELS}
            state_filter = request.args.get("state", "")
            filtered = [j for j in jobs if not state_filter or j.state == state_filter or j.google_state == state_filter
                        or (state_filter == "pending" and j.state == "retry")]
            page = max(request.args.get("page", 1, type=int), 1)
            pages = max((len(filtered) + 39) // 40, 1)
            page = min(page, pages)
            cards = [{"job": job, "issues": json.loads(job.issues or "[]")} for job in filtered[(page-1)*40:page*40]]
            response = site.app.make_response(render_template("admin_revision_google.html", opt=options(settings), config_ready=not site.google_merchant_required_missing(config),
                counts=counts, google=google, total=len(jobs), cards=cards, labels=LABELS, google_labels=GOOGLE_LABELS,
                csrf_token=token, state_filter=state_filter, page=page, pages=pages,
                message=settings.get("merchant_revision_message", "Чергу буде сформовано під час першого запуску"),
                import_message=settings.get("merchant_revision_import_message", ""),
                last_at=settings.get("merchant_revision_last_at", ""), next_at=settings.get("merchant_revision_next_at", "")))
            response.headers["Cache-Control"] = "no-store"
            return response
    return MODEL
