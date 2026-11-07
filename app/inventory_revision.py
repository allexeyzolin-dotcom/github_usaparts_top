"""Independent, counted inventory. Catalog cards are read-only copy sources."""
import csv
import hashlib
import io
import json
import re
import secrets
import shutil
import unicodedata
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from uuid import UUID, uuid4
from urllib.parse import unquote
from zoneinfo import ZoneInfo

from PIL import Image, UnidentifiedImageError

from flask import g, jsonify, render_template, request, session, Response
from sqlalchemy import Column, DateTime, Integer, Numeric, String, Text, desc, func, or_, text, select
from sqlalchemy.exc import IntegrityError


def number_key(value):
    return "".join(c for c in unicodedata.normalize("NFKC", value).upper() if c.isalnum())


def register_inventory_revision(site):
    from .native_revision_auth import register_native_revision_auth
    site.NativeRevisionSession, revision_access = register_native_revision_auth(site)
    class RevisionPart(site.Base):
        __tablename__ = "revision_parts"
        id = Column(Integer, primary_key=True)
        part_number = Column(String(255), nullable=False)
        part_key = Column(String(255), nullable=False, index=True)
        barcode = Column(String(255), nullable=False, default="")
        name = Column(String(500), nullable=False)
        brand = Column(String(255), nullable=False, default="")
        producer_type = Column(String(32), nullable=False, default="OEM")
        description = Column(Text, nullable=False, default="")
        price_usd = Column(Numeric(12, 2), nullable=False, default=0)
        qty = Column(Integer, nullable=False, default=1)
        # Plain text, with no foreign keys or relationships to the live catalog.
        warehouse_name = Column(String(255), nullable=False, default="")
        location = Column(String(255), nullable=False, default="")
        photo_urls = Column(Text, nullable=False, default="[]")
        youtube_url = Column(String(500), nullable=False, default="")
        cross_numbers = Column(Text, nullable=False, default="")
        verified_at = Column(DateTime, nullable=False, default=datetime.utcnow)
        created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
        version = Column(Integer, nullable=False, default=1)
        created_by = Column(String(100), nullable=False, default="Сайт")
        updated_by = Column(String(100), nullable=False, default="Сайт")
        created_via = Column(String(16), nullable=False, default="website")

    class RevisionOperation(site.Base):
        """A committed request can be retried without counting the same units twice."""
        __tablename__ = "revision_operations"
        request_id = Column(String(36), primary_key=True)
        request_hash = Column(String(64), nullable=False)
        part_id = Column(Integer, nullable=False)
        actor = Column(String(100), nullable=False, default="Сайт")
        action = Column(String(16), nullable=False, default="create")
        qty_delta = Column(Integer, nullable=False, default=0)
        qty_after = Column(Integer, nullable=False, default=0)
        created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def photos(value):
        # Keep local upload URLs and ordinary web image URLs; never executable schemes.
        return list(dict.fromkeys(url for url in site.parse_media_urls(value)
                                 if url.startswith(("/uploads/", "https://", "http://"))))[:30]

    def card(row, source="revision"):
        gallery = photos(getattr(row, "showcase_photo_urls", "")) + photos(row.photo_urls)
        gallery = list(dict.fromkeys(gallery))
        warehouse = (row.warehouse.name if source == "part" and row.warehouse else "")
        return {
            "id": row.id, "source": source, "part_number": row.part_number,
            "name": row.name, "brand": row.brand, "barcode": row.barcode,
            "producer_type": row.producer_type, "description": row.description,
            "price_usd": str(row.price_usd or 0), "qty": int(getattr(row, "qty", 0) or 0),
            "warehouse_name": getattr(row, "warehouse_name", warehouse),
            "location": getattr(row, "location", getattr(row, "storage_location", "")), "photo_urls": gallery,
            "youtube_url": row.youtube_url,
            "cross_numbers": (row.cross_numbers if source == "revision" else
                              "\n".join(site.template_cross_numbers(row)) if source == "template" else ""),
            "verified_at": row.verified_at.isoformat() + "Z" if source == "revision" else "",
            "verified_label": row.verified_at.replace(tzinfo=timezone.utc).astimezone(ZoneInfo("Europe/Kyiv")).strftime("%d.%m.%Y %H:%M") if source == "revision" else "",
            "version": row.version if source == "revision" else None,
            "label": "Перевірена наявність" if source == "revision" else "Основна база",
            "created_by": row.created_by if source == "revision" else "",
            "updated_by": row.updated_by if source == "revision" else "",
            "created_via": row.created_via if source == "revision" else "",
        }

    def error(message, status=400, **extra):
        return jsonify(ok=False, error=message, **extra), status

    def valid_token():
        if getattr(g, "native_revision_authorized", False):
            return True
        token = request.headers.get("X-Revision-Token", "")
        expected = session.get("revision_token", "")
        return bool(token and expected and secrets.compare_digest(token, expected))

    def matches(column, query):
        escaped = query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        raw = column.ilike(f"%{escaped}%", escape="\\")
        key = number_key(query)
        if not key:
            return raw
        compact = func.upper(column)
        for symbol in (" ", "-", ".", "/", "_"):
            compact = func.replace(compact, symbol, "")
        return or_(raw, compact.like(f"%{key}%"))

    @site.app.route("/admin/revision")
    @site.admin_required
    def admin_inventory_revision():
        mobile = request.args.get("client") == "android"
        if mobile:
            supplied_user = request.headers.get("X-Revision-User", "")
            if supplied_user:
                username = " ".join(unicodedata.normalize("NFKC", unquote(supplied_user)).split())
                if not username or len(username) > 100:
                    return error("Вкажіть користувача в налаштуваннях додатка (до 100 символів).")
                session["revision_mobile_user"] = username
            if not session.get("revision_mobile_user"):
                return error("Вкажіть користувача в налаштуваннях додатка та відкрийте ревізію ще раз.")
        else:
            session.pop("revision_mobile_user", None)
        session.setdefault("revision_token", secrets.token_urlsafe(32))
        with site.SessionLocal() as db:
            total, units = db.query(func.count(RevisionPart.id), func.coalesce(func.sum(RevisionPart.qty), 0)).one()
            page = max(request.args.get("page", 1, type=int) or 1, 1)
            pages = max((total + 49) // 50, 1)
            page = min(page, pages)
            rows = db.query(RevisionPart).order_by(desc(RevisionPart.verified_at), desc(RevisionPart.id)).offset((page - 1) * 50).limit(50).all()
            return render_template("admin_inventory_revision.html", cards=[card(row) for row in rows],
                                   total=total, units=units, page=page, pages=pages,
                                   news=db.query(site.NewsFeed).order_by(desc(site.NewsFeed.created_at)).limit(12).all(),
                                   revision_token=session["revision_token"], is_mobile_revision=mobile,
                                   revision_actor=session.get("revision_mobile_user") or "Сайт")

    @site.app.route("/api/mobile/inventory-revision")
    @revision_access
    def native_inventory_revision_list():
        with site.SessionLocal() as db:
            total, units = db.query(func.count(RevisionPart.id), func.coalesce(func.sum(RevisionPart.qty), 0)).one()
            pages = max((total + 49) // 50, 1)
            page = min(max(request.args.get("page", 1, type=int) or 1, 1), pages)
            rows = db.query(RevisionPart).order_by(desc(RevisionPart.verified_at), desc(RevisionPart.id)).offset((page - 1) * 50).limit(50).all()
            return jsonify(ok=True, cards=[card(row) for row in rows], total=total, units=units, page=page, pages=pages, user_name=g.native_revision_actor)

    @site.app.route("/api/mobile/inventory-revision/<int:card_id>")
    @revision_access
    def native_inventory_revision_card(card_id):
        with site.SessionLocal() as db:
            row = db.get(RevisionPart, card_id)
            if not row:
                return error("Картку не знайдено.", 404)
            return jsonify(ok=True, card=card(row))

    @site.app.route("/api/mobile/inventory-revision/search")
    @site.app.route("/admin/revision/search")
    @revision_access
    def inventory_revision_search():
        query = request.args.get("q", "").strip()[:255]
        if len(query) < 3:
            return jsonify(ok=True, revision=[], catalog=[])
        with site.SessionLocal() as db:
            revision = db.query(RevisionPart).filter(or_(matches(RevisionPart.part_number, query),
                                                       RevisionPart.barcode == query)).order_by(RevisionPart.id).limit(20).all()
            templates = db.query(site.PartTemplate).filter(or_(matches(site.PartTemplate.part_number, query),
                site.PartTemplate.barcode == query)).order_by(site.PartTemplate.part_number, site.PartTemplate.id).limit(25).all()
            catalog = [card(row, "template") for row in templates]
            # The "all products" view also contains legacy articles that have no
            # master card yet. Use one representative per article, never per warehouse.
            has_master = db.query(site.PartTemplate.id).filter(func.upper(site.PartTemplate.part_number) == func.upper(site.Part.part_number)).exists()
            legacy_ids = db.query(func.min(site.Part.id)).filter(site.Part.is_deleted.is_(False), ~has_master,
                or_(matches(site.Part.part_number, query), site.Part.barcode == query)).group_by(func.upper(site.Part.part_number)).limit(25).subquery()
            legacy = db.query(site.Part).filter(site.Part.id.in_(select(legacy_ids.c[0]))).all()
            catalog += [card(row, "catalog") for row in legacy]
            catalog.sort(key=lambda item: (number_key(item["part_number"]) != number_key(query), item["part_number"]))
            return jsonify(ok=True, revision=[card(row) for row in revision], catalog=catalog[:25])

    def validate(payload, updating):
        values = {}
        for field, limit in {"part_number": 255, "name": 500, "brand": 255, "barcode": 255,
                             "producer_type": 32, "description": 20000, "warehouse_name": 255,
                             "location": 255, "youtube_url": 500, "cross_numbers": 5000}.items():
            value = payload.get(field, "")
            if not isinstance(value, str) or len(value.strip()) > limit:
                raise ValueError(f"Некоректне поле {field} (максимум {limit} символів).")
            values[field] = value.strip()
        values["part_number"] = unicodedata.normalize("NFKC", values["part_number"]).upper()
        values["part_key"] = number_key(values["part_number"])
        from .storage_locations import clean_location
        values["location"] = clean_location(values["location"])
        if not values["part_key"] or not values["name"] or len(values["part_number"]) > 255:
            raise ValueError("Вкажіть номер та назву товару.")
        qty = str(payload.get("qty", ""))
        if not re.fullmatch(r"\d{1,10}", qty) or not (0 if updating else 1) <= int(qty) <= 2147483647:
            raise ValueError("Вкажіть цілу фактичну кількість: від 0 при редагуванні, від 1 при додаванні.")
        values["qty"] = int(qty)
        try:
            price = Decimal(str(payload.get("price_usd", "0")).replace(",", "."))
            if not price.is_finite() or price < 0 or price > Decimal("9999999999.99") or price.as_tuple().exponent < -2:
                raise InvalidOperation
        except (InvalidOperation, ValueError):
            raise ValueError("Вкажіть ціну від 0, не більше двох знаків після коми.") from None
        values["price_usd"] = price
        gallery = payload.get("photo_urls", [])
        if not isinstance(gallery, list) or len(gallery) > 30 or any(not isinstance(url, str) or len(url) > 2048 for url in gallery):
            raise ValueError("Дозволено до 30 посилань на фото.")
        values["photo_urls"] = json.dumps(photos(gallery), ensure_ascii=False)
        if values["youtube_url"] and not values["youtube_url"].startswith(("https://", "http://")):
            raise ValueError("Некоректне посилання на відео.")
        return values

    @site.app.route("/api/mobile/inventory-revision/photos", methods=["POST"])
    @site.app.route("/admin/revision/photos", methods=["POST"])
    @revision_access
    def inventory_revision_photos():
        if not valid_token():
            return error("Сесію оновлено. Перезавантажте сторінку.", 403)
        if not request.content_length or request.content_length > 20 * 1024 * 1024:
            return error("Завантажуйте не більше 20 МБ фото за один раз.")
        files = request.files.getlist("photos")
        if not 1 <= len(files) <= 30:
            return error("Оберіть від 1 до 30 фото.")
        prepared = []
        for upload in files:
            content = upload.read(10 * 1024 * 1024 + 1)
            if len(content) > 10 * 1024 * 1024:
                return error("Одне фото має займати не більше 10 МБ.")
            try:
                with Image.open(io.BytesIO(content)) as image:
                    extension = {"JPEG": ".jpg", "PNG": ".png", "WEBP": ".webp"}.get(image.format)
                    if not extension or image.width * image.height > 40000000:
                        raise ValueError
                    image.verify()
            except (UnidentifiedImageError, OSError, ValueError, Image.DecompressionBombError):
                return error("Оберіть коректні фото JPG, PNG або WebP (до 40 мегапікселів).")
            prepared.append((f"revision_{uuid4().hex}{extension}", content))
        for filename, content in prepared:
            (site.UPLOAD_DIR / filename).write_bytes(content)
        return jsonify(ok=True, urls=[f"/uploads/{filename}" for filename, _ in prepared])

    def independent_photos(value):
        urls = []
        for url in photos(value):
            path = site.upload_path_from_url(url)
            if path and path.is_file() and not path.name.startswith("revision_"):
                destination = site.UPLOAD_DIR / f"revision_{uuid4().hex}{path.suffix}"
                shutil.copy2(path, destination)
                url = f"/uploads/{destination.name}"
            urls.append(url)
        return json.dumps(urls, ensure_ascii=False)

    @site.app.route("/api/mobile/inventory-revision/save", methods=["POST"])
    @site.app.route("/admin/revision/save", methods=["POST"])
    @revision_access
    def inventory_revision_save():
        if not valid_token():
            return error("Сесію оновлено. Перезавантажте сторінку.", 403)
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return error("Не вдалося прочитати картку.")
        action = payload.get("action", "create")
        if action not in {"create", "increment", "update"}:
            return error("Невідома дія.")
        try:
            request_id = str(UUID(str(payload.get("request_id", ""))))
            values = validate(payload, action == "update")
        except (ValueError, TypeError) as exc:
            return error(str(exc))
        fingerprint = hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        actor = getattr(g, "native_revision_actor", None) or session.get("revision_mobile_user") or "Сайт"
        via = "mobile" if getattr(g, "native_revision_authorized", False) or session.get("revision_mobile_user") else "website"
        with site.SessionLocal() as db:
            if db.get_bind().dialect.name == "postgresql":
                # Serialize a number even before its first card exists. Concurrent
                # operators must still see the duplicate question before creating it.
                lock_key = int.from_bytes(hashlib.sha256(values["part_key"].encode()).digest()[:8], "big", signed=True)
                db.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": lock_key})
            previous = db.get(RevisionOperation, request_id)
            if previous:
                if previous.request_hash != fingerprint:
                    return error("Цей запит уже використано. Відкрийте картку ще раз.", 409)
                return jsonify(ok=True, card=card(db.get(RevisionPart, previous.part_id)), replayed=True)
            target = None
            if action in {"increment", "update"}:
                target_id = payload.get("target_id")
                if type(target_id) is not int:
                    return error("Оберіть картку ревізії.")
                target = db.query(RevisionPart).filter_by(id=target_id).with_for_update().first()
                if not target:
                    return error("Картку ревізії не знайдено.", 404)
                if action == "increment" and target.part_key != values["part_key"]:
                    return error("Номер товару не збігається з вибраною карткою.")
                if action == "update" and payload.get("version") != target.version:
                    return error("Картку вже змінено. Перезавантажте сторінку та повторіть редагування.", 409)
            if action == "create" or (action == "update" and target.part_key != values["part_key"]):
                duplicates = db.query(RevisionPart).filter_by(part_key=values["part_key"]).order_by(RevisionPart.id).all()
                if duplicates and payload.get("duplicate_action") != "new_card":
                    return error("Такий товар уже є в ревізії. Додати кількість чи створити нову картку?", 409,
                                 code="duplicate", duplicates=[card(row) for row in duplicates])
            if action == "increment":
                previous_qty = target.qty
                if target.qty + values["qty"] > 2147483647:
                    return error("Завелика кількість товару.")
                target.qty += values["qty"]
                target.location = values["location"]
            else:
                if target is None:
                    target = RevisionPart()
                    target.created_by = actor
                    target.created_via = via
                    db.add(target)
                    previous_qty = 0
                else:
                    previous_qty = target.qty
                values["photo_urls"] = independent_photos(values["photo_urls"])
                for field, value in values.items():
                    setattr(target, field, value)
            target.verified_at = datetime.utcnow()
            target.updated_by = actor
            target.version = (target.version or 0) + 1
            db.flush()
            db.add(RevisionOperation(request_id=request_id, request_hash=fingerprint, part_id=target.id,
                                     actor=actor, action=action, qty_delta=target.qty - previous_qty, qty_after=target.qty))
            try:
                db.commit()
            except IntegrityError:
                db.rollback()
                previous = db.get(RevisionOperation, request_id)
                if previous and previous.request_hash == fingerprint:
                    return jsonify(ok=True, card=card(db.get(RevisionPart, previous.part_id)), replayed=True)
                raise
            return jsonify(ok=True, card=card(target))

    @site.app.route("/api/mobile/inventory-revision/export")
    @site.app.route("/admin/revision/export")
    @revision_access
    def inventory_revision_export():
        output = io.StringIO(newline="")
        writer = csv.writer(output, delimiter=";")
        fields = ["id", "part_number", "name", "brand", "producer_type", "barcode", "qty", "price_usd",
                  "warehouse_name", "location", "description", "photo_urls", "youtube_url", "cross_numbers", "verified_at",
                  "created_by", "updated_by", "created_via"]
        writer.writerow(fields + ["label"])
        with site.SessionLocal() as db:
            for row in db.query(RevisionPart).order_by(RevisionPart.id).yield_per(500):
                values = [str(getattr(row, field)) for field in fields] + ["Перевірена наявність"]
                # Text remains text when this file is opened in a spreadsheet.
                writer.writerow(["'" + value if value.lstrip().startswith(("=", "+", "-", "@")) else value for value in values])
        return Response("\ufeff" + output.getvalue(), mimetype="text/csv; charset=utf-8",
                        headers={"Content-Disposition": 'attachment; filename="verified-inventory.csv"'})

    return RevisionPart, RevisionOperation
