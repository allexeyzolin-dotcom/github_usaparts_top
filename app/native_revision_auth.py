"""Native revision authentication, scoped to the revision API only."""
import hashlib
import secrets
import threading
import time
import unicodedata
from datetime import datetime, timedelta
from functools import wraps
from urllib.parse import unquote

from flask import g, jsonify, redirect, request, session, url_for
from sqlalchemy import Column, DateTime, String

PREFIX = "/api/mobile/inventory-revision"


def register_native_revision_auth(site):
    class NativeRevisionSession(site.Base):
        __tablename__ = "native_revision_sessions"
        token_hash = Column(String(64), primary_key=True)
        user_name = Column(String(100), nullable=False)
        credential_stamp = Column(String(64), nullable=False)
        created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
        expires_at = Column(DateTime, nullable=False, index=True)

    failures = {}
    lock = threading.Lock()
    site.native_revision_login_failures = failures

    def username(value):
        if not isinstance(value, str):
            return ""
        value = " ".join(unicodedata.normalize("NFKC", value).split())
        return value if 0 < len(value) <= 100 else ""

    def stamp(db):
        value = site.get_admin_password_hash(db) or "default:" + site.ADMIN_PASSWORD
        return hashlib.sha256(value.encode()).hexdigest()

    def denied(message="Увійдіть у ревізію повторно.", status=401):
        return jsonify(ok=False, error=message, message=message), status

    def access(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            if not request.path.startswith(PREFIX):
                if not session.get("admin_auth"):
                    return redirect(url_for("admin_login"))
                return fn(*args, **kwargs)
            authorization = request.headers.get("Authorization", "")
            if not authorization.startswith("Bearer "):
                return denied()
            token = authorization[7:]
            if not 32 <= len(token) <= 256:
                return denied()
            with site.SessionLocal() as db:
                record = db.get(NativeRevisionSession, hashlib.sha256(token.encode()).hexdigest())
                if not record or record.expires_at <= datetime.utcnow() or not secrets.compare_digest(record.credential_stamp, stamp(db)):
                    return denied()
                supplied = request.headers.get("X-Revision-User", "")
                actor = username(unquote(supplied)) if supplied else record.user_name
                if not actor:
                    return denied("Вкажіть користувача в налаштуваннях (до 100 символів).", 400)
                g.native_revision_authorized = True
                g.native_revision_actor = actor
                g.native_revision_token_hash = record.token_hash
            return fn(*args, **kwargs)
        return wrapped

    @site.app.route(PREFIX + "/login", methods=["POST"])
    def native_revision_login():
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return denied("Введіть дані для входу.", 400)
        actor = username(payload.get("user_name"))
        email, password = payload.get("email", ""), payload.get("password", "")
        if not actor or not isinstance(email, str) or not isinstance(password, str) or len(email) > 255 or len(password) > 1024:
            return denied("Вкажіть користувача, email і пароль кабінету.", 400)
        key = (request.remote_addr, email.strip().casefold())
        current = time.monotonic()
        with lock:
            for old in [old for old, (_, until) in failures.items() if until <= current]:
                failures.pop(old, None)
            attempts, until = failures.get(key, (0, current + 300))
            if attempts >= 10:
                return denied("Забагато спроб. Спробуйте знову через 5 хвилин.", 429)
        with site.SessionLocal() as db:
            valid = email.strip() == site.ADMIN_EMAIL and site.verify_admin_password(db, password.strip())
            if not valid:
                with lock:
                    attempts, until = failures.get(key, (0, time.monotonic() + 300))
                    if len(failures) >= 1024:
                        failures.pop(next(iter(failures)), None)
                    failures[key] = (attempts + 1, until)
                return denied("Неправильний email або пароль.")
            token = secrets.token_urlsafe(48)
            expiry = datetime.utcnow() + timedelta(days=30)
            db.query(NativeRevisionSession).filter(NativeRevisionSession.expires_at <= datetime.utcnow()).delete()
            db.add(NativeRevisionSession(token_hash=hashlib.sha256(token.encode()).hexdigest(), user_name=actor,
                                          credential_stamp=stamp(db), expires_at=expiry))
            db.commit()
        with lock:
            failures.pop(key, None)
        return jsonify(ok=True, token=token, user_name=actor, expires_at=expiry.isoformat() + "Z")

    @site.app.route(PREFIX + "/logout", methods=["POST"])
    @access
    def native_revision_logout():
        with site.SessionLocal.begin() as db:
            record = db.get(NativeRevisionSession, g.native_revision_token_hash)
            if record:
                db.delete(record)
        return jsonify(ok=True)

    @site.app.after_request
    def private_native_revision_response(response):
        if request.path.startswith(PREFIX):
            response.headers["Cache-Control"] = "no-store"
        return response

    return NativeRevisionSession, access
