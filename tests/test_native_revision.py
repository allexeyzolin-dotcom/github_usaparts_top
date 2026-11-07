"""Native login and cross-client stock-taking against a disposable database."""
import hashlib
import io
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch
from urllib.parse import quote
from uuid import uuid4

from PIL import Image
from flask import url_for
import app.app as site
import test_inventory_revision as fixtures

PREFIX = "/api/mobile/inventory-revision"


class NativeRevisionTests(unittest.TestCase):
    setUp = fixtures.InventoryRevisionTests.setUp
    tearDown = fixtures.InventoryRevisionTests.tearDown
    snapshot_main = fixtures.InventoryRevisionTests.snapshot_main
    login_web = fixtures.InventoryRevisionTests.login
    save_web = fixtures.InventoryRevisionTests.save

    def login(self, **changes):
        data = dict(user_name="Комірник Андрій", email=site.ADMIN_EMAIL, password="native-test-password")
        data.update(changes)
        with patch.object(site, "ADMIN_PASSWORD", "native-test-password"):
            return self.client.post(PREFIX + "/login", json=data)

    def headers(self):
        # Store a real admin hash so subsequent requests use a stable credential stamp.
        with self.sessions.begin() as db:
            site.set_admin_password(db, "native-test-password")
        site.native_revision_login_failures.clear()
        result = self.login()
        self.assertEqual(result.status_code, 200, result.json)
        return {"Authorization": "Bearer " + result.json["token"], "X-Revision-User": quote("Комірник Андрій")}

    def save(self, headers, **changes):
        data = dict(action="create", request_id=str(uuid4()), part_number="NATIVE-001", name="Нативна деталь",
                    qty=3, price_usd="20", producer_type="OEM", location="A-12", photo_urls=[])
        data.update(changes)
        return self.client.post(PREFIX + "/save", json=data, headers=headers)

    def test_native_routes_require_bearer_even_with_website_cookie(self):
        self.login_web()
        for path in ("", "/1", "/search?q=06G", "/export"):
            self.assertEqual(self.client.get(PREFIX + path).status_code, 401)
        for path in ("/photos", "/save", "/logout"):
            response = self.client.post(PREFIX + path, json={})
            self.assertEqual(response.status_code, 401)
            self.assertEqual(response.headers["Cache-Control"], "no-store")
        with site.app.test_request_context():
            self.assertEqual(url_for("inventory_revision_save"), "/admin/revision/save")
            self.assertEqual(url_for("inventory_revision_search"), "/admin/revision/search")

    def test_login_validation_throttle_and_session_scope(self):
        site.native_revision_login_failures.clear()
        self.assertEqual(self.login(user_name="").status_code, 400)
        for _ in range(10):
            self.assertEqual(self.login(password="wrong").status_code, 401)
        self.assertEqual(self.login().status_code, 429)
        site.native_revision_login_failures.clear()
        headers = self.headers()
        with self.sessions() as db:
            row = db.query(site.NativeRevisionSession).one()
            token = headers["Authorization"][7:]
            self.assertNotEqual(row.token_hash, token)
            self.assertEqual(row.token_hash, hashlib.sha256(token.encode()).hexdigest())
            self.assertNotIn("password", row.__table__.columns)
        # A native token does not grant a website administrator cookie.
        self.assertEqual(self.client.get("/admin/revision", headers=headers).status_code, 302)
        self.assertEqual(self.client.get(PREFIX, headers=headers).status_code, 200)

    def test_create_on_app_edit_on_site_and_read_back_on_app(self):
        headers = self.headers()
        before = self.snapshot_main()
        saved = self.save(headers).json["card"]
        self.assertEqual(saved["created_by"], "Комірник Андрій")
        self.assertEqual(saved["created_via"], "mobile")
        self.login_web()
        page = self.client.get("/admin/revision")
        self.assertIn("Нативна деталь", page.text)
        self.assertIn("Комірник Андрій", page.text)
        edited = self.save_web(saved, action="update", target_id=saved["id"], version=saved["version"],
                               name="Змінено через сайт", qty=7, location="B-22")
        self.assertEqual(edited.status_code, 200, edited.json)
        fresh = self.client.get(PREFIX + "/" + str(saved["id"]), headers=headers).json["card"]
        self.assertEqual((fresh["name"], fresh["qty"], fresh["location"]), ("Змінено через сайт", 7, "B-22"))
        self.assertEqual(fresh["created_by"], "Комірник Андрій")
        self.assertEqual(fresh["updated_by"], "Сайт")
        # And in the opposite direction: the website sees native edits.
        changed = self.save(headers, **{**fresh, "action": "update", "target_id": fresh["id"], "name": "Змінено в додатку"})
        self.assertEqual(changed.status_code, 200, changed.json)
        self.assertIn("Змінено в додатку", self.client.get("/admin/revision").text)
        self.assertEqual(before, self.snapshot_main())

    def test_search_canonical_sources_location_validation_and_pagination(self):
        headers = self.headers()
        self.assertEqual(self.client.get(PREFIX + "/search?q=06", headers=headers).json["catalog"], [])
        found = self.client.get(PREFIX + "/search?q=06G", headers=headers).json["catalog"]
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0]["source"], "template")
        self.assertEqual(self.save(headers, location="").status_code, 400)
        for number in range(51):
            self.assertEqual(self.save(headers, part_number=f"NATIVE-{number:03}").status_code, 200)
        page = self.client.get(PREFIX + "?page=2", headers=headers).json
        self.assertEqual((page["total"], page["units"], page["pages"], len(page["cards"])), (51, 153, 2, 1))
        self.assertEqual(self.client.get(PREFIX + "/99999", headers=headers).status_code, 404)

    def test_duplicates_increment_retry_and_stale_edit(self):
        headers = self.headers()
        card = self.save(headers).json["card"]
        duplicate = self.save(headers)
        self.assertEqual(duplicate.status_code, 409)
        self.assertEqual(duplicate.json["code"], "duplicate")
        operation = dict(action="increment", target_id=card["id"], qty=2, request_id=str(uuid4()))
        self.assertEqual(self.save(headers, **operation).json["card"]["qty"], 5)
        retry = self.save(headers, **operation)
        self.assertTrue(retry.json["replayed"])
        self.assertEqual(retry.json["card"]["qty"], 5)
        stale = self.save(headers, action="update", target_id=card["id"], version=card["version"])
        self.assertEqual(stale.status_code, 409)
        self.assertEqual(self.save(headers, duplicate_action="new_card").status_code, 200)

    def test_native_photos_export_and_user_change(self):
        headers = self.headers()
        stream = io.BytesIO()
        Image.new("RGB", (8, 8), "red").save(stream, "PNG")
        stream.seek(0)
        response = self.client.post(PREFIX + "/photos", data={"photos": (stream, "own.png")}, headers=headers)
        self.assertEqual(response.status_code, 200, response.json)
        headers["X-Revision-User"] = quote("Олена")
        saved = self.save(headers, photo_urls=response.json["urls"]).json["card"]
        self.assertEqual(saved["created_by"], "Олена")
        self.assertEqual(len(saved["photo_urls"]), 1)
        exported = self.client.get(PREFIX + "/export", headers=headers)
        self.assertEqual(exported.status_code, 200)
        self.assertIn("Олена", exported.data.decode("utf-8-sig"))

    def test_expiry_password_rotation_logout_and_invalid_username(self):
        headers = self.headers()
        self.assertEqual(self.client.get(PREFIX, headers={**headers, "X-Revision-User": " "}).status_code, 400)
        with self.sessions.begin() as db:
            row = db.query(site.NativeRevisionSession).one()
            row.expires_at = datetime.utcnow() - timedelta(seconds=1)
        self.assertEqual(self.client.get(PREFIX, headers=headers).status_code, 401)
        headers = self.headers()
        with self.sessions.begin() as db:
            site.set_admin_password(db, "changed-test-password")
        self.assertEqual(self.client.get(PREFIX, headers=headers).status_code, 401)
        headers = self.headers()
        self.assertEqual(self.client.post(PREFIX + "/logout", headers=headers).status_code, 200)
        self.assertEqual(self.client.get(PREFIX, headers=headers).status_code, 401)
