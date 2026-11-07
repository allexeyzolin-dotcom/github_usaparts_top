"""Counted inventory workflows against a disposable DB and upload directory."""
import csv
import io
import json
import tempfile
import unittest
from urllib.parse import quote
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from PIL import Image
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import app.app as site


class InventoryRevisionTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite://")
        site.Base.metadata.create_all(self.engine)
        self.sessions = sessionmaker(bind=self.engine)
        self.temp = tempfile.TemporaryDirectory()
        self.patches = [patch.object(site, "SessionLocal", self.sessions),
                        patch.object(site, "UPLOAD_DIR", Path(self.temp.name)),
                        patch.object(site, "track_daily_guest_visit")]
        for current in self.patches:
            current.start()
        site.app.config["TESTING"] = True
        self.client = site.app.test_client()
        with self.sessions.begin() as db:
            db.add_all([
                site.Warehouse(id=1, name="Головний склад"),
                site.Part(id=1, warehouse_id=1, part_number="06G-127-028H", name="Паливний насос",
                          brand="VAG", qty=40, in_stock=True, barcode="12345678", price_usd=20,
                          description="Опис оригіналу", photo_urls='["/uploads/original.png"]',
                          showcase_photo_urls='["https://example.test/gallery.jpg"]'),
                site.Part(id=2, warehouse_id=1, part_number="DELETED-123", name="Видалений", is_deleted=True),
                site.PartTemplate(id=1, part_number="TEMPLATE-123", name="Картка без складу", brand="BMW",
                                  cross_numbers_json='["CROSS-111"]'),
                site.PartTemplate(id=2, part_number="06G-127-028H", name="Паливний насос", brand="VAG",
                                  barcode="12345678", price_usd=20, storage_location="A-12",
                                  description="Опис оригіналу", photo_urls='["/uploads/original.png"]',
                                  showcase_photo_urls='["https://example.test/gallery.jpg"]'),
            ])
        Image.new("RGB", (4, 4), "blue").save(Path(self.temp.name) / "original.png")

    def tearDown(self):
        for current in reversed(self.patches):
            current.stop()
        self.engine.dispose()
        self.temp.cleanup()

    def login(self):
        with self.client.session_transaction() as state:
            state["admin_auth"] = True
            state["revision_token"] = "revision-test-token"

    def save(self, payload=None, **changes):
        data = {"action": "create", "request_id": str(uuid4()), "part_number": "REV-001", "name": "Перевірена деталь",
                "qty": "2", "price_usd": "12.50", "photo_urls": [], "producer_type": "OEM", "location": "A-12"}
        data.update(payload or {})
        data.update(changes)
        return self.client.post("/admin/revision/save", json=data, headers={"X-Revision-Token": "revision-test-token"})

    def snapshot_main(self):
        with self.sessions() as db:
            return {model.__tablename__: [tuple(getattr(row, column.name) for column in model.__table__.columns)
                    for row in db.query(model).order_by(model.id)] for model in (site.Part, site.PartTemplate, site.Warehouse)}

    def test_authentication_and_csrf_for_every_entry_point(self):
        for url in ("/admin/revision", "/admin/revision/search?q=123", "/admin/revision/export"):
            self.assertEqual(self.client.get(url).status_code, 302)
        self.assertEqual(self.save().status_code, 302)
        self.assertEqual(self.client.post("/admin/revision/photos").status_code, 302)
        self.login()
        self.assertEqual(self.client.post("/admin/revision/save", json={}).status_code, 403)
        self.assertEqual(self.client.post("/admin/revision/photos").status_code, 403)
        with self.sessions() as db:
            self.assertEqual(db.query(site.RevisionPart).count(), 0)

    def test_page_starts_empty_without_importing_or_changing_main_inventory(self):
        self.login()
        before = self.snapshot_main()
        response = self.client.get("/admin/revision")
        self.assertEqual(response.status_code, 200)
        self.assertIn("База ревізії поки порожня", response.text)
        self.assertIn('href="/admin/revision"', response.text)
        self.assertEqual(before, self.snapshot_main())

    def test_search_starts_at_three_and_reads_parts_templates_and_revision_only(self):
        self.login()
        before = self.snapshot_main()
        self.assertEqual(self.client.get("/admin/revision/search?q=06").json["catalog"], [])
        source = self.client.get("/admin/revision/search?q=06g127").json["catalog"][0]
        self.assertEqual(source["part_number"], "06G-127-028H")
        self.assertEqual(source["qty"], 0)
        self.assertEqual(source["source"], "template")
        self.assertEqual(source["warehouse_name"], "")
        self.assertEqual(source["location"], "A-12")
        self.assertEqual(len(source["photo_urls"]), 2)
        self.assertEqual(self.client.get("/admin/revision/search?q=DELETED").json["catalog"], [])
        template = self.client.get("/admin/revision/search?q=template").json["catalog"][0]
        self.assertEqual(template["cross_numbers"], "CROSS-111")
        self.assertEqual(self.client.get("/admin/revision/search?q=%25%25%25").json["catalog"], [])
        self.assertEqual(before, self.snapshot_main())

    def test_copy_is_a_snapshot_and_count_is_only_what_operator_enters(self):
        self.login()
        before = self.snapshot_main()
        source = self.client.get("/admin/revision/search?q=06g").json["catalog"][0]
        saved = self.save(source, qty="3").json["card"]
        self.assertEqual(saved["qty"], 3)
        self.assertEqual(saved["name"], source["name"])
        self.assertEqual(saved["label"], "Перевірена наявність")
        self.assertEqual(before, self.snapshot_main())
        local_photo = next(url for url in saved["photo_urls"] if url.startswith("/uploads/"))
        self.assertTrue(local_photo.startswith("/uploads/revision_"))
        with self.sessions.begin() as db:
            db.delete(db.get(site.Warehouse, 1))
            db.delete(db.get(site.PartTemplate, 1))
            db.delete(db.get(site.PartTemplate, 2))
        (Path(self.temp.name) / "original.png").unlink()
        with self.sessions() as db:
            independent = db.get(site.RevisionPart, saved["id"])
            self.assertEqual(independent.qty, 3)
            self.assertEqual(independent.warehouse_name, "")
            self.assertEqual(independent.name, "Паливний насос")
        self.assertTrue((Path(self.temp.name) / Path(local_photo).name).is_file())
        self.assertEqual(self.client.get("/admin/revision").status_code, 200)

    def test_duplicate_requires_choice_then_can_increment_or_create_another_card(self):
        self.login()
        before = self.snapshot_main()
        first = self.save(part_number="AB-123").json["card"]
        conflict = self.save(part_number="ab 123")
        self.assertEqual(conflict.status_code, 409)
        self.assertEqual(conflict.json["code"], "duplicate")
        self.assertEqual(conflict.json["duplicates"][0]["id"], first["id"])
        increment = self.save(action="increment", target_id=first["id"], part_number="AB123", qty="4", name="Не переписувати")
        self.assertEqual(increment.json["card"]["qty"], 6)
        self.assertEqual(increment.json["card"]["name"], first["name"])
        second = self.save(part_number="AB.123", duplicate_action="new_card", qty="1").json["card"]
        self.assertNotEqual(first["id"], second["id"])
        self.save(action="increment", target_id=second["id"], part_number="AB123", qty="2")
        with self.sessions() as db:
            self.assertEqual([row.qty for row in db.query(site.RevisionPart).order_by(site.RevisionPart.id)], [6, 3])
        self.assertEqual(len(self.client.get("/admin/revision/search?q=ab123").json["revision"]), 2)
        self.assertEqual(before, self.snapshot_main())

    def test_increment_retries_are_idempotent_and_cannot_target_another_article(self):
        self.login()
        first = self.save().json["card"]
        operation = {"action": "increment", "request_id": str(uuid4()), "target_id": first["id"]}
        self.assertEqual(self.save(operation).json["card"]["qty"], 4)
        repeated = self.save(operation)
        self.assertTrue(repeated.json["replayed"])
        self.assertEqual(repeated.json["card"]["qty"], 4)
        self.assertEqual(self.save(operation, qty="3").status_code, 409)
        self.assertEqual(self.save(action="increment", target_id=first["id"], part_number="OTHER").status_code, 400)
        self.assertEqual(self.save(action="increment", target_id=999).status_code, 404)

    def test_edits_handle_zero_stale_versions_and_changed_number_duplicates(self):
        self.login()
        original = self.save().json["card"]
        updated = self.save(original, action="update", target_id=original["id"], qty="0", name="Уточнена назва")
        self.assertEqual(updated.status_code, 200)
        self.assertEqual(updated.json["card"]["qty"], 0)
        self.assertEqual(self.save(original, action="update", target_id=original["id"]).status_code, 409)
        second = self.save(part_number="OTHER").json["card"]
        self.assertEqual(self.save(second, action="update", target_id=second["id"], part_number="REV001").json["code"], "duplicate")
        accepted = self.save(second, action="update", target_id=second["id"], part_number="REV001", duplicate_action="new_card")
        self.assertEqual(accepted.status_code, 200)

    def test_invalid_counts_prices_and_payloads_cannot_write(self):
        self.login()
        for changes in ({"qty": "-1"}, {"qty": "0"}, {"qty": "1.5"}, {"qty": True}, {"qty": "2147483648"},
                        {"price_usd": "NaN"}, {"price_usd": "Infinity"}, {"price_usd": "-1"}, {"price_usd": "1.001"},
                        {"name": ""}, {"part_number": "---"}, {"part_number": "A" * 256}, {"name": ["bad"]},
                        {"photo_urls": "bad"}, {"photo_urls": [None]}, {"youtube_url": "javascript:alert(1)"}, {"location": " "}):
            with self.subTest(changes=changes):
                self.assertEqual(self.save(**changes).status_code, 400)
        with self.sessions() as db:
            self.assertEqual(db.query(site.RevisionPart).count(), 0)

    def test_csv_roundtrip_and_page_escaping_with_no_main_catalog_rows(self):
        self.login()
        self.save(name='<script>alert("x")</script>', description='=HYPERLINK("bad")')
        page = self.client.get("/admin/revision")
        self.assertEqual(page.status_code, 200)
        self.assertNotIn('<script>alert("x")</script>', page.text)
        self.assertIn("&lt;script&gt;", page.text)
        exported = self.client.get("/admin/revision/export")
        rows = list(csv.DictReader(io.StringIO(exported.data.decode("utf-8-sig")), delimiter=";"))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["part_number"], "REV-001")
        self.assertEqual(rows[0]["qty"], "2")
        self.assertTrue(rows[0]["description"].startswith("'="))
        self.assertEqual(rows[0]["label"], "Перевірена наявність")

    def test_uploads_require_real_images_and_remain_independent(self):
        self.login()
        headers = {"X-Revision-Token": "revision-test-token"}
        invalid = self.client.post("/admin/revision/photos", data={"photos": (io.BytesIO(b"<script>bad</script>"), "photo.png")}, headers=headers)
        self.assertEqual(invalid.status_code, 400)
        output = io.BytesIO()
        Image.new("RGB", (8, 8), "red").save(output, format="PNG")
        response = self.client.post("/admin/revision/photos", data={"photos": (io.BytesIO(output.getvalue()), "new.png")}, headers=headers)
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json["urls"][0].startswith("/uploads/revision_"))
        saved = self.save(photo_urls=response.json["urls"]).json["card"]
        self.assertEqual(saved["photo_urls"], response.json["urls"])
        with self.client.get(saved["photo_urls"][0]) as response:
            self.assertEqual(response.status_code, 200)

    def test_website_author_is_assigned_by_server_and_cannot_be_overridden(self):
        self.login()
        result = self.save(created_by="Підміна", updated_by="Підміна", created_via="mobile")
        self.assertEqual(result.status_code, 200)
        saved = result.json["card"]
        self.assertEqual(saved["created_by"], "Сайт")
        self.assertEqual(saved["updated_by"], "Сайт")
        self.assertEqual(saved["created_via"], "website")

    def test_mobile_user_survives_reload_and_website_keeps_original_creator(self):
        self.login()
        page = self.client.get("/admin/revision?client=android", headers={"X-Revision-User": quote("Іван Петренко")})
        self.assertEqual(page.status_code, 200)
        self.assertIn("revision-mobile-client", page.text)
        first = self.save().json["card"]
        self.assertEqual(first["created_by"], "Іван Петренко")
        self.assertEqual(first["created_via"], "mobile")
        self.assertEqual(self.client.get("/admin/revision?client=android").status_code, 200)
        second = self.save(part_number="SECOND").json["card"]
        self.assertEqual(second["created_by"], "Іван Петренко")
        self.client.get("/admin/revision")
        changed = self.save(action="increment", target_id=first["id"]).json["card"]
        self.assertEqual(changed["created_by"], "Іван Петренко")
        self.assertEqual(changed["updated_by"], "Сайт")
        with self.sessions() as db:
            history = db.query(site.RevisionOperation).filter_by(part_id=first["id"]).order_by(site.RevisionOperation.created_at).all()
            self.assertEqual([operation.actor for operation in history], ["Іван Петренко", "Сайт"])
            self.assertEqual([operation.qty_delta for operation in history], [2, 2])
            self.assertEqual([operation.qty_after for operation in history], [2, 4])
        exported = self.client.get("/admin/revision/export")
        rows = list(csv.DictReader(io.StringIO(exported.data.decode("utf-8-sig")), delimiter=";"))
        self.assertEqual(rows[0]["created_by"], "Іван Петренко")
        self.assertEqual(rows[0]["updated_by"], "Сайт")

    def test_mobile_revision_requires_a_named_user(self):
        self.login()
        self.assertEqual(self.client.get("/admin/revision?client=android").status_code, 400)
        self.assertEqual(self.client.get("/admin/revision?client=android", headers={"X-Revision-User": quote("A" * 101)}).status_code, 400)

    def test_another_mobile_user_updates_quantity_without_replacing_creator(self):
        self.login()
        self.client.get("/admin/revision?client=android", headers={"X-Revision-User": quote("Іван")})
        original = self.save().json["card"]
        self.client.get("/admin/revision?client=android", headers={"X-Revision-User": quote("Олена")})
        updated = self.save(action="increment", target_id=original["id"]).json["card"]
        self.assertEqual(updated["created_by"], "Іван")
        self.assertEqual(updated["updated_by"], "Олена")


if __name__ == "__main__":
    unittest.main()
