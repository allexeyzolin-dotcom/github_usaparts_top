"""Merchant delivery against disposable stock; no live Google calls or credentials."""
from datetime import datetime, timedelta
from decimal import Decimal
from io import BytesIO
from pathlib import Path
import json
import tempfile
import unittest
from unittest.mock import patch

from PIL import Image
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import app.app as site
from app import merchant_revision as merchant


class Response:
    def __init__(self, status=200, payload=None, headers=None):
        self.status_code, self.payload, self.headers = status, payload or {}, headers or {}
        self.ok = status < 400
        self.text = json.dumps(self.payload)
    def json(self):
        return self.payload
    def raise_for_status(self):
        if not self.ok:
            raise merchant.requests.HTTPError("test API failure")


class MerchantRevisionTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite://")
        site.Base.metadata.create_all(self.engine)
        self.sessions = sessionmaker(bind=self.engine, expire_on_commit=False)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.engine.dispose)
        self.addCleanup(self.temp.cleanup)
        self.addCleanup(patch.stopall)
        patch.object(site, "engine", self.engine).start()
        patch.object(site, "SessionLocal", self.sessions).start()
        patch.object(site, "UPLOAD_DIR", Path(self.temp.name)).start()
        patch.object(site, "track_daily_guest_visit").start()
        patch.object(site, "google_merchant_request_headers", return_value={"Authorization": "Bearer MOCK"}).start()
        self.post = patch.object(merchant.requests, "post", return_value=Response(payload={})).start()
        self.get = patch.object(merchant.requests, "get", return_value=Response(404)).start()
        Image.new("RGB", (640, 600), "blue").save(Path(self.temp.name) / "original.png")
        self.config = {"enabled": True, "account_id": "123", "data_source_id": "456", "content_language": "uk",
                       "feed_label": "UA", "service_account_json": "MOCK"}
        with self.sessions.begin() as db:
            for key, value in self.config.items():
                site.set_setting(db, "google_merchant_" + key, str(int(value)) if isinstance(value, bool) else value)
            site.set_setting(db, "merchant_revision_batch", "2")
            site.set_setting(db, "merchant_revision_enabled", "1")
        self.client = site.app.test_client()
        site.app.config["TESTING"] = True
        site.public_cache_clear()

    def card(self, number="OEM001", qty=2, price=10, brand="VAG", **changes):
        with self.sessions.begin() as db:
            row = site.RevisionPart(part_number=number, part_key=number, name="Ручка дверей", brand=brand,
                price_usd=price, qty=qty, photo_urls='["/uploads/original.png"]', barcode="12345678", **changes)
            db.add(row)
            db.flush()
            site.publish_revision_part(db, row)
            return row.id

    def jobs(self):
        with self.sessions() as db:
            return db.query(merchant.MODEL).order_by(merchant.MODEL.id).all()

    def test_payload_matches_storefront_price_and_all_available_fields(self):
        self.card(description="Оригінальна ручка, права.")
        with self.sessions() as db:
            part = db.query(site.Part).one()
            part.warehouse.markup_percent = 15
            payload = merchant.prepare_payload(part, self.config)
            attrs = payload["productAttributes"]
            self.assertEqual(attrs["price"]["amountMicros"], site.google_merchant_price_micros(site.display_uah(part.price_usd, 15)))
            self.assertEqual(attrs["price"]["currencyCode"], "UAH")
            self.assertIn("Оригінальна ручка", attrs["description"])
            self.assertEqual(attrs["googleProductCategory"], "899")
            self.assertEqual(attrs["brand"], "VAG")
            self.assertEqual(attrs["mpn"], "OEM001")
            self.assertTrue(attrs["identifierExists"])
            self.assertNotIn("gtin", attrs)
            self.assertNotIn("salePriceEffectiveDate", attrs)
            self.assertEqual(attrs["shipping"][0]["country"], "UA")
            self.assertTrue(attrs["productTypes"][0].startswith("Автозапчастини"))
            self.assertEqual(payload["offerId"], site.google_merchant_offer_id(part))

    def test_batch_checkpoints_survive_restart_without_resending_first_items(self):
        for index in range(5):
            self.card(number=f"OEM{index}")
        merchant.run_batch(force=True)
        self.assertEqual(self.post.call_count, 2)
        merchant.run_batch(force=True)
        self.assertEqual(self.post.call_count, 4)
        merchant.run_batch(force=True)
        self.assertEqual(self.post.call_count, 5)
        merchant.run_batch(force=True)
        self.assertEqual(self.post.call_count, 5)
        self.assertEqual(sum(j.state == "synced" for j in self.jobs()), 5)
        self.assertEqual(len({call.kwargs["json"]["offerId"] for call in self.post.call_args_list}), 5)

    def test_google_acceptance_is_pending_until_processed_status_says_approved(self):
        self.card()
        self.get.return_value = Response(payload={"productStatus": {"destinationStatuses": [
            {"reportingContext": "FREE_LISTINGS", "pendingCountries": ["UA"]}]}})
        merchant.run_batch(force=True)
        self.assertEqual(self.jobs()[0].google_state, "pending")
        state, issues = merchant.approval({"productStatus": {"destinationStatuses": [
            {"reportingContext": "FREE_LISTINGS", "approvedCountries": ["UA"]}]}})
        self.assertEqual(state, "approved")

    def test_existing_google_zero_stock_is_withdrawn_after_catalog_reconciliation(self):
        self.card(qty=0)
        merchant.run_batch(force=True)
        self.assertEqual(self.jobs()[0].state, "skipped")
        with self.sessions.begin() as db:
            site.set_setting(db, "merchant_revision_import_active", "1")
        product = {"name": "accounts/123/products/uk~UA~OEM001", "offerId": "OEM001", "contentLanguage": "uk", "feedLabel": "UA", "dataSource": "accounts/123/dataSources/456"}
        self.get.side_effect = lambda url, **kwargs: Response(payload={"products": [product]}) if url.endswith("/products") else Response(404)
        merchant.run_batch(force=True)
        self.assertEqual(self.post.call_count, 1)
        self.assertEqual(self.post.call_args.kwargs["json"]["productAttributes"]["availability"], "OUT_OF_STOCK")

    def test_disapproval_preserves_machine_code_attribute_and_explanation(self):
        state, issues = merchant.approval({"productStatus": {"destinationStatuses": [
            {"reportingContext": "FREE_LISTINGS", "disapprovedCountries": ["UA"]}], "itemLevelIssues": [
            {"code": "image_link_broken", "attribute": "imageLink", "description": "Broken image", "severity": "DISAPPROVED"}]}})
        self.assertEqual(state, "disapproved")
        self.assertEqual(json.loads(issues)[0]["attribute"], "imageLink")

    def test_transient_failure_does_not_lose_card_or_block_other_cards_forever(self):
        self.card()
        self.post.return_value = Response(429, {"error": {"message": "Too many requests"}}, {"Retry-After": "900"})
        merchant.run_batch(force=True)
        job = self.jobs()[0]
        self.assertEqual(job.state, "retry")
        self.assertGreater(job.next_at, datetime.utcnow() + timedelta(minutes=14))
        merchant.run_batch(force=True)
        self.assertEqual(self.post.call_count, 1)
        with self.sessions.begin() as db:
            db.get(merchant.MODEL, job.id).next_at = datetime.utcnow() - timedelta(seconds=1)
        self.post.return_value = Response()
        merchant.run_batch(force=True)
        self.assertEqual(self.jobs()[0].state, "synced")

    def test_zero_stock_withdraws_previous_offer_even_after_photo_and_price_removed(self):
        rid = self.card()
        merchant.run_batch(force=True)
        with self.sessions.begin() as db:
            revision = db.get(site.RevisionPart, rid)
            revision.qty, revision.price_usd, revision.photo_urls = 0, 0, "[]"
            revision.version += 1
            site.publish_revision_part(db, revision)
        merchant.run_batch(force=True)
        self.assertEqual(self.post.call_args.kwargs["json"]["productAttributes"]["availability"], "OUT_OF_STOCK")
        self.assertEqual(self.jobs()[0].state, "synced")
        merchant.run_batch(force=True)
        self.assertEqual(self.post.call_count, 2)

    def test_card_changed_during_http_is_queued_on_next_discovery(self):
        rid = self.card()
        def concurrent_edit(*args, **kwargs):
            with self.sessions.begin() as db:
                revision = db.get(site.RevisionPart, rid)
                revision.price_usd, revision.version = 17, revision.version + 1
                site.publish_revision_part(db, revision)
            return Response()
        self.post.side_effect = concurrent_edit
        merchant.run_batch(force=True)
        self.post.side_effect = None
        merchant.run_batch(force=True)
        self.assertEqual(self.post.call_count, 2)
        self.assertEqual(self.post.call_args.kwargs["json"]["productAttributes"]["price"]["amountMicros"], site.google_merchant_price_micros(site.display_uah(17, 0)))

    def test_invalid_price_brand_photo_and_colliding_ids_never_reach_google(self):
        self.card(number="NOPRICE", price=0)
        self.card(number="NOBRAND", brand="")
        first = self.card(number="DUP.A")
        self.card(number="DUPA")
        missing = self.card(number="NOPHOTO")
        with self.sessions.begin() as db:
            row = db.get(site.RevisionPart, missing)
            row.photo_urls = '[]'
            site.publish_revision_part(db, row)
        merchant.run_batch(force=True)
        self.assertEqual(self.post.call_count, 0)
        self.assertTrue(all(j.state == "blocked" for j in self.jobs()))
        self.assertTrue(any("Merchant ID" in j.error for j in self.jobs()))

    def test_changed_destination_gets_separate_queue(self):
        self.card()
        merchant.run_batch(force=True)
        with self.sessions.begin() as db:
            site.set_setting(db, "google_merchant_data_source_id", "789")
        merchant.run_batch(force=True)
        self.assertEqual(len(self.jobs()), 2)
        self.assertEqual(self.post.call_count, 2)
        self.assertEqual(self.post.call_args.kwargs["params"]["dataSource"], "accounts/123/dataSources/789")

    def test_same_product_uses_exact_public_winner_without_overwriting_one_offer_twice(self):
        self.card(number="SAME", price=10)
        self.card(number="SAME", price=12)
        with self.sessions() as db:
            preferred = site.best_unique_public_parts(db.query(site.Part).all())[0]
            expected_price = site.google_merchant_price_micros(site.display_uah(preferred.price_usd, preferred.warehouse.markup_percent))
        merchant.run_batch(force=True)
        self.assertEqual(self.post.call_count, 1)
        self.assertEqual(self.post.call_args.kwargs["json"]["productAttributes"]["price"]["amountMicros"], expected_price)
        self.assertEqual(sorted(j.state for j in self.jobs()), ["skipped", "synced"])

    def test_images_are_real_jpeg_public_cacheable_and_not_upscaled(self):
        url, warning = merchant.cache_image(site.absolute_public_url("/uploads/original.png"))
        file = site.UPLOAD_DIR / "merchant" / url.rsplit("/", 1)[-1]
        with Image.open(file) as image:
            self.assertEqual(image.format, "JPEG")
            self.assertEqual(image.size, (640, 600))
        self.assertEqual(warning, "")
        response = self.client.get("/uploads/merchant/" + file.name)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.mimetype, "image/jpeg")
        self.assertIn("public", response.headers["Cache-Control"])
        self.assertNotIn("X-Robots-Tag", response.headers)

    def test_original_upload_is_used_when_showcase_photo_is_stale(self):
        self.card()
        with self.sessions.begin() as db:
            part = db.query(site.Part).one()
            part.showcase_photo_urls = '["https://i.avto.pro/broken.jpg"]'
        original_loader = merchant.image_bytes
        def load_photo(url):
            if "i.avto.pro" in url:
                raise merchant.requests.HTTPError("photo 404")
            return original_loader(url)
        with patch.object(merchant, "image_bytes", side_effect=load_photo) as load:
            merchant.run_batch(force=True)
        self.assertEqual(self.jobs()[0].state, "synced")
        self.assertIn("/uploads/original.png", load.call_args_list[0].args[0])
        self.assertIn("/uploads/merchant/", self.post.call_args.kwargs["json"]["productAttributes"]["imageLink"])

    def test_bad_image_origins_traversal_and_corrupt_files_are_blocked(self):
        for url in ("https://127.0.0.1/photo.jpg", "http://i.avto.pro/a.jpg", "https://i.avto.pro.evil.test/a.jpg",
                    "https://i.avto.pro:8443/a.jpg", "https://usaparts.top/static/product-photo-placeholder.png",
                    "https://usaparts.top/uploads/%2e%2e/app.py"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                merchant.cache_image(url)
        (site.UPLOAD_DIR / "broken.jpg").write_text("NOT A PHOTO")
        with self.assertRaises(ValueError):
            merchant.cache_image(site.absolute_public_url("/uploads/broken.jpg"))
        self.get.assert_not_called()

    def test_small_real_image_warns_without_inventing_resolution(self):
        Image.new("RGB", (300, 300), "red").save(site.UPLOAD_DIR / "small.jpg")
        url, warning = merchant.cache_image(site.absolute_public_url("/uploads/small.jpg"))
        self.assertIn("500×500", warning)
        with Image.open(site.UPLOAD_DIR / "merchant" / url.rsplit("/", 1)[-1]) as image:
            self.assertEqual(image.size, (300, 300))

    def test_existing_google_import_is_scoped_to_exact_source_and_does_not_add_unlisted_stock(self):
        self.card()
        with self.sessions.begin() as db:
            warehouse = site.Warehouse(name="Старий склад")
            db.add(warehouse)
            db.flush()
            db.add_all([site.Part(id=99, warehouse_id=warehouse.id, part_number="OLD", name="Стара деталь", brand="VAG", price_usd=10,
                qty=1, in_stock=True, photo_urls='["/uploads/original.png"]'),
                site.Part(id=100, warehouse_id=warehouse.id, part_number="UNLISTED", name="Не передавати", qty=1, in_stock=True)])
            site.set_setting(db, "merchant_revision_import_active", "1")
        product = {"name": "accounts/123/products/uk~UA~OLD", "offerId": "OLD", "contentLanguage": "uk", "feedLabel": "UA", "dataSource": "accounts/123/dataSources/456"}
        self.get.side_effect = lambda url, **kwargs: Response(payload={"products": [product, {**product, "offerId": "UNLISTED", "dataSource": "accounts/123/dataSources/999"}]}) if url.endswith("/products") else Response(404)
        merchant.run_batch(force=True)
        self.assertEqual({j.revision_id for j in self.jobs()}, {1, -99})
        legacy = [j for j in self.jobs() if j.revision_id < 0][0]
        self.assertNotEqual(json.loads(legacy.source_json)["productAttributes"]["customLabel0"], "Перевірена наявність")

    def test_dashboard_auth_csrf_and_credential_redaction(self):
        self.assertEqual(self.client.get("/admin/revision/google").status_code, 302)
        with self.client.session_transaction() as state:
            state["admin_auth"] = True
        response = self.client.get("/admin/revision/google")
        self.assertEqual(response.status_code, 200)
        self.assertNotIn("Bearer MOCK", response.text)
        self.assertIn("no-store", response.headers["Cache-Control"])
        self.assertEqual(self.client.post("/admin/revision/google", data={"action": "settings", "enabled": "1"}).status_code, 403)
        with self.client.session_transaction() as state:
            token = state["merchant_revision_csrf"]
        self.client.post("/admin/revision/google", data={"action": "settings", "csrf_token": token, "enabled": "1", "batch": "5000", "interval": "1"})
        with self.sessions() as db:
            settings = merchant.options(site.get_api_settings_map(db))
            self.assertEqual(settings, {"enabled": True, "batch": 100, "interval": 60})

    def test_periodic_refresh_and_disabled_scheduler(self):
        self.card()
        merchant.run_batch(force=True)
        with self.sessions.begin() as db:
            db.query(merchant.MODEL).one().sent_at = datetime.utcnow() - timedelta(days=15)
            site.set_setting(db, "merchant_revision_enabled", "0")
        merchant.run_batch()
        self.assertEqual(self.post.call_count, 1)
        merchant.run_batch(force=True)
        self.assertEqual(self.post.call_count, 2)


if __name__ == "__main__":
    unittest.main()
