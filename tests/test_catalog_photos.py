"""Reviewed reference images must stay separate from real inventory photos."""
import json
import re
import unittest
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import app.app as site
from app.catalog_photos import CATALOG_PHOTOS


class CatalogPhotoTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite://")
        site.Base.metadata.create_all(self.engine)
        self.sessions = sessionmaker(bind=self.engine)
        self.db_patch = patch.object(site, "SessionLocal", self.sessions)
        self.db_patch.start()
        self.track_patch = patch.object(site, "track_daily_guest_visit")
        self.track_patch.start()
        site.public_cache_clear()
        self.client = site.app.test_client()
        with self.sessions.begin() as db:
            db.add(site.Warehouse(id=1, name="Test", markup_percent=0))
            for identifier, ref in CATALOG_PHOTOS.items():
                db.add(site.Part(id=identifier, warehouse_id=1,
                                 part_number=ref["part_number"], brand=ref["brand"],
                                 name="Тестова запчастина", price_usd=20, qty=1,
                                 in_stock=True, photo_urls="", showcase_photo_urls="[]"))

    def tearDown(self):
        self.db_patch.stop()
        self.track_patch.stop()
        self.engine.dispose()
        site.public_cache_clear()

    def test_reviewed_cards_have_consistent_gallery_schema_and_sitemap(self):
        with self.sessions() as db, site.app.test_request_context("/"):
            images_xml = "".join(site.build_sitemap_inventory_documents()["images"])
            for identifier, ref in CATALOG_PHOTOS.items():
                part = db.get(site.Part, identifier)
                response = self.client.get(site.part_detail_url(part), headers={"User-Agent": "Googlebot"})
                self.assertEqual(response.status_code, 200)
                graph = json.loads(re.search(r'<script type="application/ld\+json">(.*?)</script>', response.text, re.S).group(1))["@graph"]
                product = next(item for item in graph if item["@type"] == "Product")
                self.assertEqual(product["image"], [site.absolute_public_url(ref["image"])])
                self.assertIn(ref["image"], images_xml)
                self.assertIn(ref["source_url"], response.text)
                self.assertIn("Ілюстративне фото за артикулом", response.text)
                self.assertIn("catalog-reference-photo", response.text)
                self.assertEqual(site.part_gallery_urls(part), [])
                self.assertEqual(part.qty, 1)
                self.assertEqual(part.price_usd, 20)
                with self.client.get(ref["image"]) as asset:
                    self.assertEqual(asset.status_code, 200)
                    self.assertEqual(asset.mimetype, "image/webp")

    def test_real_photos_always_override_the_reference(self):
        with self.sessions() as db:
            part = db.get(site.Part, 3869)
            part.showcase_photo_urls = '["/uploads/actual-rail.jpg"]'
            self.assertIsNone(site.part_catalog_photo(part))
            self.assertEqual(site.storefront_part_gallery_urls(part), ["/uploads/actual-rail.jpg"])
            part.showcase_photo_urls = "[]"
            part.photo_urls = "/uploads/export-rail.jpg"
            self.assertIsNone(site.part_catalog_photo(part))
            self.assertEqual(site.storefront_part_gallery_urls(part), ["/uploads/export-rail.jpg"])

    def test_id_number_and_brand_must_all_match(self):
        for changes in ({"id": 9999}, {"part_number": "04627892AC"}, {"brand": "OTHER"}):
            values = dict(id=3869, part_number="04627892AB", brand="CHRYSLER")
            values.update(changes)
            part = site.Part(**values)
            self.assertIsNone(site.part_catalog_photo(part))
            self.assertEqual(site.public_part_gallery_urls(part), [site.PUBLIC_PART_PLACEHOLDER_IMAGE])

    def test_references_do_not_enter_inventory_or_export_galleries(self):
        with self.sessions() as db:
            for part in db.query(site.Part):
                self.assertEqual(site.primary_part_photo(part), "")
                self.assertEqual(site.part_gallery_urls(part), [])
                self.assertFalse(part.has_photo)
                self.assertEqual(part.showcase_photo_urls, "[]")


if __name__ == "__main__":
    unittest.main()
