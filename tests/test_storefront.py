"""Regression checks against the local Docker database. No inventory writes."""
import gzip
import json
import re
import unittest
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch
from urllib.parse import urlsplit

from PIL import Image

import app.app as site


class StorefrontTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = site.app.test_client()
        cls.headers = {"User-Agent": "Googlebot (regression test)"}

    def get(self, path, **kwargs):
        return self.client.get(path, headers=self.headers, **kwargs)

    def test_unknown_entities_and_pages_are_404(self):
        for path in ["/part/999999999", "/cross/FAKE/999999999", "/cars/999999999",
                     "/brand/not-a-real-brand", "/list/not-a-real-category",
                     "/vehicle/not-a-real-model", "/catalog?page=99999999",
                     "/catalog?page=-2", "/catalog?page=bad", "/sitemap/parts-99.xml"]:
            with self.subTest(path=path):
                response = self.get(path)
                self.assertEqual(response.status_code, 404)
                self.assertNotIn("Location", response.headers)

    def test_catalog_canonical_normalization(self):
        for path in ["/catalog?q=", "/catalog?condition=invalid", "/catalog?page=1"]:
            response = self.get(path)
            self.assertEqual(response.status_code, 301)
            self.assertEqual(response.headers["Location"], site.public_url_for("catalog"))

    def test_search_and_private_pages_remain_noindex(self):
        for path in ["/catalog?q=09P321361", "/catalog?condition=used", "/cart"]:
            response = self.get(path)
            self.assertEqual(response.status_code, 200)
            self.assertIn('content="noindex,', response.text)

    def test_category_pagination_contains_all_products(self):
        # More than the old hard limit of 72; mock the renderer input, not routing.
        parts = [site.Part(id=i, part_number=f"OEM{i:04}", name=f"Detail {i}",
                           qty=1, price_usd=1, in_stock=True, is_deleted=False,
                           brand="BMW", producer_type="OEM", photo_urls="",
                           showcase_photo_urls="", created_at=datetime(2026, 1, 1),
                           updated_at=datetime(2026, 1, 1)) for i in range(1, 111)]
        seen = set()
        for page in [1, 2, 3]:
            path = "/brand/test" + (f"?page={page}" if page > 1 else "")
            with site.app.test_request_context(path):
                html = site.render_seo_listing(parts, "Test", "Description", site.public_site_base_url() + "/brand/test")
                page_ids = {int(identifier) for identifier in re.findall(r'href="/part/(\d+)/', html)}
                self.assertFalse(seen & page_ids)
                self.assertLessEqual(len(page_ids), 48)
                seen.update(page_ids)
                expected = site.public_site_base_url() + path
                self.assertIn(f'rel="canonical" href="{expected}"', html)
                self.assertIn('content="index,follow"', html)
                if page < 3:
                    self.assertIn('rel="next"', html)
        self.assertEqual(len(seen), 110)

    def test_no_photo_products_are_discoverable_without_fake_schema_image(self):
        part = site.Part(id=777, part_number="NO-PHOTO-777", name="Detail", qty=1,
                         price_usd=1, in_stock=True, is_deleted=False, photo_urls="",
                         showcase_photo_urls="", brand="BMW", producer_type="OEM")
        shown, total = site.build_showcase_parts([part])
        self.assertEqual(total, 1)
        self.assertEqual(shown[0].id, 777)
        with site.app.test_request_context("/"):
            graph = json.loads(site.build_part_product_schema(part, None))["@graph"]
            product = next(item for item in graph if item["@type"] == "Product")
            self.assertNotIn("image", product)
            self.assertEqual(product["sku"], "NO-PHOTO-777")
            self.assertEqual(product["offers"]["priceCurrency"], "UAH")

    def test_exact_oem_match_is_first(self):
        exact = site.Part(id=1, part_number="09P321361", name="Pan", qty=1, in_stock=True,
                          is_deleted=False, price_usd=10, photo_urls="", showcase_photo_urls="")
        related = site.Part(id=2, part_number="OTHER", name="Gasket for 09P321361", qty=1,
                            in_stock=True, is_deleted=False, price_usd=1, photo_urls="", showcase_photo_urls="")
        shown, count = site.build_showcase_parts([related, exact], "09P321361")
        self.assertEqual(count, 2)
        self.assertEqual(shown[0].id, exact.id)

    def test_public_cache_clears_after_admin_changes(self):
        key = ("regression", "invalidation")
        site.public_cache_set(key, "old catalog")
        with site.app.test_request_context("/admin/parts/1", method="POST"):
            site.apply_crawl_headers(site.Response("updated", status=200))
        self.assertIsNone(site.public_cache_get(key))

    def test_pagination_schema_matches_visible_page(self):
        db = site.SessionLocal()
        try:
            parts = site.cached_catalog_active_parts(db)
            if len(parts) < 49:
                self.skipTest("Local inventory has fewer than 49 products")
        finally:
            db.close()
        html = self.get("/catalog?page=2").text
        raw = html.split('<script type="application/ld+json">', 1)[1].split("</script>", 1)[0]
        collection = next(item for item in json.loads(raw)["@graph"] if item["@type"] == "CollectionPage")
        cards = html.split('<div class="showcase-grid">', 1)[1]
        for item in collection["mainEntity"]["itemListElement"]:
            self.assertIn(urlsplit(item["url"]).path, cards)

    def test_sitemap_etag_and_gzip(self):
        response = self.get("/sitemap.xml")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.mimetype, "application/xml")
        self.assertIn("ETag", response.headers)
        revalidated = self.client.get("/sitemap.xml", headers={**self.headers, "If-None-Match": response.headers["ETag"]})
        self.assertEqual(revalidated.status_code, 304)
        compressed = self.client.get("/catalog", headers={**self.headers, "Accept-Encoding": "gzip"})
        self.assertEqual(compressed.headers.get("Content-Encoding"), "gzip")
        self.assertIn(b"<!doctype html>", gzip.decompress(compressed.data))

    def test_thumbnail_is_bounded_and_updates_with_source(self):
        from tempfile import TemporaryDirectory
        with TemporaryDirectory() as folder, patch.object(site, "UPLOAD_DIR", Path(folder)):
            source = Path(folder) / "sample.jpg"
            Image.new("RGB", (1400, 1000), "white").save(source)
            response = self.get("/media/320/sample.jpg")
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.mimetype, "image/webp")
            old_etag = response.headers["ETag"]
            import io
            with Image.open(io.BytesIO(response.data)) as image:
                self.assertLessEqual(max(image.size), 320)
            response.close()
            Image.new("RGB", (1400, 1000), "black").save(source)
            with self.get("/media/320/sample.jpg") as updated:
                self.assertNotEqual(updated.headers["ETag"], old_etag)
            self.assertEqual(self.get("/media/999/sample.jpg").status_code, 404)
            self.assertEqual(self.get("/media/320/../../requirements.txt").status_code, 404)
            self.assertEqual(self.get("/uploads/../../requirements.txt").status_code, 404)

    def test_admin_navigation_counts_not_loaded_on_storefront(self):
        with site.app.test_request_context("/"):
            site.session["admin_auth"] = True
            with patch.object(site, "SessionLocal", side_effect=AssertionError("Admin counts queried on public page")):
                site.inject_globals()

    def test_cart_prices_match_storefront_and_order_uses_selling_price(self):
        part = site.Part(id=777, part_number="PRICE-777", name="Detail", qty=3,
                         price_usd=Decimal("10.01"), in_stock=True, is_deleted=False,
                         warehouse_id=1, brand="BMW")
        warehouse = site.Warehouse(id=1, name="Test", markup_percent=Decimal("7.5"))
        db = MagicMock()
        db.query.return_value.filter.return_value.all.side_effect = [[part], [warehouse]]
        state = site.build_cart_state(db, {"777": 2})
        price_usd = Decimal(site.display_usd(part.price_usd, warehouse.markup_percent))
        price_uah = Decimal(site.display_uah(part.price_usd, warehouse.markup_percent))
        self.assertEqual(price_usd, Decimal("10.76"))
        self.assertEqual(state["items"][0]["unit_price_usd"], price_usd)
        self.assertEqual(state["items"][0]["unit_price_uah"], price_uah)
        self.assertEqual(state["total"], price_usd * 2)
        self.assertEqual(state["total_uah"], price_uah * 2)
        self.assertEqual(part.price_usd, Decimal("10.01"))
        with site.app.test_request_context("/cart"):
            html = site.render_template("cart.html", items=state["items"], total=state["total"], total_uah=state["total_uah"])
            self.assertIn(f'{state["total_uah"]:.2f} грн', html)
            self.assertIn('aria-modal="true"', html)
            self.assertNotIn("Ціна: $", html)

    def test_top_level_pages_have_heading(self):
        for path in ["/cars", "/delivery-calculator"]:
            response = self.get(path)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(len(re.findall(r"<h1[ >]", response.text)), 1)

    def test_favicons_and_crawl_rules(self):
        import io
        from urllib.robotparser import RobotFileParser
        robots = RobotFileParser()
        robots.parse(self.get("/robots.txt").text.splitlines())
        for path, size in [("/favicon-48.png", 48), ("/favicon-96.png", 96), ("/favicon.png", 192)]:
            response = self.get(path)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.mimetype, "image/png")
            with Image.open(io.BytesIO(response.data)) as picture:
                self.assertEqual(picture.size, (size, size))
            response.close()
            self.assertTrue(robots.can_fetch("Googlebot-Image", path))
        for path in ["/part/123/oem", "/catalog?page=2", "/list/bampery-kryla-kuzov?page=2", "/media/320/photo.jpg"]:
            self.assertTrue(robots.can_fetch("Googlebot", path))
        self.assertFalse(robots.can_fetch("Googlebot", "/admin/parts"))

    def test_checkout_persists_retail_usd_without_network_or_stock_writes(self):
        part = site.Part(id=777, part_number="PRICE-777", name="Detail", qty=3,
                         price_usd=Decimal("10.01"), in_stock=True, is_deleted=False)
        price = Decimal("10.76")
        snapshot = {"items": [{"part": part, "qty": 2, "unit_price_usd": price}],
                    "total": price * 2, "normalized_cart": {"777": 2}, "issues": []}
        db = MagicMock()
        with site.app.test_request_context("/checkout", method="POST", data={"customer_name": "Test"}):
            site.session["cart"] = {"777": 2}
            with patch.object(site, "SessionLocal", return_value=db), \
                 patch.object(site, "build_cart_state", return_value=snapshot), \
                 patch.object(site, "reserve_order_inventory"), \
                 patch.object(site, "track_stats_event"), \
                 patch.object(site, "flash_news"), \
                 patch.object(site, "send_telegram_message"):
                response = site.checkout()
            self.assertEqual(response.status_code, 302)
            order = db.add.call_args.args[0]
            self.assertEqual(order.total_usd, price * 2)
            self.assertEqual(order.items[0].price_usd, price)
            self.assertEqual(part.price_usd, Decimal("10.01"))


if __name__ == "__main__":
    unittest.main()
