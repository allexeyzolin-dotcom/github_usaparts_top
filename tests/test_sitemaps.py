"""Sitemap integration checks using an isolated, disposable SQLite database."""
import json
import re
import unittest
from datetime import datetime
from unittest.mock import patch
from urllib.parse import urlsplit
from urllib.robotparser import RobotFileParser
import xml.etree.ElementTree as ET

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import app.app as site
from app.sitemap_support import sitemap_documents, SITEMAP_NAMESPACE, IMAGE_NAMESPACE

NS = {"s": SITEMAP_NAMESPACE, "image": IMAGE_NAMESPACE}


class SitemapTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite://")
        site.Base.metadata.create_all(self.engine)
        self.sessions = sessionmaker(bind=self.engine)
        self.session_patch = patch.object(site, "SessionLocal", self.sessions)
        self.session_patch.start()
        self.tracking_patch = patch.object(site, "track_daily_guest_visit")
        self.tracking_patch.start()
        site.public_cache_clear()
        self.client = site.app.test_client()
        with self.sessions.begin() as db:
            db.add(site.Warehouse(id=1, name="Test", markup_percent=0))
            for part_id, number, in_stock, qty, deleted, photos in (
                (1, "SAME-OEM", True, 2, False, '["/uploads/a.jpg?x=1&y=2"]'),
                (2, "SAME-OEM", True, 1, False, "[]"),
                (3, "SOLD-OEM", False, 0, False, "[]"),
                (4, "ZERO-OEM", True, 0, False, "[]"),
                (5, "DELETED-OEM", True, 1, True, "[]"),
            ):
                db.add(site.Part(id=part_id, warehouse_id=1, part_number=number,
                                 name="Крило & деталь", in_stock=in_stock, qty=qty,
                                 is_deleted=deleted, showcase_photo_urls=photos,
                                 updated_at=datetime(2025, 7, 1), price_usd=20))
            db.add(site.Car(id=1, brand="BMW", model="X5", year=2020,
                            image_urls='["/uploads/car.jpg"]'))

    def tearDown(self):
        self.session_patch.stop()
        self.tracking_patch.stop()
        site.public_cache_clear()
        self.engine.dispose()

    def get(self, path):
        return self.client.get(path, base_url="https://usaparts.top",
                               headers={"User-Agent": "Googlebot"})

    def xml(self, path):
        response = self.get(path)
        self.assertEqual(response.status_code, 200, response.text[:300])
        self.assertEqual(response.mimetype, "application/xml")
        self.assertNotIn("noindex", response.headers.get("X-Robots-Tag", ""))
        return ET.fromstring(response.data)

    def test_all_public_cards_including_repeated_numbers_and_out_of_stock(self):
        root = self.xml("/sitemap/parts.xml")
        locations = [node.text for node in root.findall("s:url/s:loc", NS)]
        self.assertEqual([int(urlsplit(url).path.split('/')[2]) for url in locations], [1, 2, 3, 4])
        self.assertEqual(len(locations), len(set(locations)))
        for url in locations:
            self.assertTrue(url.startswith("https://usaparts.top/part/"))
            response = self.get(urlsplit(url).path)
            self.assertEqual(response.status_code, 200)
            self.assertIn(f'<link rel="canonical" href="{url}">', response.text)
            self.assertIn('content="index,follow"', response.text)
            data = re.search(r'<script type="application/ld\+json">(.*?)</script>', response.text, re.S)
            schema = json.loads(data.group(1))
            self.assertTrue(any(item.get("@type") == "Product" for item in schema["@graph"]))
        self.assertEqual(self.get("/part/5/deleted").status_code, 404)
        self.assertEqual(self.get("/part/999").status_code, 404)
        self.assertTrue(all(node.text == "2025-07-01" for node in root.findall("s:url/s:lastmod", NS)))

    def test_index_links_valid_chunks_without_nested_indexes_or_card_omissions(self):
        with patch("app.sitemap_support.SITEMAP_MAX_URLS", 2):
            root = self.xml("/sitemap.xml")
            locations = [node.text for node in root.findall("s:sitemap/s:loc", NS)]
            part_sitemaps = [url for url in locations if "/sitemap/parts-" in url]
            self.assertEqual(len(part_sitemaps), 2)
            self.assertNotIn("https://usaparts.top/sitemap/priority-parts.xml", locations)
            self.assertFalse(root.findall("s:sitemap/s:lastmod", NS))
            cards = []
            for url in part_sitemaps:
                chunk = self.xml(urlsplit(url).path)
                self.assertEqual(chunk.tag, f"{{{SITEMAP_NAMESPACE}}}urlset")
                cards.extend(node.text for node in chunk.findall("s:url/s:loc", NS))
            self.assertEqual(len(cards), 4)
            self.assertEqual(len(set(cards)), 4)
            self.assertEqual(self.xml("/sitemap/parts.xml").tag, f"{{{SITEMAP_NAMESPACE}}}sitemapindex")
            for path in ("/sitemap/parts-0.xml", "/sitemap/parts-3.xml", "/sitemap/images-9.xml"):
                self.assertEqual(self.get(path).status_code, 404)

    def test_images_are_absolute_escaped_and_robots_allows_cards(self):
        root = self.xml("/sitemap/images.xml")
        photos = [node.text for node in root.findall("s:url/image:image/image:loc", NS)]
        self.assertIn("https://usaparts.top/uploads/a.jpg?x=1&y=2", photos)
        self.assertIn("https://usaparts.top/uploads/car.jpg", photos)
        self.assertFalse(root.findall(".//image:title", NS))
        robots_response = self.get("/robots.txt")
        self.assertEqual(robots_response.status_code, 200)
        robots = RobotFileParser()
        robots.parse(robots_response.text.splitlines())
        self.assertTrue(robots.can_fetch("Googlebot", "https://usaparts.top/part/2/example"))
        self.assertTrue(robots.can_fetch("Googlebot-Image", "https://usaparts.top/uploads/a.jpg"))
        self.assertFalse(robots.can_fetch("Googlebot", "https://usaparts.top/admin/products"))
        self.assertEqual(robots.site_maps(), ["https://usaparts.top/sitemap.xml"])

    def test_cached_documents_are_refreshed_after_catalog_change(self):
        self.xml("/sitemap/parts.xml")
        with self.sessions.begin() as db:
            db.add(site.Part(id=6, warehouse_id=1, part_number="NEW", name="New"))
        with site.app.test_request_context("/admin/part/create", method="POST"):
            site.apply_crawl_headers(site.Response(status=302))
        self.assertEqual(len(self.xml("/sitemap/parts.xml").findall("s:url", NS)), 5)


class SitemapLimitTests(unittest.TestCase):
    def test_more_than_google_url_limit_keeps_every_url(self):
        nodes = (f"<url><loc>https://usaparts.top/part/{n}</loc></url>" for n in range(50_001))
        documents = sitemap_documents(nodes)
        roots = [ET.fromstring(document) for document in documents]
        self.assertEqual(sum(len(root) for root in roots), 50_001)
        self.assertTrue(all(len(root) <= 10_000 for root in roots))

    def test_uncompressed_utf8_byte_limit_splits_before_overflow(self):
        nodes = [f"<url><loc>https://example.com/{n}/{'ї' * 50}</loc></url>" for n in range(7)]
        documents = sitemap_documents(nodes, max_bytes=500)
        self.assertGreater(len(documents), 1)
        self.assertTrue(all(len(document.encode("utf-8")) <= 500 for document in documents))
        self.assertEqual(sum(len(ET.fromstring(document)) for document in documents), 7)


if __name__ == "__main__":
    unittest.main()
