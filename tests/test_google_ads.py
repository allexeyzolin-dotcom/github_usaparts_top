"""Purchase tracking in disposable SQLite; no real orders, stock or network calls."""
import json
import re
import unittest
from decimal import Decimal
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import app.app as site


class GoogleAdsTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite://")
        site.Base.metadata.create_all(self.engine)
        self.sessions = sessionmaker(bind=self.engine)
        for target, replacement in [("SessionLocal", self.sessions),
                                    ("track_daily_guest_visit", None),
                                    ("send_telegram_message", None)]:
            mocked = patch.object(site, target, replacement) if replacement else patch.object(site, target)
            mocked.start()
            self.addCleanup(mocked.stop)
        self.addCleanup(self.engine.dispose)
        site.app.config["TESTING"] = True
        self.client = site.app.test_client()
        with self.sessions.begin() as db:
            db.add(site.Warehouse(id=1, name="Test", markup_percent=Decimal("7.5")))
            db.add(site.Part(id=1, warehouse_id=1, part_number="ADS-TEST", name="Test part",
                             price_usd=Decimal("10.01"), qty=10, in_stock=True))

    def set_cart(self, qty=2):
        with self.client.session_transaction() as state:
            state["cart"] = {"1": qty}

    def expected_value(self, qty=2):
        with self.sessions() as db:
            snapshot = site.build_cart_state(db, {"1": qty})
            return float(snapshot.get("total_uah", site.display_uah(snapshot["total"], 0)))

    def checkout(self):
        return self.client.post("/checkout", data={
            "customer_name": "Private Customer", "phone": "+380501234567",
            "city_name": "Kyiv", "delivery_type": "warehouse", "warehouse_label": "1",
        })

    def conversion(self, response):
        matches = re.findall(r"gtag\('event', 'conversion', (\{[^\n]+\})\);", response.text)
        self.assertEqual(len(matches), 1)
        return json.loads(matches[0])

    def assert_no_conversion(self, response):
        self.assertNotIn("gtag('event', 'conversion'", response.text)

    def test_public_pages_configure_ads_without_recording_purchases(self):
        for path in ("/", "/catalog", "/cart"):
            response = self.client.get(path)
            self.assertEqual(response.status_code, 200)
            head = response.text.split("</head>")[0]
            self.assertEqual(head.count("gtag('config', 'AW-16746839372')"), 1)
            self.assertEqual(head.count("gtag('config', 'G-6NBBNH4VT8')"), 1)
            self.assert_no_conversion(response)

    def test_committed_order_has_exact_cart_value_and_unique_order_id(self):
        expected = self.expected_value()
        self.set_cart()
        response = self.checkout()
        self.assertEqual(response.location, "/checkout/success")
        response = self.client.get(response.location)
        self.assertEqual(response.status_code, 200)
        payload = self.conversion(response)
        with self.sessions() as db:
            order = db.query(site.Order).one()
            self.assertTrue(order.stock_reserved)
            self.assertEqual(payload["transaction_id"], str(order.id))
        self.assertEqual(payload["value"], expected)
        self.assertEqual(payload["currency"], "UAH")
        self.assertEqual(payload["send_to"], "AW-16746839372/zox0CJaEzqMcEMz6wbE-")
        self.assertNotIn("Private Customer", response.text)
        self.assertNotIn("+380501234567", response.text)
        self.assertIn("no-store", response.headers["Cache-Control"])
        self.assertIn("noindex", response.headers["X-Robots-Tag"])
        self.assertLess(response.text.index("gtag('config', 'AW-"), response.text.index("gtag('event', 'conversion'"))
        self.assertLess(response.text.index("gtag('event', 'conversion'"), response.text.index("</head>"))
        self.assert_no_conversion(self.client.get("/checkout/success"))
        self.assert_no_conversion(self.client.get("/"))

    def test_second_order_is_independent_and_has_different_id(self):
        identifiers = []
        for qty in (2, 1):
            expected = self.expected_value(qty)
            self.set_cart(qty)
            response = self.checkout()
            payload = self.conversion(self.client.get(response.location))
            identifiers.append(payload["transaction_id"])
            self.assertEqual(payload["value"], expected)
        self.assertNotEqual(*identifiers)

    def test_value_keeps_cart_rounding_for_multiple_units(self):
        with self.sessions.begin() as db:
            db.get(site.Part, 1).price_usd = Decimal("0.11")
        expected = self.expected_value(3)
        self.set_cart(3)
        response = self.checkout()
        self.assertEqual(self.conversion(self.client.get(response.location))["value"], expected)

    def test_empty_cart_and_direct_success_visit_cannot_record_purchase(self):
        self.assertEqual(self.checkout().location, "/cart")
        self.assertEqual(self.client.get("/checkout/success?transaction_id=999&value=999").location, "/")
        self.assert_no_conversion(self.client.get("/"))
        with self.sessions() as db:
            self.assertEqual(db.query(site.Order).count(), 0)

    def test_stock_adjustment_requires_confirmation_without_conversion(self):
        self.set_cart(99)
        self.assertEqual(self.checkout().location, "/cart")
        self.assertEqual(self.client.get("/checkout/success").location, "/")
        with self.sessions() as db:
            self.assertEqual(db.query(site.Order).count(), 0)

    def test_failed_inventory_reservation_cannot_record_purchase(self):
        self.set_cart()
        with patch.object(site, "reserve_order_inventory", side_effect=ValueError("stock changed")):
            self.assertEqual(self.checkout().location, "/cart")
        self.assertEqual(self.client.get("/checkout/success").location, "/")
        with self.sessions() as db:
            self.assertEqual(db.query(site.Order).count(), 0)

    def test_database_commit_failure_cannot_record_purchase(self):
        self.set_cart()
        db = self.sessions()
        with patch.object(site, "SessionLocal", return_value=db), \
             patch.object(db, "commit", side_effect=RuntimeError("Database unavailable")):
            with self.assertRaises(RuntimeError):
                self.checkout()
        self.assertEqual(self.client.get("/checkout/success").location, "/")
        with self.sessions() as db:
            self.assertEqual(db.query(site.Order).count(), 0)

    def test_telegram_failure_still_tracks_committed_order(self):
        self.set_cart()
        with patch.object(site, "send_telegram_message", side_effect=RuntimeError("Telegram unavailable")):
            response = self.checkout()
        self.conversion(self.client.get(response.location))


if __name__ == "__main__":
    unittest.main()
