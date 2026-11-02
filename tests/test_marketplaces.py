"""Inventory and integration regressions against disposable SQLite and a fake Prom."""
import copy
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from datetime import datetime, timedelta
from decimal import Decimal
from unittest.mock import Mock, patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import app.app as site
from app import marketplaces as mp
from app.prom_api import PromClient, PromError, money, quantity
from app.server_secrets import session_secret


class FakeProm:
    def __init__(self, products=None, orders=None):
        self.products = {str(row["id"]): copy.deepcopy(row) for row in (products or [])}
        self.orders = orders or []
        self.writes = []

    def pages(self, resource, **params):
        return iter(copy.deepcopy(list(self.products.values()) if resource == "products" else self.orders))

    def product(self, identifier):
        return copy.deepcopy(self.products[str(identifier)])

    def stock(self, identifier, qty):
        self.writes.append((str(identifier), qty))
        row = self.products[str(identifier)]
        row.update(quantity_in_stock=qty, presence="available" if qty else "not_available")

    def close(self):
        pass


class MarketplaceTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite://")
        site.Base.metadata.create_all(self.engine)
        self.sessions = sessionmaker(bind=self.engine, expire_on_commit=False)
        self.patch = patch.object(site, "SessionLocal", self.sessions)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        self.addCleanup(self.engine.dispose)
        self.addCleanup(patch.stopall)
        self.old_secret = site.app.secret_key
        site.app.secret_key = "test-marketplaces-secret-key-at-least-32"
        self.addCleanup(setattr, site.app, "secret_key", self.old_secret)
        site.app.config["TESTING"] = True
        patch.object(site, "track_daily_guest_visit").start()
        patch.object(site, "queue_part_inventory_change").start()
        self.client = site.app.test_client()
        self.start = datetime.utcnow() - timedelta(minutes=10)
        with self.sessions.begin() as db:
            db.add_all([site.Warehouse(id=1, name="Основний"), site.Warehouse(id=2, name="Інший")])
            db.add_all([site.Part(id=1, warehouse_id=1, part_number="OEM-01", name="Запчастина", qty=3,
                                in_stock=True, price_usd=10),
                        site.Part(id=2, warehouse_id=1, part_number="DUP", name="Дубль", qty=1, in_stock=True),
                        site.Part(id=3, warehouse_id=2, part_number="DUP", name="Дубль", qty=1, in_stock=True)])
            mp.put(db, "prom_started_at", self.start.isoformat())
            mp.put(db, "prom_connected", "1")
        self.remote = {"id": 3244000001, "external_id": "640442898363528", "sku": "OEM-01",
                       "name": "Запчастина Prom", "quantity_in_stock": 3, "price": 415,
                       "currency": "UAH", "presence": "available", "status": "on_display"}
        self.fake = FakeProm([self.remote])
        with self.sessions() as db:
            mp.catalog(db, self.fake, datetime.utcnow(), full=True)

    def raw(self, **overrides):
        row = {"id": 437000001, "date_created": (self.start + timedelta(minutes=1)).isoformat(),
               "date_modified": datetime.utcnow().isoformat(), "status": "pending", "status_name": "Нове",
               "client_first_name": "Тест", "client_last_name": "Покупець", "phone": "TEST_PHONE",
               "delivery_address": "Тестова адреса", "delivery_option": {"id": 1, "name": "Нова пошта"},
               "delivery_provider_data": {"provider": "nova_poshta", "recipient_warehouse_id": "abc"},
               "full_price": "4\u00a0416 грн", "products": [{"id": self.remote["id"],
                "sku": "OEM-01", "name": "Запчастина", "quantity": 1.0, "price": "4\u00a0416 грн"}]}
        row.update(overrides)
        return row

    def part_qty(self):
        with self.sessions() as db:
            return db.get(site.Part, 1).qty

    def process(self, row, **kwargs):
        with self.sessions() as db:
            return mp.process_order(db, row, **kwargs)

    def login(self):
        with self.client.session_transaction() as state:
            state["admin_auth"] = True

    def post(self, action, **fields):
        self.login()
        self.client.get("/admin/marketplaces?tab=prom")
        with self.client.session_transaction() as state:
            csrf = state["marketplace_csrf"]
        return self.client.post("/admin/marketplaces?tab=prom", data={"action": action, "csrf_token": csrf, **fields})

    def test_money_uses_uah_and_accepts_real_prom_format_without_truncation(self):
        self.assertEqual(money("4\u00a0416 грн"), Decimal("4416.00"))
        self.assertEqual(money("184,35 грн"), Decimal("184.35"))
        for value in ["NaN", "Infinity", "-5", "150 USD"]:
            with self.assertRaises(PromError):
                money(value)
        self.assertEqual(quantity(1.0), 1)
        for value in [1.5, -1, "nan", None]:
            with self.assertRaises(PromError):
                quantity(value)

    def test_new_order_reserves_exactly_once_and_keeps_original_uah_total(self):
        row = self.raw()
        self.assertEqual(self.process(row), "created")
        self.assertEqual(self.part_qty(), 2)
        self.assertEqual(self.process(row), "unchanged")
        self.assertEqual(self.part_qty(), 2)
        with self.sessions() as db:
            state = db.query(mp.MODELS.OrderState).one()
            order = db.get(site.Order, state.order_id)
            self.assertEqual(state.total_uah, Decimal("4416.00"))
            self.assertNotEqual(order.total_usd, state.total_uah)
            self.assertTrue(order.stock_reserved)
            self.assertEqual(order.items[0].part_id, 1)
            self.assertEqual(db.query(mp.MODELS.Event).filter_by(notify=True).count(), 1)

    def test_cancel_releases_only_our_reservation_once(self):
        row = self.raw()
        self.process(row)
        row.update(status="canceled", date_modified=(datetime.utcnow()+timedelta(seconds=1)).isoformat())
        self.process(row)
        self.assertEqual(self.part_qty(), 3)
        self.process(row, retry=True)
        self.assertEqual(self.part_qty(), 3)

    def test_historical_orders_do_not_deduct_and_cancellation_does_not_create_stock(self):
        row = self.raw(date_created=(self.start-timedelta(days=1)).isoformat(), status="received")
        self.process(row)
        self.assertEqual(self.part_qty(), 3)
        row.update(status="canceled", date_modified=(datetime.utcnow()+timedelta(seconds=1)).isoformat())
        self.process(row)
        self.assertEqual(self.part_qty(), 3)
        with self.sessions() as db:
            self.assertEqual(db.query(mp.MODELS.Event).filter_by(notify=True).count(), 0)

    def test_historical_active_order_can_be_explicitly_adopted(self):
        row = self.raw(date_created=(self.start-timedelta(days=1)).isoformat(), status="received")
        self.process(row)
        self.process(row, retry=True, adopt=True)
        self.assertEqual(self.part_qty(), 2)
        self.process(row, retry=True, adopt=True)
        self.assertEqual(self.part_qty(), 2)

    def test_edit_changes_only_delta_and_shortage_rolls_back_old_reserve(self):
        row = self.raw()
        self.process(row)
        row["products"][0]["quantity"] = 2
        row["date_modified"] = (datetime.utcnow()+timedelta(seconds=1)).isoformat()
        self.process(row)
        self.assertEqual(self.part_qty(), 1)
        row["products"][0]["quantity"] = 6
        row["date_modified"] = (datetime.utcnow()+timedelta(seconds=2)).isoformat()
        self.assertEqual(self.process(row), "issue")
        self.assertEqual(self.part_qty(), 1)
        with self.sessions() as db:
            order = db.query(site.Order).one()
            self.assertTrue(order.stock_reserved)
            self.assertEqual(order.items[0].qty, 2)

    def test_shipped_cancel_requires_return_confirmation_on_every_retry(self):
        row = self.raw()
        self.process(row)
        with self.sessions.begin() as db:
            db.query(site.Order).one().status = "shipped"
        row.update(status="canceled", date_modified=(datetime.utcnow()+timedelta(seconds=1)).isoformat())
        self.assertEqual(self.process(row), "issue")
        self.assertEqual(self.part_qty(), 2)
        self.assertEqual(self.process(row, retry=True), "issue")
        self.assertEqual(self.part_qty(), 2)
        self.process(row, retry=True, confirm_return=True)
        self.assertEqual(self.part_qty(), 3)
        self.process(row, retry=True)
        self.assertEqual(self.part_qty(), 3)

    def test_unmapped_or_unreconciled_lines_cannot_use_first_sku_match(self):
        row = self.raw()
        row["products"][0]["id"] = 999
        self.assertEqual(self.process(row), "issue")
        self.assertEqual(self.part_qty(), 3)
        with self.sessions() as db:
            item = db.query(site.OrderItem).one()
            self.assertIsNone(site.find_part_for_order_item(db, item))
        with self.sessions.begin() as db:
            link = db.query(mp.MODELS.Link).one()
            link.stock_ready = False
        row = self.raw(id=437000002)
        self.assertEqual(self.process(row), "issue")
        self.assertEqual(self.part_qty(), 3)

    def test_unknown_status_and_fractional_units_are_quarantined(self):
        self.assertEqual(self.process(self.raw(status="custom-status")), "issue")
        row = self.raw(id=437000002)
        row["products"][0]["quantity"] = 1.5
        self.assertEqual(self.process(row), "issue")
        self.assertEqual(self.part_qty(), 3)

    def test_old_snapshot_cannot_reopen_a_canceled_order(self):
        row = self.raw()
        self.process(row)
        canceled = copy.deepcopy(row)
        canceled.update(status="canceled", date_modified=(datetime.utcnow()+timedelta(minutes=1)).isoformat())
        self.process(canceled)
        self.process(row)
        self.assertEqual(self.part_qty(), 3)
        with self.sessions() as db:
            self.assertEqual(db.query(mp.MODELS.OrderState).one().status, "canceled")

    def test_unique_sku_mapping_never_chooses_between_warehouses_or_duplicates(self):
        duplicate = {**self.remote, "id": 3244000002, "sku": "DUP", "quantity_in_stock": 1}
        with self.sessions() as db:
            mp.catalog(db, FakeProm([duplicate]), datetime.utcnow())
            self.assertIsNone(db.query(mp.MODELS.Link).filter_by(remote_id=str(duplicate["id"])).one().part_id)

    def test_zero_stock_and_site_sale_are_written_with_readback(self):
        with self.sessions.begin() as db:
            part = db.get(site.Part, 1)
            part.qty = 0
            part.in_stock = False
        with self.sessions() as db:
            self.assertEqual(mp.sync_stock(db, self.fake), 1)
            link = db.query(mp.MODELS.Link).one()
            self.assertEqual(link.synced_qty, 0)
            self.assertEqual(self.fake.products[link.remote_id]["presence"], "not_available")
            self.assertEqual(mp.sync_stock(db, self.fake), 0)
        self.assertEqual(self.fake.writes, [(str(self.remote["id"]), 0)])

    def test_remote_manual_edit_is_not_silently_overwritten(self):
        self.fake.products[str(self.remote["id"])]["quantity_in_stock"] = 9
        with self.sessions.begin() as db:
            db.query(mp.MODELS.Link).one().remote_qty = 9
        with self.sessions() as db:
            self.assertEqual(mp.sync_stock(db, self.fake), 0)
            self.assertTrue(db.query(mp.MODELS.Link).one().issue)
        self.assertEqual(self.fake.writes, [])
        self.assertEqual(self.part_qty(), 3)

    def test_already_deducted_prom_stock_does_not_double_deduct(self):
        self.process(self.raw())
        self.fake.products[str(self.remote["id"])]["quantity_in_stock"] = 2
        with self.sessions() as db:
            mp.sync_stock(db, self.fake)
        self.assertEqual(self.fake.writes, [])
        self.assertEqual(self.part_qty(), 2)

    def test_bad_order_does_not_block_valid_ones_or_advance_inventory(self):
        invalid = self.raw(full_price="unknown currency")
        valid = self.raw(id=437000002)
        self.fake.orders = [invalid, valid]
        with self.sessions() as db:
            counts = mp.sync_orders(db, self.fake, datetime.utcnow())
            self.assertEqual(counts["issue"], 1)
            self.assertEqual(counts["created"], 1)
            self.assertTrue(mp.setting(db, "prom_orders_cursor"))
        self.assertEqual(self.part_qty(), 2)

    def test_private_pages_and_search_require_admin_and_csrf(self):
        self.assertEqual(self.client.get("/admin/marketplaces").status_code, 302)
        self.assertEqual(self.client.get("/admin/marketplaces/parts?q=OEM").status_code, 302)
        self.login()
        self.assertEqual(self.client.post("/admin/marketplaces", data={"action":"sync"}).status_code, 403)
        for tab in ["overview", "prom", "products", "orders", "journal", "avtopro"]:
            response = self.client.get("/admin/marketplaces?tab=" + tab)
            self.assertEqual(response.status_code, 200, tab)
            self.assertEqual(response.headers["Cache-Control"], "no-store")
        self.assertEqual(self.client.get("/admin/marketplaces/parts?q=OEM").json["results"][0]["id"], 1)

    def test_token_is_encrypted_and_never_rendered_back_to_browser(self):
        secret = "TEST_TOKEN_NOT_A_REAL_CREDENTIAL_12345678"
        fake_client = Mock()
        fake_client.request.return_value = {"products": [], "orders": []}
        with patch.object(mp, "PromClient", return_value=fake_client), patch.object(mp, "kick"):
            self.assertEqual(self.post("connect", prom_token=secret).status_code, 302)
        with self.sessions() as db:
            self.assertNotIn(secret, mp.setting(db, "prom_token"))
            self.assertEqual(mp.token(db), secret)
        self.assertNotIn(secret, self.client.get("/admin/marketplaces?tab=prom").text)

    def test_example_session_secret_uses_persistent_random_encryption_key(self):
        with TemporaryDirectory() as directory, patch.object(site, "BACKUP_DIR", Path(directory)):
            site.app.secret_key = "change-me-to-a-long-random-secret"
            first = mp.cipher()
            encrypted = first.encrypt(b"PRIVATE_FAKE_TOKEN")
            key_file = Path(directory) / ".marketplace-encryption.key"
            original = key_file.read_bytes()
            self.assertEqual(mp.cipher().decrypt(encrypted), b"PRIVATE_FAKE_TOKEN")
            self.assertEqual(key_file.read_bytes(), original)
            self.assertNotIn(b"PRIVATE_FAKE_TOKEN", original)
            key_file.write_bytes(b"invalid-key")
            with self.assertRaises(PromError):
                mp.cipher()
            self.assertEqual(key_file.read_bytes(), b"invalid-key")

    def test_production_example_session_secret_is_replaced_once_and_retained(self):
        with TemporaryDirectory() as directory:
            example = "change-me-to-a-long-random-secret"
            created = session_secret(example, directory, production=True)
            self.assertGreaterEqual(len(created), 40)
            self.assertNotEqual(created, example)
            self.assertEqual(session_secret(example, directory, production=True), created)
            self.assertEqual(session_secret("strong-existing-secret-at-least-32", directory, production=True),
                             "strong-existing-secret-at-least-32")
            self.assertEqual(session_secret(example, directory), example)

    def test_populated_orders_and_journal_render_with_escaped_product_names(self):
        row = self.raw()
        row["products"][0]["name"] = '<script>alert("not executable")</script>'
        self.process(row)
        self.login()
        for tab in ["overview", "orders", "journal"]:
            response = self.client.get("/admin/marketplaces?tab=" + tab)
            self.assertEqual(response.status_code, 200)
        rendered = self.client.get("/admin/marketplaces?tab=orders").text
        self.assertIn("&lt;script&gt;", rendered)
        self.assertNotIn('<script>alert("not executable")', rendered)

    def test_idempotent_absolute_write_requires_processed_id(self):
        transport = Mock()
        response = Mock(status_code=200)
        response.json.return_value = {"processed_ids": [], "errors": {"1":"rejected"}}
        transport.request.return_value = response
        client = PromClient("FAKE_TOKEN_FOR_LOCAL_TESTS_123456", transport=transport)
        with self.assertRaises(PromError):
            client.stock(1, 0)
        response.json.return_value = {"processed_ids": [1], "errors": {}}
        client.stock(1, 0)
        payload = transport.request.call_args.kwargs["json"]
        self.assertEqual(payload, [{"id":1,"quantity_in_stock":0,"presence":"not_available"}])
        self.assertFalse(transport.request.call_args.kwargs["allow_redirects"])

    def test_transport_never_leaks_credentials_or_follows_redirects(self):
        transport = Mock()
        transport.request.return_value = Mock(status_code=403)
        client = PromClient("FAKE_TOKEN_FOR_LOCAL_TESTS_123456", transport=transport)
        with self.assertRaises(PromError) as error:
            client.request("GET", "/orders/list")
        self.assertNotIn("FAKE_TOKEN", str(error.exception))
        with self.assertRaises(PromError):
            client.request("GET", "https://example.com")

    def test_pagination_reaches_more_than_one_hundred_without_duplicates(self):
        transport = Mock()
        first, second = Mock(status_code=200), Mock(status_code=200)
        first.json.return_value = {"orders":[{"id":i} for i in range(300,200,-1)]}
        second.json.return_value = {"orders":[{"id":200}]}
        transport.request.side_effect = [first, second]
        client = PromClient("FAKE_TOKEN_FOR_LOCAL_TESTS_123456", transport=transport)
        self.assertEqual(len(list(client.pages("orders", sort_dir="desc"))), 101)
        self.assertEqual(transport.request.call_args.kwargs["params"]["last_id"], 200)

    def test_complete_worker_persists_run_cursors_and_survives_restart(self):
        self.fake.orders = [self.raw()]
        with patch.object(mp, "PromClient", return_value=self.fake), patch.object(site, "engine", self.engine):
            self.assertTrue(mp.run_sync())
            self.assertTrue(mp.run_sync())
        self.assertEqual(self.part_qty(), 2)
        self.assertEqual(self.fake.writes, [(str(self.remote["id"]), 2)])
        with self.sessions() as db:
            self.assertTrue(mp.setting(db, "prom_orders_cursor"))
            self.assertEqual(db.query(mp.MODELS.Run).filter_by(status="success").count(), 2)

    def test_failed_worker_keeps_order_cursor_and_records_retry_time(self):
        self.fake.pages = Mock(side_effect=PromError("Ліміт запитів Prom"))
        with self.sessions.begin() as db:
            mp.put(db, "prom_orders_cursor", "2026-11-01T00:00:00")
        with patch.object(mp, "PromClient", return_value=self.fake), patch.object(site, "engine", self.engine):
            self.assertFalse(mp.run_sync())
        with self.sessions() as db:
            self.assertEqual(mp.setting(db, "prom_orders_cursor"), "2026-11-01T00:00:00")
            self.assertTrue(mp.setting(db, "prom_retry_after"))
            self.assertEqual(db.query(mp.MODELS.Run).one().status, "error")

    def test_permission_probe_preserves_quantity_and_reads_it_back(self):
        with self.sessions() as db:
            self.assertEqual(mp.verify_stock_access(db, self.fake), 3)
            self.assertTrue(mp.setting(db, "prom_write_verified"))
            self.assertFalse(mp.setting(db, "prom_zero_verified"))
        self.assertEqual(self.part_qty(), 3)
        self.assertEqual(self.fake.products[str(self.remote["id"])]["quantity_in_stock"], 3)


if __name__ == "__main__":
    unittest.main()
