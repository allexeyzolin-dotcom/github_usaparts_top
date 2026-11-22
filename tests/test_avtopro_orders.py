import hashlib
import io
import json
import unittest
from decimal import Decimal
from unittest.mock import patch
from zipfile import ZipFile

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app import app as site
from app.avtopro_orders import TOKEN_KEY


class AvtoproCaptureTests(unittest.TestCase):
    token = "A" * 43
    api = "/api/integrations/avtopro/order"
    admin = "/admin/marketplaces/avtopro-orders"

    def setUp(self):
        engine = create_engine("sqlite://")
        self.addCleanup(engine.dispose)
        site.Base.metadata.create_all(engine)
        self.db = sessionmaker(bind=engine)
        self.addCleanup(patch.stopall)
        patch.object(site, "SessionLocal", self.db).start()
        patch.object(site, "track_daily_guest_visit").start()
        self.client = site.app.test_client()
        with self.db.begin() as db:
            site.set_setting(db, TOKEN_KEY, hashlib.sha256(self.token.encode()).hexdigest())
            warehouse = site.Warehouse(name="Тестовий склад")
            db.add(warehouse)
            db.flush()
            db.add(site.Part(warehouse_id=warehouse.id, part_number="01234AB", brand="VAG", name="Ручка", qty=3, in_stock=True))

    def login(self):
        with self.client.session_transaction() as session:
            session["admin_auth"] = True

    def send(self, **updates):
        payload = {"orderId": "12345678", "sourceUrl": "https://avtopro.ua/orders/12345678/", "customer": "Тестовий клієнт",
                   "currency": "UAH", "status": "Замовлення формується", "totalUah": "839.55",
                   "delivery": "Самовивіз", "payment": "При отриманні", "phone": "",
                   "items": [{"partNumber": "01234AB", "brand": "VAG", "name": "Ручка дверей", "qty": 2, "unitUah": "419.77"}]}
        return self.client.post(self.api, json={**payload, **updates}, headers={"Authorization": f"Bearer {self.token}"})

    def test_first_capture_preserves_price_matches_by_brand_and_does_not_reserve(self):
        response = self.send()
        self.assertEqual(response.status_code, 201)
        self.assertEqual(self.send().json["result"], "unchanged")
        with self.db() as db:
            order = db.query(site.Order).one()
            self.assertEqual(order.external_order_id, "12345678")
            self.assertEqual(order.items[0].part_number, "01234AB")
            self.assertEqual(order.items[0].qty, 2)
            self.assertEqual(order.items[0].part_id, db.query(site.Part).one().id)
            self.assertEqual(order.total_usd, Decimal("20.23"))
            self.assertFalse(order.stock_reserved)
            self.assertEqual(db.query(site.Part).one().qty, 3)
        self.login()
        self.assertIn("839.55 грн", self.client.get("/admin/orders?q=12345678").get_data(as_text=True))
        self.assertIn("419.77 грн", self.client.get(self.admin).get_data(as_text=True))

    def test_cancellation_moves_to_history_and_preserves_original_order_items(self):
        order_id = self.send().json["orderId"]
        self.assertEqual(self.send(status="Замовлення 12345678 скасовано покупцем", statusOnly=True).json["result"], "cancelled")
        self.login()
        self.client.post(f"/admin/orders/{order_id}/packing")
        with self.db() as db:
            order = db.query(site.Order).one()
            self.assertEqual(order.status, "cancelled")
            self.assertEqual(order.items[0].qty, 2)
            self.assertEqual(order.total_usd, Decimal("20.23"))
            self.assertEqual(db.query(site.PackingRequest).count(), 0)
        self.assertNotIn(f'id="order-{order_id}"', self.client.get("/admin/orders").get_data(as_text=True))
        self.assertIn(f'id="order-{order_id}"', self.client.get("/admin/orders?group=history").get_data(as_text=True))

    def test_cancellation_releases_before_shipment_once_and_closes_packing(self):
        order_id = self.send().json["orderId"]
        with self.db.begin() as db:
            order = db.get(site.Order, order_id)
            site.reserve_order_inventory(db, order)
            order.status = "processing"
            pack = site.PackingRequest(source_order_id=order_id, source_type="order", status="packed")
            db.add(pack)
        self.assertEqual(self.send(status="Скасовано покупцем").json["result"], "cancelled")
        self.assertEqual(self.send(status="Скасовано покупцем").json["result"], "unchanged")
        self.assertEqual(self.send().json["result"], "unchanged")
        with self.db() as db:
            order = db.get(site.Order, order_id)
            self.assertEqual(order.status, "cancelled")
            self.assertFalse(order.stock_reserved)
            self.assertEqual(db.query(site.Part).one().qty, 3)
            self.assertEqual(db.query(site.PackingRequest).one().status, "deleted")

    def test_cancellation_of_shipped_order_repeatedly_requires_confirmed_return(self):
        order_id = self.send().json["orderId"]
        with self.db.begin() as db:
            order = db.get(site.Order, order_id)
            site.reserve_order_inventory(db, order)
        self.send(status="Замовлення очікує доставки або отримання")
        for _ in range(2):
            self.assertEqual(self.send(status="Скасовано покупцем", statusOnly=True).json["result"], "return_review")
        with self.db() as db:
            order = db.get(site.Order, order_id)
            self.assertTrue(order.stock_reserved)
            self.assertTrue(site.avtopro_order_details(db, [order])[order_id]["cancelled"])
            self.assertEqual(db.query(site.Part).one().qty, 1)

    def test_status_only_rejects_unknown_order_and_an_ambiguous_warning(self):
        self.assertEqual(self.send(status="Скасовано покупцем", statusOnly=True).status_code, 422)
        self.send()
        self.assertEqual(self.send(status="Замовлення 99999999 скасовано покупцем", statusOnly=True).status_code, 422)
        self.assertEqual(self.send(status="Замовлення може бути скасовано", statusOnly=True).status_code, 422)

    def test_wrong_brand_cannot_fall_back_to_arbitrary_warehouse_item(self):
        order = self.send(items=[{"partNumber": "01234AB", "brand": "BMW", "name": "Інший виробник", "qty": 1, "unitUah": "1"}])
        self.assertEqual(order.status_code, 201)
        with self.db() as db:
            item = db.query(site.OrderItem).one()
            self.assertIsNone(item.part_id)
            self.assertIsNone(site.find_part_for_order_item(db, item))

    def test_source_validation_privacy_and_download_package(self):
        self.assertEqual(self.client.post(self.api, json={}).status_code, 401)
        self.assertEqual(self.send(sourceUrl="https://avtopro.ua.evil.invalid/orders/12345678/").status_code, 422)
        self.assertEqual(self.send(sourceUrl="https://avtopro.ua/orders/12345679/").status_code, 422)
        self.assertEqual(self.send(items=[]).status_code, 422)
        self.assertEqual(self.send(currency="USD").status_code, 422)
        self.assertEqual(self.client.get(self.admin).status_code, 302)
        self.login()
        self.assertEqual(self.client.post(self.admin, data={}).status_code, 403)
        self.assertNotIn(self.token, self.client.get(self.admin).get_data(as_text=True))
        with ZipFile(io.BytesIO(self.client.get("/admin/marketplaces/avtopro-extension.zip").data)) as archive:
            manifest = json.loads(archive.read("usaparts-avtopro/manifest.json"))
            self.assertEqual(manifest["host_permissions"], ["https://usaparts.top/*"])
            self.assertNotIn("cookies", manifest["permissions"])

    def test_first_shipped_capture_uses_shipped_lane(self):
        result = self.send(status="Замовлення очікує доставки або отримання").json
        self.assertIn("group=new", result["orderUrl"])
        with self.db() as db:
            self.assertEqual(db.query(site.Order).one().status, "shipped")

    def test_admin_can_replay_the_same_capture_without_pairing_an_extension(self):
        sent = self.send()
        self.login()
        self.client.get(self.admin)
        with self.client.session_transaction() as session:
            csrf = session["avtopro_import_csrf"]
        with self.db() as db:
            data = site.avtopro_order_details(db, db.query(site.Order).all())[sent.json["orderId"]]["data"]
        response = self.client.post(self.admin, data={"action": "import", "csrf_token": csrf,
            "capture": json.dumps({**data, "currency": "UAH"})})
        self.assertEqual(response.status_code, 302)
        self.assertIn("#order-", response.location)
        with self.db() as db:
            self.assertEqual(db.query(site.Order).count(), 1)
