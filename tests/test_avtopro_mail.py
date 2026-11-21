import hashlib
import unittest
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app import app as site
from app.avtopro_mail import TOKEN_KEY


class AvtoproMailTests(unittest.TestCase):
    path = "/api/integrations/n8n/avtopro-mail"
    token = "a" * 43

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

    def send(self, number="12345", message_id="one", subject=None, **fields):
        return self.client.post(self.path, headers={"Authorization": f"Bearer {self.token}"}, json={
            "sender": "notification@avtopro.ua", "messageId": message_id,
            "subject": subject if subject is not None else f"Нове замовлення №{number}",
            "text": "Тестове повідомлення", **fields,
        })

    def test_same_mail_and_second_notice_create_one_order_for_review(self):
        first = self.send()
        self.assertEqual(first.status_code, 201)
        self.assertEqual(self.send().json["result"], "duplicate")
        self.assertEqual(self.send(message_id="different").json["orderId"], first.json["orderId"])
        with self.db() as db:
            order = db.query(site.Order).one()
            self.assertEqual((order.external_source, order.external_order_id, order.status), ("autopro", "12345", "new"))
            self.assertFalse(order.stock_reserved)
            self.assertEqual(order.items, [])
        with self.client.session_transaction() as session:
            session["admin_auth"] = True
        board = self.client.get("/admin/orders?q=12345").get_data(as_text=True)
        self.assertIn("Потребує звірки", board)
        self.assertIn("Автопро №12345", board)

    def test_cancellation_and_payment_attach_without_changing_live_order(self):
        self.send()
        with self.db.begin() as db:
            order = db.query(site.Order).one()
            order.status, order.stock_reserved = "processing", True
        self.send(message_id="cancel", subject="Покупець скасував замовлення №12345")
        self.send(message_id="paid", subject="Покупець сплатив замовлення №12345")
        with self.db() as db:
            order = db.query(site.Order).one()
            self.assertEqual(order.status, "processing")
            self.assertTrue(order.stock_reserved)
            self.assertEqual(site.avtopro_order_notices(db, [order])[order.id], {"new", "cancelled", "paid"})

    def test_missing_conflicting_and_previously_cancelled_numbers_go_to_journal(self):
        self.assertIsNone(self.send(message_id="unknown", subject="Нове замовлення").json["orderId"])
        self.assertIsNone(self.send(message_id="conflict", text="Замовлення №54321").json["orderId"])
        self.send(message_id="cancel", subject="Замовлення №12345 скасовано")
        self.assertIsNone(self.send(message_id="late").json["orderId"])
        with self.db() as db:
            self.assertEqual(db.query(site.Order).count(), 0)

    def test_receiver_permissions_and_private_log(self):
        self.assertEqual(self.client.post(self.path, json={}).status_code, 401)
        self.assertEqual(self.send(sender="notification@avtopro.ua.example").status_code, 422)
        self.assertEqual(self.send(text="a" * 32768).status_code, 413)
        dashboard = "/admin/marketplaces/avtopro-mail"
        self.assertEqual(self.client.get(dashboard).status_code, 302)
        self.send(text="<script>untrusted email</script>")
        with self.client.session_transaction() as session:
            session["admin_auth"] = True
        self.assertEqual(self.client.post(dashboard, data={}).status_code, 403)
        response = self.client.get(dashboard)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["Cache-Control"], "no-store")
        self.assertIn("&lt;script&gt;untrusted email&lt;/script&gt;", response.get_data(as_text=True))
        self.assertNotIn(self.token, response.get_data(as_text=True))
