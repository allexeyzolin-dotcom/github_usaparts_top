"""Concurrency checks restricted to the explicitly isolated PostgreSQL QA database."""
import os
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlsplit
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

import app.app as site
from app import telegram_stock as bot


URL = os.getenv("PROM_POSTGRES_TEST_URL", "")


@unittest.skipUnless(URL, "Dedicated PostgreSQL QA URL not supplied")
class TelegramStockConcurrencyTests(unittest.TestCase):
    def setUp(self):
        parsed = urlsplit(URL)
        if (parsed.hostname, parsed.port, parsed.path) != ("127.0.0.1", 55439, "/marketplaces_qa"):
            self.fail("Refusing destructive fixtures outside the dedicated local QA database")
        self.engine = create_engine(URL, pool_size=5)
        site.Base.metadata.drop_all(self.engine)
        site.Base.metadata.create_all(self.engine)
        self.sessions = sessionmaker(bind=self.engine, expire_on_commit=False)
        self.db_patch = patch.object(site, "SessionLocal", self.sessions)
        self.db_patch.start()
        self.addCleanup(self.db_patch.stop)
        self.addCleanup(self.engine.dispose)
        with self.sessions.begin() as db:
            db.add(site.Warehouse(id=1, name="QA ONLY"))
            db.flush()
            db.add(site.Part(id=1, warehouse_id=1, part_number="LAST-UNIT", qty=1, in_stock=True, price_usd=10))
            site.set_setting(db, "telegram_stock_recent_chats", "[]")

    def receipt(self, db, update_id, delta):
        row = bot.MODELS.Command(bot_id="777", update_id=update_id, chat_id="-123", sender_id="55",
                                 message_id=update_id, nonce=f"{update_id:016d}", identifier="LAST-UNIT", delta=delta)
        db.add(row)
        db.flush()
        return row

    def test_bot_command_and_website_cannot_both_take_last_unit(self):
        barrier = threading.Barrier(2)
        def command():
            with self.sessions() as db:
                row = self.receipt(db, 10, -1)
                barrier.wait(timeout=10)
                try:
                    bot.apply_to_part(db, row, 1)
                    db.commit()
                    return "reserved"
                except bot.CommandError:
                    db.rollback()
                    return "shortage"
        def sale():
            with self.sessions() as db:
                order = site.Order(customer_name="QA ONLY")
                db.add(order)
                order.items.append(site.OrderItem(part_id=1, part_number="LAST-UNIT", name="QA", qty=1, price_usd=10))
                db.flush()
                barrier.wait(timeout=10)
                try:
                    site.reserve_order_inventory(db, order)
                    db.commit()
                    return "reserved"
                except ValueError:
                    db.rollback()
                    return "shortage"
        with ThreadPoolExecutor(max_workers=2) as pool:
            jobs = [pool.submit(command), pool.submit(sale)]
            results = [job.result(timeout=20) for job in jobs]
        self.assertEqual(sorted(results), ["reserved", "shortage"])
        with self.sessions() as db:
            self.assertEqual(db.get(site.Part, 1).qty, 0)
            movements = db.query(bot.MODELS.Command).filter_by(status="applied").count()
            movements += db.query(site.Order).filter_by(stock_reserved=True).count()
            self.assertEqual(movements, 1)

    def test_simultaneous_additions_do_not_overwrite_each_other(self):
        barrier = threading.Barrier(2)
        def add(update_id, delta):
            with self.sessions() as db:
                row = self.receipt(db, update_id, delta)
                barrier.wait(timeout=10)
                bot.apply_to_part(db, row, 1)
                db.commit()
        with ThreadPoolExecutor(max_workers=2) as pool:
            jobs = [pool.submit(add, 10, 1), pool.submit(add, 11, 2)]
            for job in jobs:
                job.result(timeout=20)
        with self.sessions() as db:
            self.assertEqual(db.get(site.Part, 1).qty, 4)
            self.assertEqual(db.query(bot.MODELS.Command).filter_by(status="applied").count(), 2)

    def test_duplicate_update_in_two_transactions_changes_quantity_once(self):
        barrier = threading.Barrier(2)
        config = {"enabled": True, "chat_id": "-123", "started_at": 0}
        update = {"update_id": 10, "message": {"message_id": 10, "date": int(time.time()),
                   "chat": {"id": -123, "type": "group"}, "from": {"id": 55}, "text": "+LAST-UNIT"}}
        def receive():
            with self.sessions() as db:
                barrier.wait(timeout=10)
                try:
                    result = bot.handle_update(db, update, "777", config)
                    db.commit()
                    return result
                except IntegrityError:
                    db.rollback()
                    return "duplicate"
        with ThreadPoolExecutor(max_workers=2) as pool:
            jobs = [pool.submit(receive) for _ in range(2)]
            for job in jobs:
                job.result(timeout=20)
        with self.sessions() as db:
            self.assertEqual(db.get(site.Part, 1).qty, 2)
            self.assertEqual(db.query(bot.MODELS.Command).count(), 1)


if __name__ == "__main__":
    unittest.main()
