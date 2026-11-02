"""Run only against the explicitly named, disposable local PostgreSQL QA database."""
import os
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from urllib.parse import urlsplit
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import app.app as site
from app import marketplaces as mp

URL = os.getenv("PROM_POSTGRES_TEST_URL", "")


@unittest.skipUnless(URL, "Dedicated PostgreSQL QA URL not supplied")
class MarketplaceConcurrencyTests(unittest.TestCase):
    def setUp(self):
        parsed = urlsplit(URL)
        if (parsed.hostname, parsed.port, parsed.path) != ("127.0.0.1", 55439, "/marketplaces_qa"):
            self.fail("Refusing to run destructive fixtures outside the dedicated local QA database")
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
            db.flush()
            db.add(mp.MODELS.Link(remote_id="3244000001", part_id=1, sku="LAST-UNIT", stock_ready=True,
                                  remote_qty=1, synced_qty=1, presence="available"))
            mp.put(db, "prom_started_at", (datetime.utcnow()-timedelta(minutes=5)).isoformat())

    def raw(self, status="pending"):
        return {"id":437000001,"date_created":(datetime.utcnow()-timedelta(minutes=1)).isoformat(),
                "date_modified":datetime.utcnow().isoformat(),"status":status,"full_price":"415 грн",
                "products":[{"id":3244000001,"sku":"LAST-UNIT","name":"QA", "quantity":1,"price":"415 грн"}]}

    def website_sale(self, barrier):
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

    def test_prom_and_website_cannot_both_sell_the_last_unit(self):
        barrier = threading.Barrier(2)
        def prom_sale():
            with self.sessions() as db:
                barrier.wait(timeout=10)
                return mp.process_order(db, self.raw())
        with ThreadPoolExecutor(max_workers=2) as pool:
            jobs = [pool.submit(self.website_sale, barrier), pool.submit(prom_sale)]
            results = [job.result(timeout=20) for job in jobs]
        with self.sessions() as db:
            self.assertEqual(db.get(site.Part, 1).qty, 0, results)
            self.assertEqual(db.query(site.Order).filter_by(stock_reserved=True).count(), 1, results)

    def test_two_website_reservations_cannot_overwrite_each_other(self):
        barrier = threading.Barrier(2)
        with ThreadPoolExecutor(max_workers=2) as pool:
            jobs = [pool.submit(self.website_sale, barrier) for _ in range(2)]
            results = [job.result(timeout=20) for job in jobs]
        self.assertEqual(sorted(results), ["reserved", "shortage"])
        with self.sessions() as db:
            self.assertEqual(db.get(site.Part, 1).qty, 0)

    def test_concurrent_cancel_events_return_stock_only_once(self):
        with self.sessions() as db:
            mp.process_order(db, self.raw())
        row = self.raw("canceled")
        barrier = threading.Barrier(2)
        def cancel():
            with self.sessions() as db:
                barrier.wait(timeout=10)
                return mp.process_order(db, row)
        with ThreadPoolExecutor(max_workers=2) as pool:
            jobs = [pool.submit(cancel) for _ in range(2)]
            results = [job.result(timeout=20) for job in jobs]
        with self.sessions() as db:
            self.assertEqual(db.get(site.Part, 1).qty, 1, results)


if __name__ == "__main__":
    unittest.main()
