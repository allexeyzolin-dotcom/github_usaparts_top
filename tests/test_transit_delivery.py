"""Inbound notifications use disposable data and fake carrier/Telegram APIs."""
import json
import unittest
from datetime import date, datetime, timedelta
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import app.app as site
from app import transit_delivery as shipping


class TransitDeliveryTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite://")
        site.Base.metadata.create_all(self.engine)
        self.sessions = sessionmaker(bind=self.engine, expire_on_commit=False)
        patch.object(site, "SessionLocal", self.sessions).start()
        self.addCleanup(patch.stopall)
        self.addCleanup(self.engine.dispose)
        self.telegram = patch.object(shipping, "telegram_api", return_value={"message_id": 42}).start()
        self.np = patch.object(site, "np_request", return_value=([], {"success": True})).start()
        self.client = site.app.test_client()
        self.winter_8 = datetime(2026, 11, 17, 6, 0)  # 08:00 Europe/Kyiv
        with self.sessions.begin() as db:
            db.add(site.Warehouse(id=1, name="Тестовий склад"))
            for key, value in [("telegram_bot_token", "fake-token"), ("telegram_chat_id", "-123"),
                               ("nova_poshta_api_key", "fake-np-key")]:
                site.set_setting(db, key, value)

    def create(self, batch="batch-1", carrier="other", expected="2026-11-17", ttn="", quantities=(2, 1)):
        with self.sessions.begin() as db:
            for index, qty in enumerate(quantities, 1):
                db.add(site.TransitOrder(batch_id=batch, part_number=f"OEM-{index}", title=f"Запчастина {index}",
                         qty=qty, accepted_qty=0, short_description="Постачальник", barcode=f"{index:08d}"))
            shipping.save_shipment(db, batch, {"carrier": carrier, "carrier_name": "Водій Олексій",
                      "expected_date": expected, "ttn": ttn}, created=True)

    def codes(self, kind=None):
        with self.sessions() as db:
            query = db.query(shipping.MODELS.Notice)
            if kind:
                query = query.filter_by(kind=kind)
            return [(row.event_key, row.status) for row in query.order_by(shipping.MODELS.Notice.id).all()]

    def login(self):
        with self.client.session_transaction() as state:
            state["admin_auth"] = True

    def snapshot(self, code, **extra):
        return {"Number": "20450000000001", "StatusCode": str(code), "Status": "Тестовий статус НП",
                "CitySender": "Київ", "CityRecipient": "Ромни", "ScheduledDeliveryDate": "17-11-2026", **extra}

    def apply(self, snapshot, now=None):
        with self.sessions.begin() as db:
            shipment = db.get(shipping.MODELS.Shipment, "batch-1")
            shipping.apply_np_snapshot(db, shipment, snapshot, now or self.winter_8)

    def render(self, kind):
        with self.sessions() as db:
            notice = db.query(shipping.MODELS.Notice).filter_by(kind=kind).one()
            return "\n".join(shipping.render_notice(db.get(shipping.MODELS.Shipment, notice.batch_id),
                               notice, shipping.orders_for(db, notice.batch_id), self.winter_8))

    def test_other_carrier_requires_name_and_estimated_date(self):
        for fields in [{"carrier": "invalid"}, {"carrier": "other", "expected_date": "2026-11-17"},
                       {"carrier": "other", "carrier_name": "Водій"},
                       {"carrier": "other", "carrier_name": "Водій", "expected_date": "not-a-date"}]:
            with self.subTest(fields=fields), self.assertRaises(shipping.DeliveryError):
                shipping.validate(fields)

    def test_worker_records_successful_checks_even_without_shipments(self):
        with patch.object(site, "engine", self.engine):
            self.assertFalse(shipping.run_once(self.winter_8))
        with self.sessions() as db:
            self.assertEqual(site.get_api_settings_map(db)["transit_delivery_last_run"], self.winter_8.isoformat())

    def test_public_tracking_is_not_blocked_by_an_empty_api_key(self):
        self.create(carrier="nova_poshta", ttn="20450000000001")
        self.np.return_value = ([self.snapshot(7)], {"success": True})
        with self.sessions.begin() as db:
            site.set_setting(db, "nova_poshta_api_key", "")
        with self.sessions.begin() as db:
            shipping.process_due(db, self.winter_8)
        self.np.assert_called_once()
        self.assertEqual(len(self.codes("arrived")), 1)

    def test_np_ttn_can_be_added_after_creation_but_must_be_valid(self):
        self.assertEqual(shipping.validate({"carrier": "nova_poshta"})["ttn"], "")
        with self.assertRaises(shipping.DeliveryError):
            shipping.validate({"carrier": "nova_poshta", "ttn": "123"})
        self.assertEqual(shipping.validate({"carrier": "nova_poshta", "ttn": "2045 0000000001"})["ttn"], "20450000000001")

    def test_create_no_longer_requires_a_physical_storage_location(self):
        self.login()
        response = self.client.post("/admin/transit/create", data={"part_number": "NEW-1", "title": "Нова деталь",
                   "qty": "2", "price_usd": "10", "carrier": "other", "carrier_name": "Водій", "expected_date": "2026-11-17"})
        self.assertEqual(response.status_code, 302)
        with self.sessions() as db:
            row = db.query(site.TransitOrder).one()
            self.assertEqual(row.storage_location, "")
            self.assertEqual(db.get(shipping.MODELS.Shipment, row.batch_id).carrier_name, "Водій")
            self.assertEqual(db.query(shipping.MODELS.Notice).filter_by(kind="created").count(), 1)
        self.telegram.assert_not_called()  # sending happens after the transaction, in the worker

    def test_invalid_carrier_does_not_create_partial_templates_or_orders(self):
        self.login()
        self.client.post("/admin/transit/create", data={"part_number": "NEW-1", "title": "Нове", "qty": "1"})
        with self.sessions() as db:
            self.assertEqual(db.query(site.TransitOrder).count(), 0)
            self.assertEqual(db.query(site.PartTemplate).count(), 0)

    def test_draft_preserves_shared_delivery_fields_and_all_positions(self):
        self.login()
        for index in (1, 2):
            fields = {"part_number": f"DRAFT-{index}", "title": "Деталь", "qty": "1"}
            if index == 1:
                fields.update(carrier="other", carrier_name="Водій", expected_date="2026-11-18")
            self.client.post("/admin/transit/draft-item", data=fields)
        self.client.post("/admin/transit/create", data={})
        with self.sessions() as db:
            self.assertEqual(db.query(site.TransitOrder).count(), 2)
            shipment = db.query(shipping.MODELS.Shipment).one()
            self.assertEqual((shipment.carrier_name, shipment.expected_date), ("Водій", date(2026, 11, 18)))
            self.assertEqual(db.query(shipping.MODELS.Notice).filter_by(kind="created").count(), 1)

    def test_created_message_has_carrier_date_and_every_ordered_position(self):
        self.create(quantities=tuple([1] * 12))
        message = self.render("created")
        self.assertIn("Водій Олексій", message)
        self.assertIn("17.11.2026", message)
        for index in range(1, 13):
            self.assertIn(f"OEM-{index} —", message)

    def test_no_reminder_before_8_kyiv_and_once_per_day_afterwards(self):
        self.create()
        with self.sessions.begin() as db:
            shipping.process_due(db, self.winter_8 - timedelta(seconds=1))
        self.assertEqual(self.codes("reminder"), [])
        for now in [self.winter_8, self.winter_8 + timedelta(hours=4)]:
            with self.sessions.begin() as db:
                shipping.process_due(db, now)
        self.assertEqual(len(self.codes("reminder")), 1)
        with self.sessions.begin() as db:
            shipping.process_due(db, self.winter_8 + timedelta(days=1))
        self.assertEqual(len(self.codes("reminder")), 2)

    def test_summer_8_kyiv_uses_5_utc_and_server_timezone_does_not_matter(self):
        self.create(expected="2026-07-17")
        at_8 = datetime(2026, 7, 17, 5, 0)
        with self.sessions.begin() as db:
            shipping.process_due(db, at_8 - timedelta(seconds=1))
        self.assertEqual(self.codes("reminder"), [])
        with self.sessions.begin() as db:
            shipping.process_due(db, at_8)
        self.assertEqual(len(self.codes("reminder")), 1)

    def test_partial_receipt_lists_only_remaining_numbers_and_quantities(self):
        self.create()
        with self.sessions.begin() as db:
            rows = shipping.orders_for(db, "batch-1")
            rows[0].accepted_qty, rows[1].accepted_qty = 1, 1
            shipping.process_due(db, self.winter_8)
        message = self.render("reminder")
        self.assertIn("OEM-1 — Запчастина 1 — 1 шт.", message)
        self.assertNotIn("OEM-2", message)

    def test_reminders_stop_after_every_position_has_been_received(self):
        self.create()
        with self.sessions.begin() as db:
            for row in shipping.orders_for(db, "batch-1"):
                row.accepted_qty = row.qty
            shipping.process_due(db, self.winter_8)
        self.assertEqual(self.codes("reminder"), [])

    def test_queued_reminder_is_cancelled_if_receipt_finishes_before_sending(self):
        self.create()
        with self.sessions.begin() as db:
            shipping.process_due(db, self.winter_8)
            db.query(shipping.MODELS.Notice).filter_by(kind="created").one().status = "sent"
            for row in shipping.orders_for(db, "batch-1"):
                row.accepted_qty = row.qty
        with self.sessions() as db:
            shipping.deliver_one(db, self.winter_8)
        self.assertEqual(self.codes("reminder")[0][1], "cancelled")
        self.telegram.assert_not_called()

    def test_arrival_lists_origin_city_romny_label_reminder_and_all_positions_once(self):
        self.create(carrier="nova_poshta", ttn="20450000000001")
        self.apply(self.snapshot(7))
        self.apply(self.snapshot(8))
        self.assertEqual(len(self.codes("arrived")), 1)
        message = self.render("arrived")
        for value in ["Київ", "Ромни", "роздрукувати етикетки", "OEM-1", "OEM-2"]:
            self.assertIn(value, message)

    def test_actual_other_destination_is_not_falsely_reported_as_romny(self):
        self.create(carrier="nova_poshta", ttn="20450000000001")
        self.apply(self.snapshot(7, CityRecipient="Суми"))
        message = self.render("arrived")
        self.assertIn("Суми", message)
        self.assertNotIn("прибув у Ромни", message)

    def test_pickup_preserves_exact_np_time_and_deduplicates_final_status_changes(self):
        self.create(carrier="nova_poshta", ttn="20450000000001")
        self.apply(self.snapshot(9, RecipientDateTime="17.11.2026 11:22:33"))
        self.apply(self.snapshot(10, RecipientDateTime="17.11.2026 11:22:33"))
        self.apply(self.snapshot(11, RecipientDateTime="17.11.2026 11:22:33"))
        self.assertEqual(len(self.codes("picked")), 1)
        self.assertIn("17.11.2026 11:22:33 (Київ)", self.render("picked"))
        self.assertEqual(self.codes("arrived"), [])

    def test_missing_pickup_time_is_not_replaced_with_poll_or_arrival_time(self):
        self.create(carrier="nova_poshta", ttn="20450000000001")
        self.apply(self.snapshot(9, DateScan="17.11.2026 08:40:50", ActualDeliveryDate="17.11.2026 08:40:50"))
        message = self.render("picked")
        self.assertIn("точний час Нова пошта ще не передала", message)
        self.assertNotIn("08:40:50", message)
        self.apply(self.snapshot(9, RecipientDateTime="17.11.2026 11:22:33"))
        self.assertEqual(len(self.codes("picked_time")), 1)
        self.assertIn("11:22:33", self.render("picked_time"))

    def test_date_without_time_is_not_presented_as_exact_midnight_pickup(self):
        self.create(carrier="nova_poshta", ttn="20450000000001")
        self.apply(self.snapshot(9, RecipientDateTime="2026-11-17"))
        self.assertIn("точний час Нова пошта ще не передала", self.render("picked"))

    def test_tracking_fills_estimated_date_but_preserves_manual_estimate(self):
        self.create(carrier="nova_poshta", expected="", ttn="20450000000001")
        self.apply(self.snapshot(5))
        with self.sessions.begin() as db:
            shipment = db.get(shipping.MODELS.Shipment, "batch-1")
            self.assertEqual(shipment.expected_date, date(2026, 11, 17))
            shipment.expected_date, shipment.manual_date = date(2026, 11, 18), True
        self.apply(self.snapshot(6))
        with self.sessions() as db:
            self.assertEqual(db.get(shipping.MODELS.Shipment, "batch-1").expected_date, date(2026, 11, 18))

    def test_tracking_continues_until_pickup_even_if_stock_was_already_received(self):
        self.create(carrier="nova_poshta", ttn="20450000000001")
        with self.sessions.begin() as db:
            for row in shipping.orders_for(db, "batch-1"):
                row.accepted_qty, row.archived_at = row.qty, self.winter_8
        self.np.return_value = ([self.snapshot(9, RecipientDateTime="17.11.2026 07:30:00")], {"success": True})
        with self.sessions.begin() as db:
            shipping.process_due(db, self.winter_8)
        self.assertEqual(len(self.codes("picked")), 1)
        self.assertEqual(self.codes("reminder"), [])

    def test_carrier_requests_are_batched_and_do_not_repeat_inside_five_minutes(self):
        self.create(carrier="nova_poshta", ttn="20450000000001")
        self.np.return_value = ([self.snapshot(5)], {"success": True})
        with self.sessions.begin() as db:
            shipping.process_due(db, self.winter_8)
        with self.sessions.begin() as db:
            shipping.process_due(db, self.winter_8 + timedelta(minutes=1))
        self.assertEqual(self.np.call_count, 1)
        self.assertEqual(self.np.call_args.args[3], {"Documents": [{"DocumentNumber": "20450000000001"}]})

    def test_new_ttn_can_be_saved_on_a_legacy_batch_without_changing_stock(self):
        self.login()
        with self.sessions.begin() as db:
            db.add(site.TransitOrder(batch_id="legacy-1", part_number="OLD", title="Стара деталь", qty=2))
        response = self.client.get("/admin/transit")
        self.assertEqual(response.status_code, 200, response.text[:500])
        with self.client.session_transaction() as state:
            csrf = state["transit_delivery_csrf"]
        self.client.post("/admin/transit/batch/legacy-1/delivery", data={"csrf_token": csrf, "carrier": "nova_poshta", "ttn": "20450000000001"})
        with self.sessions() as db:
            self.assertEqual(db.get(shipping.MODELS.Shipment, "legacy-1").ttn, "20450000000001")
            self.assertEqual(db.query(site.TransitOrder).one().qty, 2)
            self.assertEqual(db.query(site.Part).count(), 0)

    def test_telegram_failure_is_retried_with_no_lost_creation_notice(self):
        self.create()
        self.telegram.side_effect = ValueError("Telegram unavailable")
        with self.sessions() as db:
            shipping.deliver_one(db, self.winter_8)
        self.assertEqual(self.codes("created")[0][1], "pending")
        self.telegram.side_effect = None
        with self.sessions() as db:
            shipping.deliver_one(db, self.winter_8 + timedelta(minutes=2))
        self.assertEqual(self.codes("created")[0][1], "sent")

    def test_long_position_lists_are_split_without_losing_any_oem_number(self):
        self.create(quantities=tuple([1] * 150))
        with self.sessions() as db:
            shipment = db.get(shipping.MODELS.Shipment, "batch-1")
            notice = db.query(shipping.MODELS.Notice).filter_by(kind="created").one()
            chunks = shipping.render_notice(shipment, notice, shipping.orders_for(db, "batch-1"), self.winter_8)
        self.assertGreater(len(chunks), 1)
        self.assertTrue(all(len(value.encode("utf-16-le")) // 2 <= 4096 for value in chunks))
        for index in range(1, 151):
            self.assertIn(f"OEM-{index} —", "\n".join(chunks))

    def test_changing_ttn_cancels_queued_old_tracking_alerts(self):
        self.create(carrier="nova_poshta", ttn="20450000000001")
        self.apply(self.snapshot(7))
        with self.sessions.begin() as db:
            shipping.save_shipment(db, "batch-1", {"carrier": "nova_poshta", "ttn": "20450000000002"})
        self.assertEqual(self.codes("arrived")[0][1], "cancelled")

    def test_unchanged_np_estimate_stays_automatic_when_metadata_is_saved(self):
        self.create(carrier="nova_poshta", expected="", ttn="20450000000001")
        self.apply(self.snapshot(5))
        with self.sessions.begin() as db:
            row = shipping.save_shipment(db, "batch-1", {"carrier": "nova_poshta", "ttn": "20450000000001", "expected_date": "2026-11-17"})
            self.assertFalse(row.manual_date)
        self.apply(self.snapshot(5, ScheduledDeliveryDate="18-11-2026"))
        with self.sessions() as db:
            self.assertEqual(db.get(shipping.MODELS.Shipment, "batch-1").expected_date, date(2026, 11, 18))

    def test_telegram_retry_uses_current_missing_positions_if_nothing_was_sent_yet(self):
        self.create()
        with self.sessions.begin() as db:
            shipping.process_due(db, self.winter_8)
            db.query(shipping.MODELS.Notice).filter_by(kind="created").one().status = "sent"
        self.telegram.side_effect = ValueError("offline")
        with self.sessions() as db:
            shipping.deliver_one(db, self.winter_8)
        with self.sessions.begin() as db:
            rows = shipping.orders_for(db, "batch-1")
            rows[0].accepted_qty = rows[0].qty
        self.telegram.side_effect = None
        with self.sessions() as db:
            shipping.deliver_one(db, self.winter_8 + timedelta(minutes=2))
        sent = self.telegram.call_args.args[2]["text"]
        self.assertNotIn("OEM-1", sent)
        self.assertIn("OEM-2", sent)

    def test_carrier_outage_does_not_block_daily_reminders(self):
        self.create(carrier="nova_poshta", ttn="20450000000001")
        self.np.side_effect = RuntimeError("offline")
        with self.sessions.begin() as db:
            shipping.process_due(db, self.winter_8)
        self.assertEqual(len(self.codes("reminder")), 1)
        with self.sessions() as db:
            self.assertIn("через 5 хвилин", db.get(shipping.MODELS.Shipment, "batch-1").last_error)

    def test_old_reminders_are_not_replayed_after_an_outage(self):
        self.create()
        with self.sessions.begin() as db:
            shipping.process_due(db, self.winter_8)
            db.query(shipping.MODELS.Notice).filter_by(kind="created").one().status = "sent"
        with self.sessions() as db:
            shipping.deliver_one(db, self.winter_8 + timedelta(days=1))
        self.assertEqual(self.codes("reminder")[0][1], "cancelled")
        self.telegram.assert_not_called()

    def test_partial_stock_transfer_keeps_outstanding_quantity_scannable_and_notifies_only_missing(self):
        self.login()
        with self.sessions.begin() as db:
            db.add(site.Part(id=1, warehouse_id=1, part_number="OEM-1", name="Запчастина 1", qty=5,
                             in_stock=True, storage_location="A-1"))
        self.create(quantities=(3,))
        with self.sessions.begin() as db:
            row = db.query(site.TransitOrder).one()
            row.linked_part_id, row.accepted_qty = 1, 1
            received_id = row.id
        self.client.post(f"/admin/transit/{received_id}/arrive", data={"storage_location": "A-1", "print_labels": "0"})
        with self.sessions() as db:
            archived = db.get(site.TransitOrder, received_id)
            pending = db.query(site.TransitOrder).filter(site.TransitOrder.archived_at.is_(None)).one()
            self.assertEqual((archived.qty, archived.accepted_qty), (1, 1))
            self.assertEqual((pending.qty, pending.accepted_qty), (2, 0))
            self.assertEqual(pending.batch_id, archived.batch_id)
            self.assertEqual(db.get(site.Part, 1).qty, 6)
            self.assertEqual(len(site.mobile_transit_orders(db)), 1)
        # A repeated POST cannot add the same accepted unit to stock twice.
        self.client.post(f"/admin/transit/{received_id}/arrive", data={"storage_location": "A-1", "print_labels": "0"})
        with self.sessions.begin() as db:
            self.assertEqual(db.get(site.Part, 1).qty, 6)
            shipping.process_due(db, self.winter_8)
        self.assertIn("OEM-1 — Запчастина 1 — 2 шт.", self.render("reminder"))


if __name__ == "__main__":
    unittest.main()
