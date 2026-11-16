"""Stock commands against a disposable database, fake Telegram and fake Prom."""
import copy
import json
import time
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import app.app as site
from app import marketplaces as mp, telegram_stock as bot
from app.prom_api import PromError


class FakeProm:
    def __init__(self):
        self.rows = {"3244000001": {"id": 3244000001, "sku": "OEM-01", "quantity_in_stock": 3,
                     "presence": "available", "status": "on_display", "price": 415}}
        self.writes = []
        self.offline = False

    def product(self, identifier):
        if self.offline:
            raise PromError("Prom тимчасово недоступний")
        return copy.deepcopy(self.rows[str(identifier)])

    def stock(self, identifier, qty):
        self.writes.append((str(identifier), qty))
        self.rows[str(identifier)].update(quantity_in_stock=qty, presence="available" if qty else "not_available")
        return True

    def close(self):
        pass


class TelegramStockTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite://")
        site.Base.metadata.create_all(self.engine)
        self.sessions = sessionmaker(bind=self.engine, expire_on_commit=False)
        patcher = patch.object(site, "SessionLocal", self.sessions)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(self.engine.dispose)
        self.addCleanup(patch.stopall)
        self.fake = FakeProm()
        patch.object(mp, "PromClient", return_value=self.fake).start()
        patch.object(mp, "token", return_value="test-token-that-is-not-a-real-credential").start()
        self.telegram = patch.object(bot, "api", return_value={"message_id": 400}).start()
        self.config = {"enabled": True, "chat_id": "-123", "token": "fake-bot-token", "started_at": int(time.time()) - 30}
        with self.sessions.begin() as db:
            db.add(site.Warehouse(id=1, name="Основний"))
            db.add(site.Warehouse(id=2, name="Другий"))
            db.flush()
            db.add(site.Part(id=1, warehouse_id=1, part_number="OEM-01", name="Запчастина", qty=3,
                             in_stock=True, price_usd=10))
            db.flush()
            db.add(mp.MODELS.Link(remote_id="3244000001", part_id=1, sku="OEM-01", stock_ready=True,
                                   remote_qty=3, synced_qty=3, presence="available", remote_status="on_display"))
            mp.put(db, "prom_connected", "1")
            mp.put(db, "prom_stock", "1")

    def update(self, value="+OEM-01", update_id=10, **overrides):
        message = {"message_id": update_id + 100, "date": int(time.time()), "chat": {"id": -123, "type": "group"},
                   "from": {"id": 55, "is_bot": False}, "text": value}
        message.update(overrides)
        return {"update_id": update_id, "message": message}

    def handle(self, update):
        with self.sessions.begin() as db:
            return bot.handle_update(db, update, "777", self.config)

    def quantity(self, part_id=1):
        with self.sessions() as db:
            part = db.get(site.Part, part_id)
            return part.qty, part.in_stock

    def test_optional_quantity_and_space_after_sign(self):
        for text, result in [("+OEM-01", ("OEM-01", 1)), ("-OEM-01", ("OEM-01", -1)),
                             (" + OEM-01 2 ", ("OEM-01", 2)), ("-OEM-01 12", ("OEM-01", -12))]:
            self.assertEqual(bot.parse_command(text), result)

    def test_invalid_quantities_are_not_silently_truncated(self):
        for value in ["+", "-", "+OEM-01 0", "+OEM-01 -2", "+OEM-01 1.5", "+OEM-01 2 extra", "+OEM-01 2147483648", "+-OEM-01"]:
            with self.subTest(value=value), self.assertRaises(bot.CommandError):
                bot.parse_command(value)
        self.assertIsNone(bot.parse_command("звичайне повідомлення"))

    def test_increment_and_duplicate_delivery_apply_once(self):
        update = self.update()
        first = self.handle(update)
        self.assertEqual(self.handle(update), first)
        self.assertEqual(self.quantity(), (4, True))
        with self.sessions() as db:
            self.assertEqual(db.query(bot.MODELS.Command).count(), 1)
            self.assertEqual(db.query(site.NewsFeed).count(), 1)

    def test_two_distinct_commands_with_same_text_are_both_applied(self):
        self.handle(self.update(update_id=10))
        self.handle(self.update(update_id=11))
        self.assertEqual(self.quantity(), (5, True))

    def test_decrement_to_zero_removes_availability_and_increment_restores_it(self):
        self.handle(self.update("-OEM-01 3"))
        self.assertEqual(self.quantity(), (0, False))
        self.handle(self.update("+OEM-01", update_id=11))
        self.assertEqual(self.quantity(), (1, True))

    def test_shortage_does_not_change_inventory(self):
        identifier = self.handle(self.update("-OEM-01 4"))
        self.assertEqual(self.quantity(), (3, True))
        with self.sessions() as db:
            command = db.get(bot.MODELS.Command, identifier)
            self.assertEqual(command.status, "error")
            self.assertIn("На складі 3 шт.", command.reply_text)

    def test_unknown_or_substring_oem_does_not_change_another_item(self):
        self.handle(self.update("-OEM"))
        self.assertEqual(self.quantity(), (3, True))

    def test_normalized_oem_is_exact_not_case_sensitive(self):
        self.handle(self.update("+oem01 2"))
        self.assertEqual(self.quantity(), (5, True))

    def test_other_chat_bot_edited_forwarded_and_stale_messages_are_ignored(self):
        for update in [self.update(chat={"id": -999}), self.update(**{"from": {"id": 55, "is_bot": True}}),
                       self.update(forward_origin={"type": "user"}), self.update(sender_chat={"id": -123}),
                       self.update(date=self.config["started_at"] - 1),
                       {"update_id": 12, "edited_message": self.update()["message"]}]:
            self.assertIsNone(self.handle(update))
        self.assertEqual(self.quantity(), (3, True))

    def test_disabled_commands_cannot_change_stock(self):
        self.config["enabled"] = False
        self.assertIsNone(self.handle(self.update()))
        self.assertEqual(self.quantity(), (3, True))

    def test_duplicate_oem_requires_request_sender_to_choose_warehouse_once(self):
        with self.sessions.begin() as db:
            db.add(site.Part(id=2, warehouse_id=2, part_number="OEM-01", name="Інша позиція", qty=8, in_stock=True))
        identifier = self.handle(self.update("-OEM-01 2"))
        self.assertEqual(self.quantity(), (3, True))
        with self.sessions() as db:
            command = db.get(bot.MODELS.Command, identifier)
            self.assertEqual(command.status, "select")
            data = json.loads(command.reply_markup)["inline_keyboard"][1][0]["callback_data"]
        callback = {"update_id": 11, "callback_query": {"id": "callback", "data": data,
                     "from": {"id": 99}, "message": {"chat": {"id": -123}}}}
        self.assertIsNone(self.handle(callback))
        self.assertEqual(self.quantity(2), (8, True))
        callback["callback_query"]["from"]["id"] = 55
        self.assertEqual(self.handle(callback), identifier)
        self.assertIsNone(self.handle(callback))
        self.assertEqual(self.quantity(), (3, True))
        self.assertEqual(self.quantity(2), (6, True))

    def test_final_reply_includes_committed_quantity_and_verified_prom_result(self):
        bot.consume_batch([self.update("+OEM-01 2")], "777", self.config)
        self.assertEqual(self.quantity(), (5, True))
        self.assertEqual(self.fake.writes, [("3244000001", 5)])
        payload = self.telegram.call_args.args[2]
        self.assertIn("Було: 3 шт.", payload["text"])
        self.assertIn("Після команди: 5 шт.", payload["text"])
        self.assertIn("Prom: підтверджено 5 шт.", payload["text"])
        self.assertEqual(self.fake.rows["3244000001"]["price"], 415)
        with self.sessions() as db:
            self.assertEqual(float(db.get(site.Part, 1).price_usd), 10)

    def test_zero_quantity_is_transmitted_to_prom_with_unavailable_presence(self):
        bot.consume_batch([self.update("-OEM-01 3")], "777", self.config)
        self.assertEqual(self.fake.rows["3244000001"]["quantity_in_stock"], 0)
        self.assertEqual(self.fake.rows["3244000001"]["presence"], "not_available")

    def test_repeated_update_sends_no_duplicate_reply_or_remote_write(self):
        update = self.update()
        bot.consume_batch([update, update], "777", self.config)
        self.assertEqual(self.quantity(), (4, True))
        self.assertEqual(self.telegram.call_count, 1)
        self.assertEqual(len(self.fake.writes), 1)

    def test_reply_retry_after_send_failure_never_repeats_stock_movement(self):
        self.telegram.side_effect = bot.CommandError("Telegram тимчасово недоступний")
        bot.consume_batch([self.update()], "777", self.config)
        self.assertEqual(self.quantity(), (4, True))
        self.telegram.side_effect = None
        bot.retry_pending("777", self.config)
        self.assertEqual(self.quantity(), (4, True))
        self.assertEqual(len(self.fake.writes), 1)
        self.assertIn("підтверджено 4 шт.", self.telegram.call_args.args[2]["text"])

    def test_prom_failure_reports_pending_and_retries_without_reapplying_delta(self):
        self.fake.offline = True
        bot.consume_batch([self.update()], "777", self.config)
        self.assertEqual(self.quantity(), (4, True))
        self.assertNotIn("підтверджено", self.telegram.call_args.args[2]["text"])
        self.fake.offline = False
        with self.sessions.begin() as db:
            db.query(bot.MODELS.Command).one().next_attempt = datetime.utcnow() - timedelta(seconds=1)
        bot.retry_pending("777", self.config)
        self.assertEqual(self.quantity(), (4, True))
        self.assertIn("підтверджено 4 шт.", self.telegram.call_args.args[2]["text"])

    def test_ready_mapping_is_checked_against_actual_prom_state(self):
        self.fake.rows["3244000001"]["quantity_in_stock"] = 7
        bot.consume_batch([self.update()], "777", self.config)
        self.assertEqual(self.fake.writes, [])
        self.assertEqual(self.quantity(), (4, True))
        self.assertIn("Залишок змінено на Prom", self.telegram.call_args.args[2]["text"])

    def test_matching_initial_balance_can_enable_an_existing_unique_mapping(self):
        with self.sessions.begin() as db:
            link = db.query(mp.MODELS.Link).one()
            link.stock_ready, link.synced_qty = False, None
        bot.consume_batch([self.update()], "777", self.config)
        self.assertEqual(self.fake.writes, [("3244000001", 4)])
        self.assertIn("підтверджено 4 шт.", self.telegram.call_args.args[2]["text"])

    def test_unmatched_initial_balance_is_not_overwritten(self):
        with self.sessions.begin() as db:
            link = db.query(mp.MODELS.Link).one()
            link.stock_ready, link.synced_qty = False, None
        self.fake.rows["3244000001"]["quantity_in_stock"] = 7
        bot.consume_batch([self.update()], "777", self.config)
        self.assertEqual(self.fake.writes, [])
        self.assertIn("початкові залишки", self.telegram.call_args.args[2]["text"])

    def test_offset_is_persisted_before_reply_delivery(self):
        self.telegram.side_effect = bot.CommandError("offline")
        bot.consume_batch([self.update()], "777", self.config)
        with self.sessions() as db:
            self.assertEqual(site.get_api_settings_map(db)["telegram_stock_offset:777"], "11")
            self.assertEqual(db.query(bot.MODELS.Command).one().reply_sent, 0)

    def test_cache_is_invalidated_after_committed_stock_change(self):
        site.public_cache_set(("telegram-stock-test",), "old quantity")
        bot.consume_batch([self.update()], "777", self.config)
        self.assertIsNone(site.public_cache_get(("telegram-stock-test",)))

    def test_startup_uses_durable_offset_and_never_consumes_pre_activation_stock(self):
        with self.sessions.begin() as db:
            for key, value in [("telegram_stock_enabled", "1"), ("telegram_chat_id", "-123"),
                               ("telegram_bot_token", "fake-token"), ("telegram_stock_started_at", str(int(time.time()))),
                               ("telegram_stock_offset:777", "10")]:
                site.set_setting(db, key, value)
        old = self.update("-OEM-01 3", date=int(time.time()) - 60)
        self.telegram.side_effect = [{"id": 777}, {"url": ""}, [old]]
        bot.poll_once()
        self.assertEqual(self.quantity(), (3, True))
        self.assertEqual(self.telegram.call_args_list[2].args[2]["offset"], 10)
        with self.sessions() as db:
            self.assertEqual(site.get_api_settings_map(db)["telegram_stock_offset:777"], "11")

    def test_configuration_change_during_poll_does_not_authorize_the_old_chat(self):
        with self.sessions.begin() as db:
            for key, value in [("telegram_stock_enabled", "1"), ("telegram_chat_id", "-123"),
                               ("telegram_bot_token", "fake-token")]:
                site.set_setting(db, key, value)
        def request(_token, method, *_args, **_kwargs):
            if method == "getMe":
                return {"id": 777}
            if method == "getWebhookInfo":
                return {"url": ""}
            if method == "getUpdates":
                with self.sessions.begin() as db:
                    site.set_setting(db, "telegram_stock_enabled", "0")
                return [self.update()]
            self.fail("A revoked chat must not receive a response")
        self.telegram.side_effect = request
        bot.poll_once()
        self.assertEqual(self.quantity(), (3, True))

    def test_api_page_disables_competing_getupdates_when_stock_worker_is_enabled(self):
        with self.sessions.begin() as db:
            site.set_setting(db, "telegram_stock_enabled", "1")
            site.set_setting(db, "telegram_stock_recent_chats", json.dumps([{"id": "-123", "title": "QA", "type": "group"}]))
        with self.sessions() as db, patch.object(site, "telegram_api_request") as legacy:
            self.assertEqual(site.telegram_recent_chats(db)[0]["title"], "QA")
            legacy.assert_not_called()

    def test_enabling_commands_requires_admin_and_csrf(self):
        client = site.app.test_client()
        with patch.object(site, "telegram_connection_status", return_value={"configured": False}), \
                patch.object(site, "telegram_recent_chats", return_value=[]):
            self.assertEqual(client.post("/admin/api", data={"api_action": "telegram_stock_settings"}).status_code, 302)
            with client.session_transaction() as session:
                session["admin_auth"] = True
            self.assertEqual(client.post("/admin/api", data={"api_action": "telegram_stock_settings"}).status_code, 403)
            self.assertEqual(client.get("/admin/api").status_code, 200)
            with client.session_transaction() as session:
                csrf = session["telegram_stock_csrf"]
            with self.sessions.begin() as db:
                site.set_setting(db, "telegram_bot_token", "fake-token")
                site.set_setting(db, "telegram_chat_id", "-123")
            response = client.post("/admin/api", data={"api_action": "telegram_stock_settings",
                                   "csrf_token": csrf, "telegram_stock_enabled": "1"})
            self.assertEqual(response.status_code, 302)
            with self.sessions() as db:
                values = site.get_api_settings_map(db)
                self.assertEqual(values["telegram_stock_enabled"], "1")
                self.assertGreater(int(values["telegram_stock_started_at"]), 0)

    def test_webhook_is_never_removed_implicitly(self):
        with self.sessions.begin() as db:
            for key, value in [("telegram_stock_enabled", "1"), ("telegram_chat_id", "-123"),
                               ("telegram_bot_token", "fake-token")]:
                site.set_setting(db, key, value)
        self.telegram.side_effect = [{"id": 777}, {"url": "https://existing.example/webhook"}]
        with self.assertRaises(bot.CommandError):
            bot.poll_once()
        self.assertEqual([call.args[1] for call in self.telegram.call_args_list], ["getMe", "getWebhookInfo"])


if __name__ == "__main__":
    unittest.main()
