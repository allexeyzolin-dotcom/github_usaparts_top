"""Manual inventory and import positions; all fixtures are disposable."""
import io
import json
import unittest
from unittest.mock import patch
from datetime import datetime, timedelta
from decimal import Decimal

from sqlalchemy import create_engine, inspect

import app.app as site
from app.storage_locations import migrate_storage_locations
import test_inventory_revision as revision_tests


class StorageLocationTests(unittest.TestCase):
    setUp = revision_tests.InventoryRevisionTests.setUp
    tearDown = revision_tests.InventoryRevisionTests.tearDown
    login = revision_tests.InventoryRevisionTests.login
    save = revision_tests.InventoryRevisionTests.save
    snapshot_main = revision_tests.InventoryRevisionTests.snapshot_main

    def test_storage_location_is_private_to_cabinet(self):
        location = "INTERNAL-SHELF-476"
        with self.sessions.begin() as db:
            part = db.get(site.Part, 1)
            part.storage_location = location
            detail_url = f"/part/{part.id}/{site.part_seo_slug(part)}"

        site.public_cache_clear()
        self.addCleanup(site.public_cache_clear)
        for path in ("/", "/catalog", detail_url):
            with self.subTest(path=path):
                response = self.client.get(path, headers={"User-Agent": "Googlebot (regression test)"})
                self.assertEqual(response.status_code, 200)
                self.assertIn("06G-127-028H", response.text)
                self.assertNotIn("Місце на складі", response.text)
                self.assertNotIn(location, response.text)

        self.login()
        response = self.client.get("/admin/parts")
        self.assertEqual(response.status_code, 200)
        self.assertIn(location, response.text)

    def test_revision_search_returns_one_master_card_not_warehouse_copies(self):
        self.login()
        with self.sessions.begin() as db:
            db.add(site.Warehouse(id=2, name="Другий склад"))
            db.add(site.Part(id=3, warehouse_id=2, part_number="06G-127-028H", name="Інший опис складу", qty=7))
        before = self.snapshot_main()
        response = self.client.get("/admin/revision/search?q=06G").json
        self.assertEqual(len(response["catalog"]), 1)
        self.assertEqual(response["catalog"][0]["source"], "template")
        self.assertEqual(response["catalog"][0]["location"], "A-12")
        self.assertEqual(before, self.snapshot_main())

    def test_legacy_products_without_master_cards_are_still_deduplicated(self):
        self.login()
        with self.sessions.begin() as db:
            db.add_all([site.Part(id=3, warehouse_id=1, part_number="LEGACY-1", name="Один", qty=7),
                        site.Part(id=4, warehouse_id=1, part_number="legacy-1", name="Дублікат", qty=9)])
        data = self.client.get("/admin/revision/search?q=legacy").json
        self.assertEqual(len(data["catalog"]), 1)
        self.assertEqual(data["catalog"][0]["warehouse_name"], "")

    def test_missing_location_blocks_site_mobile_autosave_and_old_revision(self):
        self.login()
        before = self.snapshot_main()
        for path, payload in [
            ("/admin/part/create", {"warehouse_id": "all", "part_number": "NEW", "name": "Нове", "qty": 2}),
            ("/admin/part/1/update", {"part_number": "06G-127-028H", "name": "Змінено", "qty": 1}),
            ("/admin/api/parts/autosave", {"part_number": "06G-127-028H", "name": "Змінено", "qty": 1}),
            ("/api/mobile/receiving/items", {"partNumber": "NEW", "title": "Нове", "qty": 2}),
            ("/api/mobile/revision/1/submit", {"partId": 1, "qty": 1, "isAvailable": "true"}),
            ("/admin/revision/1", {"qty": 1, "availability": "yes"}),
        ]:
            with self.subTest(path=path):
                response = self.client.post(path, data=payload)
                self.assertIn(response.status_code, (302, 400))
        self.assertEqual(before, self.snapshot_main())
        with self.sessions() as db:
            self.assertEqual(db.query(site.ReceivingDraftItem).count(), 0)

    def test_autosave_persists_master_location_without_moving_warehouse_rows(self):
        self.login()
        with self.sessions.begin() as db:
            db.get(site.Part, 1).storage_location = "Старий стелаж"
        response = self.client.post("/admin/api/parts/autosave", data={"part_number": "06G-127-028H", "warehouse_id": "all",
            "name": "Паливний насос", "qty": "0", "price_usd": "20", "storage_location": "Новий стелаж / 7"})
        self.assertEqual(response.status_code, 200, response.text)
        with self.sessions() as db:
            self.assertEqual(db.get(site.PartTemplate, 2).storage_location, "Новий стелаж / 7")
            self.assertEqual(db.get(site.Part, 1).storage_location, "Старий стелаж")
            self.assertEqual(db.get(site.Part, 1).qty, 40)

    def test_mobile_receiving_stores_normalized_location_and_suggests_after_three(self):
        response = self.client.post("/api/mobile/receiving/items", data={"partNumber": "MOBILE-001", "title": "Нове", "qty": "2",
            "storageLocation": "  Стелаж   B / 12  "})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["items"][0]["storageLocation"], "Стелаж B / 12")
        self.assertEqual(self.client.get("/api/mobile/storage-locations?q=Ст").json["items"], [])
        self.assertIn("Стелаж B / 12", self.client.get("/api/mobile/storage-locations?q=сте").json["items"])
        self.assertEqual(self.client.get("/admin/api/storage-locations?q=сте").status_code, 302)
        self.login()
        self.assertIn("Стелаж B / 12", self.client.get("/admin/api/storage-locations?q=сте").json["items"])
        self.assertEqual(self.client.get("/admin/api/storage-locations?q=%25%25%25").json["items"], [])

    def test_revision_increment_requires_and_updates_location(self):
        self.login()
        first = self.save().json["card"]
        bad = self.save(action="increment", target_id=first["id"], location=" ")
        self.assertEqual(bad.status_code, 400)
        changed = self.save(action="increment", target_id=first["id"], location="C-15").json["card"]
        self.assertEqual(changed["qty"], 4)
        self.assertEqual(changed["location"], "C-15")
        self.assertIn("C-15", self.client.get("/admin/api/storage-locations?q=C-1").json["items"])

    def test_receiving_import_requires_each_position_and_preserves_per_row_places(self):
        self.login()
        with self.sessions.begin() as db:
            db.add_all([site.ReceivingDraftItem(part_number="ROW-1", title="Один", qty=2, storage_location="R-101"),
                        site.ReceivingDraftItem(part_number="ROW-2", title="Два", qty=3)])
        self.client.post("/admin/receiving/import", data={"draft_scope": "all", "warehouse_id": "1"})
        with self.sessions() as db:
            self.assertEqual(db.query(site.Part).filter(site.Part.part_number.like("ROW-%")).count(), 0)
            self.assertEqual(db.query(site.ReceivingDraftItem).count(), 2)
        self.client.post("/admin/receiving/import", data={"draft_scope": "all", "warehouse_id": "1", "storage_location": "R-102"})
        with self.sessions() as db:
            parts = db.query(site.Part).filter(site.Part.part_number.like("ROW-%")).order_by(site.Part.part_number).all()
            self.assertEqual([(part.storage_location, part.qty) for part in parts], [("R-101", 2), ("R-102", 3)])
            self.assertEqual(db.query(site.ReceivingDraftItem).count(), 0)

    def import_file(self, content, **fields):
        return self.client.post("/admin/import/preview", data={"warehouse_id": "1", "file": (io.BytesIO(content.encode()), "stock.csv"), **fields})

    def test_file_import_can_read_locations_fill_rows_and_reject_missing_before_writes(self):
        self.login()
        response = self.import_file("Код;Назва;Кількість;Ціна;Місце на складі\nFILE-1;Один;2;10;F-101\nFILE-2;Два;3;20;\n")
        self.assertEqual(response.status_code, 302)
        with self.sessions() as db:
            imported = db.query(site.ImportSession).one()
            session_id = imported.id
            changes = db.query(site.ImportChange).order_by(site.ImportChange.id).all()
            ids = [change.id for change in changes]
            self.assertEqual(json.loads(changes[0].payload_json)["storage_location"], "F-101")
        self.client.post(f"/admin/import/confirm/{session_id}", data={"action": "all"})
        with self.sessions() as db:
            self.assertEqual(db.query(site.Part).filter(site.Part.part_number.like("FILE-%")).count(), 0)
        data = {"action": "all", f"after_qty_{ids[0]}": "2", f"after_qty_{ids[1]}": "3",
                f"storage_location_{ids[0]}": "F-101", f"storage_location_{ids[1]}": "F-102"}
        self.client.post(f"/admin/import/confirm/{session_id}", data=data)
        with self.sessions() as db:
            parts = db.query(site.Part).filter(site.Part.part_number.like("FILE-%")).order_by(site.Part.part_number).all()
            self.assertEqual([(part.storage_location, part.qty) for part in parts], [("F-101", 2), ("F-102", 3)])

    def test_import_common_location_only_fills_rows_without_a_location(self):
        self.login()
        self.import_file("Код;Назва;Кількість;Ціна;Місце на складі\nONE;Один;2;10;F-101\nTWO;Два;3;20;\n", storage_location="F-102")
        with self.sessions() as db:
            rows = db.query(site.ImportChange).order_by(site.ImportChange.id).all()
            self.assertEqual([json.loads(row.payload_json)["storage_location"] for row in rows], ["F-101", "F-102"])

    def test_transit_batch_keeps_each_entered_position(self):
        self.login()
        for number, location in (("TRANSIT-1", "T-101"), ("TRANSIT-2", "T-102")):
            response = self.client.post("/admin/transit/draft-item", data={"part_number": number, "title": number,
                "qty": "1", "price_usd": "10", "storage_location": location,
                "carrier": "other", "carrier_name": "Тестовий перевізник", "expected_date": "2026-11-20"})
            self.assertEqual(response.status_code, 302)
        self.client.post("/admin/transit/create", data={})
        with self.sessions() as db:
            rows = db.query(site.TransitOrder).order_by(site.TransitOrder.part_number).all()
            self.assertEqual([row.storage_location for row in rows], ["T-101", "T-102"])

    def test_single_transit_records_position_on_order_and_master(self):
        self.login()
        self.client.post("/admin/transit/create", data={"part_number": "SOLO-1", "title": "Один", "qty": "1",
            "price_usd": "10", "storage_location": "T-100", "carrier": "other",
            "carrier_name": "Тестовий перевізник", "expected_date": "2026-11-20"})
        with self.sessions() as db:
            self.assertEqual(db.query(site.TransitOrder).one().storage_location, "T-100")
            self.assertEqual(db.query(site.PartTemplate).filter_by(part_number="SOLO-1").one().storage_location, "T-100")

    def test_transit_scan_normalizes_barcode_before_requiring_position(self):
        with self.sessions.begin() as db:
            db.add(site.TransitOrder(part_number="SCAN-1", title="Один", qty=2, barcode="87654321"))
        response = self.client.post("/api/mobile/transit/scan", json={"barcode": " 87654321 "})
        self.assertEqual(response.status_code, 400)
        response = self.client.post("/api/mobile/transit/scan", json={"barcode": " 87654321 ", "storageLocation": "S-100"})
        self.assertEqual(response.status_code, 200, response.text)
        with self.sessions() as db:
            order = db.query(site.TransitOrder).one()
            self.assertEqual((order.accepted_qty, order.storage_location), (1, "S-100"))

    def test_warehouse_update_moves_only_the_selected_warehouse_card(self):
        self.login()
        with self.sessions.begin() as db:
            db.add(site.Warehouse(id=2, name="Другий"))
            db.add(site.Part(id=3, warehouse_id=2, part_number="06G-127-028H", name="Інший", qty=7, storage_location="Інша полиця"))
        self.client.post("/admin/part/1/update", data={"part_number": "06G-127-028H", "name": "Паливний насос", "qty": "40",
            "price_usd": "20", "storage_location": "Нова полиця"})
        with self.sessions() as db:
            self.assertEqual(db.get(site.Part, 1).storage_location, "Нова полиця")
            self.assertEqual(db.get(site.Part, 3).storage_location, "Інша полиця")
            self.assertEqual(db.get(site.Part, 3).qty, 7)

    def test_new_all_goods_card_records_position_without_a_warehouse_copy(self):
        self.login()
        self.client.post("/admin/part/create", data={"warehouse_id": "all", "part_number": "NEW-ALL", "name": "Нове", "qty": "1",
            "storage_location": "R-103"})
        with self.sessions() as db:
            self.assertEqual(db.query(site.PartTemplate).filter_by(part_number="NEW-ALL").one().storage_location, "R-103")
            self.assertEqual(db.query(site.Part).filter_by(part_number="NEW-ALL").count(), 0)

    def test_arrival_can_fill_a_missing_legacy_position_before_stock_changes(self):
        self.login()
        with self.sessions.begin() as db:
            db.add(site.TransitOrder(id=1, part_number="06G-127-028H", title="Насос", qty=1, accepted_qty=1, linked_part_id=1))
        self.client.post("/admin/transit/1/arrive", data={"storage_location": "C-100", "print_labels": "0"})
        with self.sessions() as db:
            part = db.get(site.Part, 1)
            self.assertEqual((part.qty, part.storage_location), (41, "C-100"))

    def test_import_can_update_only_position_when_other_inventory_values_match(self):
        self.login()
        self.import_file("Код;Назва;Кількість;Ціна\n06G-127-028H;Паливний насос;40;20\n")
        with self.sessions() as db:
            row = db.query(site.ImportChange).one()
            identifier, session_id = row.id, row.import_session_id
        self.client.post(f"/admin/import/confirm/{session_id}", data={"action": "all", f"after_qty_{identifier}": "40",
            f"storage_location_{identifier}": "A-99"})
        with self.sessions() as db:
            part = db.get(site.Part, 1)
            self.assertEqual((part.qty, part.storage_location), (40, "A-99"))

    def test_position_migration_is_additive_and_idempotent(self):
        engine = create_engine("sqlite://")
        with engine.begin() as connection:
            for table in ("parts", "part_templates", "receiving_draft_items", "transit_orders"):
                connection.exec_driver_sql(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, qty INTEGER)")
                connection.exec_driver_sql(f"INSERT INTO {table} (id, qty) VALUES (1, 7)")
        migrate_storage_locations(engine)
        migrate_storage_locations(engine)
        with engine.connect() as connection:
            for table in ("parts", "part_templates", "receiving_draft_items", "transit_orders"):
                self.assertEqual(connection.exec_driver_sql(f"SELECT qty, storage_location FROM {table}").one(), (7, ""))
        engine.dispose()

    def test_revision_moves_existing_card_and_preserves_order_issue_links(self):
        from app import courier
        self.login()
        with self.sessions.begin() as db:
            db.add(site.Warehouse(id=2, name="Другий"))
            db.flush()
            db.add_all([
                site.Part(id=3, warehouse_id=2, part_number="０６ｇ / １２７.０２８ｈ", name="Дублікат", qty=7, price_usd=33, storage_location="Стара полиця"),
                site.Part(id=4, warehouse_id=2, part_number="X06G127028H", name="Інший номер", qty=5, storage_location="НЕ РУХАТИ"),
            ])
            db.flush()
            order = site.Order(id=901, customer_name="Тест", status="awaiting_shipment", delivery_type="nova_poshta", stock_reserved=True)
            order.items.append(site.OrderItem(part_id=3, part_number="MARKETPLACE-SKU", name="Насос", qty=2, price_usd=33))
            db.add_all([order, site.PackingRequest(id=901, source_order_id=901), site.AvailabilityRequest(id=901, warehouse_id=2, title="Тест")])
            db.flush()
            db.add_all([
                site.PackingRequestItem(id=901, request_id=901, part_id=3, part_number="MARKETPLACE-SKU", title="Насос", expected_qty=2),
                site.AvailabilityRequestItem(id=901, request_id=901, part_id=3, part_number="06G127028H", title="Насос", expected_qty=2),
                courier.MODELS.Dispatch(order_id=901, payment=Decimal("0"), created_at=datetime.utcnow()),
            ])
        result = self.save(part_number="06G-127-028H", location="Стелаж 7 / Полиця 2", qty="2")
        self.assertEqual(result.status_code, 200, result.text)
        with self.sessions() as db:
            link = db.get(site.RevisionPublication, result.json["card"]["id"])
            self.assertEqual(link.part_id, 1)
            canonical, old = db.get(site.Part, 1), db.get(site.Part, 3)
            self.assertEqual((canonical.qty, canonical.warehouse.name, canonical.storage_location), (2, "Ревізія", "Стелаж 7 / Полиця 2"))
            self.assertEqual((old.qty, old.is_deleted, old.in_stock, old.storage_location), (0, True, False, ""))
            self.assertEqual(old.price_usd, Decimal("33"))
            self.assertEqual(db.query(site.Part).filter_by(is_deleted=False).count(), 2)
            self.assertEqual(db.get(site.Part, 4).storage_location, "НЕ РУХАТИ")
            self.assertEqual(db.get(site.PartTemplate, 2).storage_location, "A-12")
            order = db.get(site.Order, 901)
            self.assertEqual((order.items[0].part_id, order.items[0].qty, order.items[0].price_usd), (1, 2, Decimal("33")))
            for model in (site.PackingRequestItem, site.AvailabilityRequestItem):
                self.assertEqual(db.get(model, 901).part_id, 1)
            self.assertEqual(site.serialize_admin_order(db, order)["items"][0]["storageLocation"], "Стелаж 7 / Полиця 2")
            self.assertEqual(site.serialize_packing_item(db, db.get(site.PackingRequestItem, 901))["storageLocation"], "Стелаж 7 / Полиця 2")
            self.assertEqual(site.serialize_availability_item(db, db.get(site.AvailabilityRequestItem, 901))["storageLocation"], "Стелаж 7 / Полиця 2")
            self.assertEqual(courier.card(db, order, db.get(courier.MODELS.Dispatch, 901))["items"][0]["location"], "Стелаж 7 / Полиця 2")

    def test_revision_locations_are_resolved_for_orders_without_writing_other_stock(self):
        self.login()
        first = self.save(part_number="06G-127-028H", location="РЕВІЗІЯ-21").json["card"]
        with self.sessions.begin() as db:
            db.add(site.Warehouse(id=20, name="Другий"))
            db.flush()
            db.add_all([
                site.Part(id=30, warehouse_id=20, part_number="06g127028h", qty=9, storage_location="Інше"),
                site.ReceivingDraftItem(id=1, part_number="06G_127_028H", title="Насос", qty=2),
                site.TransitOrder(id=1, part_number="06G127028H", title="Насос", qty=3),
            ])
        with self.sessions() as db:
            self.assertEqual(db.get(site.Part, 30).storage_location, "Інше")
            self.assertEqual(db.get(site.ReceivingDraftItem, 1).storage_location, "")
            self.assertEqual(db.get(site.TransitOrder, 1).storage_location, "")
            self.assertEqual(site.location_payload(None, db=db, part_number="06g 127 028h")["storageLocation"], "РЕВІЗІЯ-21")
        # Publishing an edit also absorbs a copy imported since the original count.
        response = self.save(first, action="update", target_id=first["id"], qty="3", location="ПОЛИЦЯ-2")
        self.assertEqual(response.status_code, 200, response.text)
        with self.sessions() as db:
            self.assertEqual((db.get(site.Part, 1).qty, db.get(site.Part, 30).qty), (3, 0))
            self.assertTrue(db.get(site.Part, 30).is_deleted)
            self.assertEqual(db.get(site.PartTemplate, 2).storage_location, "A-12")

    def test_explicit_separate_revision_cards_keep_their_own_counts_and_locations(self):
        self.login()
        first = self.save(part_number="06G-127-028H", location="ПОЛИЦЯ-1").json["card"]
        second = self.save(part_number="06g127028h", location="ПОЛИЦЯ-2", duplicate_action="new_card", qty="3").json["card"]
        with self.sessions() as db:
            self.assertEqual((db.get(site.RevisionPart, first["id"]).qty, db.get(site.RevisionPart, second["id"]).qty), (2, 3))
            self.assertEqual(db.get(site.RevisionPart, first["id"]).location, "ПОЛИЦЯ-1")
            self.assertEqual(db.query(site.Part).filter_by(is_deleted=False).count(), 2)
            self.assertEqual(db.get(site.Part, 1).storage_location, "ПОЛИЦЯ-1")

    def test_startup_cleans_other_locations_and_absorbs_legacy_duplicates_once_with_backup(self):
        from pathlib import Path
        self.login()
        counted = self.save(part_number="06G-127-028H", location="РЕВІЗІЯ-21").json["card"]
        with self.sessions.begin() as db:
            db.add_all([
                site.Part(id=50, warehouse_id=1, part_number="06g127028h", qty=99, in_stock=True, storage_location="ДУБЛІКАТ"),
                site.Part(id=51, warehouse_id=1, part_number="НЕ-ЗБІГ", qty=7, in_stock=True, storage_location="СТАРЕ"),
                site.ReceivingDraftItem(id=1, part_number="НЕ-ЗБІГ", title="Деталь", qty=4, storage_location="ПРИЙМАННЯ"),
                site.TransitOrder(id=1, part_number="НЕ-ЗБІГ", title="Деталь", qty=8, storage_location="В ДОРОЗІ"),
            ])
        with patch.object(site, "BACKUP_DIR", Path(self.temp.name)):
            with self.sessions.begin() as db:
                site.publish_existing_revision_parts(db)
            files = list(Path(self.temp.name).glob("revision_stock_before_move_*.json"))
            self.assertEqual(len(files), 1)
            backup = json.loads(files[0].read_text(encoding="utf-8"))
            self.assertEqual(next(row for row in backup["tables"]["parts"] if row["id"] == 50)["qty"], 99)
            with self.sessions.begin() as db:
                self.assertEqual((db.get(site.Part, 1).qty, db.get(site.Part, 1).storage_location), (2, "РЕВІЗІЯ-21"))
                self.assertEqual(db.get(site.RevisionPart, counted["id"]).location, "РЕВІЗІЯ-21")
                self.assertTrue(db.get(site.Part, 50).is_deleted)
                self.assertEqual((db.get(site.Part, 51).qty, db.get(site.Part, 51).storage_location), (7, ""))
                for model in (site.PartTemplate, site.ReceivingDraftItem, site.TransitOrder):
                    self.assertTrue(all(row.storage_location == "" for row in db.query(model)))
                db.get(site.Part, 51).storage_location = "НОВЕ ПІСЛЯ ОЧИЩЕННЯ"
            site.index_existing_storage_locations()
            with self.sessions.begin() as db:
                site.publish_existing_revision_parts(db)
                self.assertEqual(db.get(site.Part, 51).storage_location, "НОВЕ ПІСЛЯ ОЧИЩЕННЯ")
                self.assertEqual(db.get(site.PartTemplate, 2).storage_location, "")
            self.assertEqual(len(list(Path(self.temp.name).glob("revision_stock_before_move_*.json"))), 1)

    def test_move_and_reference_changes_roll_back_together(self):
        self.login()
        with self.sessions.begin() as db:
            db.add(site.RevisionPart(id=99, part_number="06G127028H", part_key="06G127028H", name="Ревізія", qty=2, location="R-1"))
            db.add(site.Part(id=3, warehouse_id=1, part_number="06g127028h", qty=7, in_stock=True))
            db.flush()
            order = site.Order(id=901, customer_name="Тест")
            order.items.append(site.OrderItem(part_id=3, part_number="SKU", name="Насос", qty=1))
            db.add(order)
        with self.sessions() as db:
            site.publish_revision_part(db, db.get(site.RevisionPart, 99))
            db.flush()
            self.assertEqual((db.get(site.Part, 1).qty, db.get(site.Part, 1).warehouse.name), (2, "Ревізія"))
            self.assertEqual(db.get(site.Order, 901).items[0].part_id, 1)
            db.rollback()
        with self.sessions() as db:
            self.assertEqual((db.get(site.Part, 1).qty, db.get(site.Part, 1).warehouse_id), (40, 1))
            self.assertIsNone(db.get(site.RevisionPublication, 99))
            self.assertEqual((db.get(site.Part, 3).qty, db.get(site.Part, 3).is_deleted), (7, False))
            self.assertEqual(db.get(site.Order, 901).items[0].part_id, 3)


if __name__ == "__main__":
    unittest.main()
