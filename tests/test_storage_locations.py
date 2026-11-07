"""Manual inventory and import positions; all fixtures are disposable."""
import io
import json
import unittest
from unittest.mock import patch

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
                "qty": "1", "price_usd": "10", "storage_location": location})
            self.assertEqual(response.status_code, 302)
        self.client.post("/admin/transit/create", data={})
        with self.sessions() as db:
            rows = db.query(site.TransitOrder).order_by(site.TransitOrder.part_number).all()
            self.assertEqual([row.storage_location for row in rows], ["T-101", "T-102"])

    def test_single_transit_records_position_on_order_and_master(self):
        self.login()
        self.client.post("/admin/transit/create", data={"part_number": "SOLO-1", "title": "Один", "qty": "1",
            "price_usd": "10", "storage_location": "T-100"})
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


if __name__ == "__main__":
    unittest.main()
