"""Counted stock is sellable, exportable and has one transactional balance."""
import csv
import io
import unittest
from decimal import Decimal
from unittest.mock import patch

import test_inventory_revision as revision_tests

# Column order and names from the user's Avtopro export, independent of the implementation.
EXPECTED_DOWNLOAD_HEADER = ['Виробник', 'Код', 'Ціна', 'Кількість', 'Опис', 'Б/В', 'Реставрація',
                            'Фото', 'Вага, кг', 'Глибина, см', 'Ширина, см', 'Висота, см']

site = revision_tests.site


class RevisionPublicationTests(unittest.TestCase):
    tearDown = revision_tests.InventoryRevisionTests.tearDown
    login = revision_tests.InventoryRevisionTests.login
    save = revision_tests.InventoryRevisionTests.save

    def setUp(self):
        revision_tests.InventoryRevisionTests.setUp(self)
        site.public_cache_clear()

    def create(self, **changes):
        self.login()
        values = {"brand": "VAG", **changes}
        response = self.save(**values)
        self.assertEqual(response.status_code, 200, response.text)
        return response.json["card"]

    def publication(self, card_id):
        with self.sessions() as db:
            link = db.get(site.RevisionPublication, card_id)
            part = db.get(site.Part, link.part_id)
            return {"id": part.id, "warehouse_id": part.warehouse_id, "qty": part.qty,
                    "price": part.price_usd, "in_stock": part.in_stock, "name": part.name}

    def export_rows(self):
        response = self.client.get("/admin/revision/export/avtopro")
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.headers["Cache-Control"], "no-store")
        self.assertTrue(response.data.startswith(b"\xef\xbb\xbf"))
        rows = list(csv.reader(io.StringIO(response.data.decode("utf-8-sig")), delimiter=";"))
        self.assertEqual(rows[0], EXPECTED_DOWNLOAD_HEADER)
        return rows[1:]

    def test_both_exports_keep_two_decimal_prices_and_leading_zero_codes(self):
        for index, (value, expected) in enumerate((("12", "12.00"), ("12,5", "12.50"),
                                                    ("0.01", "0.01"), ("12345678.90", "12345678.90"))):
            self.create(part_number=f"00{index}", price_usd=value)
        rows = self.export_rows()
        self.assertEqual([row[1] for row in rows], ["000", "001", "002", "003"])
        self.assertEqual([row[2] for row in rows], ["12.00", "12.50", "0.01", "12345678.90"])
        self.assertTrue(all(len(row) == 12 for row in rows))
        self.assertTrue(all(Decimal(row[2]).is_finite() for row in rows))
        response = self.client.get("/admin/revision/export")
        full = list(csv.DictReader(io.StringIO(response.data.decode("utf-8-sig")), delimiter=";"))
        self.assertEqual([row["price_usd"] for row in full], [row[2] for row in rows])

    def test_avtopro_escapes_text_uses_absolute_photos_and_exports_only_available_stock(self):
        self.create(part_number="000123", name='Деталь; "ліва"', description="PRIVATE",
                    warehouse_name="PRIVATE WAREHOUSE", location="PRIVATE LOCATION",
                    photo_urls=["/uploads/revision_demo.jpg", "https://example.com/photo.jpg"])
        sold = self.create(part_number="SOLD")
        self.save(sold, action="update", target_id=sold["id"], qty="0")
        rows = self.export_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][:7], ["VAG", "000123", "12.50", "2", 'Деталь; "ліва"', "+", ""])
        self.assertIn(site.public_site_base_url() + "/uploads/revision_demo.jpg", rows[0][7])
        self.assertNotIn("PRIVATE", str(rows))

    def test_invalid_cards_show_specific_issues_before_any_partial_download(self):
        first = self.create(part_number="DUP", price_usd="12.50")
        self.create(part_number="DUP", price_usd="12.50", duplicate_action="new_card", qty="3")
        self.assertEqual(self.export_rows()[0][3], "5")
        self.save(first, action="update", target_id=first["id"], price_usd="13")
        response = self.client.get("/admin/revision/export/avtopro")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Потрібно уточнити дані товарів", response.text)
        self.assertIn("DUP", response.text)
        self.assertNotIn("attachment", response.headers.get("Content-Disposition", ""))
        for changes in ({"brand": ""}, {"price_usd": "0"}):
            with self.subTest(changes=changes):
                # Keep the conflicting duplicate sold so this specifically checks invalid fields.
                with self.sessions.begin() as db:
                    for row in db.query(site.RevisionPart):
                        row.qty = 0
                self.create(part_number=f"BAD-{len(changes.get('brand', 'x'))}", **changes)
                response = self.client.get("/admin/revision/export/avtopro")
                self.assertEqual(response.status_code, 200)
                self.assertNotIn("text/csv", response.content_type)

    def test_revision_cards_without_photos_are_visible_and_can_be_ordered(self):
        counted = self.create()
        part = self.publication(counted["id"])
        for path in ("/", "/catalog", "/catalog?q=REV-001"):
            with self.subTest(path=path):
                response = self.client.get(path)
                self.assertEqual(response.status_code, 200)
                self.assertIn("Перевірена деталь", response.text)
        with site.app.test_request_context():
            detail_url = site.part_detail_url(site.Part(id=part["id"], part_number=counted["part_number"], name=counted["name"]))
        response = self.client.get(detail_url)
        self.assertEqual(response.status_code, 200)
        self.assertIn("Перевірена наявність", response.text)
        self.client.post(f"/cart/add/{part['id']}")
        self.assertIn("Перевірена деталь", self.client.get("/cart").text)
        with patch.object(site, "send_telegram_message"):
            response = self.client.post("/checkout", data={"customer_name": "Test", "phone": "0671234567"})
        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.location.endswith("/checkout/success"))
        with self.sessions.begin() as db:
            order = db.query(site.Order).one()
            self.assertTrue(order.stock_reserved)
            self.assertEqual(order.items[0].part_id, part["id"])
            self.assertEqual(db.get(site.RevisionPart, counted["id"]).qty, 1)
            self.assertEqual(db.get(site.Part, part["id"]).qty, 1)
            self.assertEqual(db.get(site.Part, 1).qty, 40)
            site.release_order_inventory(db, order)
        self.assertEqual(self.publication(counted["id"])["qty"], 2)
        with self.sessions() as db:
            self.assertEqual(db.get(site.RevisionPart, counted["id"]).qty, 2)
        # Stock changes invalidate an already-open revision edit.
        self.assertEqual(self.save(counted, action="update", target_id=counted["id"]).status_code, 409)

    def test_edit_zero_and_increment_update_one_publication_without_duplicate_cards(self):
        counted = self.create()
        original = self.publication(counted["id"])
        result = self.save(counted, action="update", target_id=counted["id"], qty="0", price_usd="42.10")
        self.assertEqual(result.status_code, 200, result.text)
        self.assertFalse(self.publication(counted["id"])["in_stock"])
        self.assertNotIn("Перевірена деталь", self.client.get("/catalog").text)
        self.assertIn("Поки немає товарів для експорту", self.client.get("/admin/revision/export/avtopro").text)
        self.save(result.json["card"], action="increment", target_id=counted["id"], qty="3")
        current = self.publication(counted["id"])
        self.assertEqual(current["id"], original["id"])
        self.assertEqual(current["qty"], 3)
        self.assertEqual(current["price"], Decimal("42.10"))
        with self.sessions() as db:
            self.assertEqual(db.query(site.RevisionPublication).count(), 1)

    def test_checked_card_takes_precedence_over_original_card_with_more_stock(self):
        counted = self.create(part_number="06G-127-028H", name="Перевірений насос", qty="1")
        part = self.publication(counted["id"])
        response = self.client.get("/catalog?q=06G-127-028H")
        self.assertIn("Перевірений насос", response.text)
        self.assertIn(f"/part/{part['id']}/", response.text)
        self.assertNotIn('>Паливний насос</a>', response.text)

    def test_checked_home_card_wins_over_guaranteed_stock_even_without_photo(self):
        counted = self.create(part_number="06G-127-028H", qty="1")
        with self.sessions() as db:
            source = db.get(site.Part, 1)
            source.warehouse.name = "Склад гарантованої наявності"
            link = db.get(site.RevisionPublication, counted["id"])
            verified = db.get(site.Part, link.part_id)
            selected, total = site.build_home_showcase_parts([source, verified])
            self.assertEqual(total, 1)
            self.assertEqual([part.id for part in selected], [verified.id])

    def test_existing_revision_backfill_is_idempotent_and_does_not_adopt_existing_warehouses(self):
        with self.sessions.begin() as db:
            db.add(site.Warehouse(id=2, name="Ревізія", markup_percent=50))
            db.add(site.RevisionPart(part_number="LEGACY", part_key="LEGACY", name="Збережена ревізія",
                                     brand="BMW", qty=3, price_usd=Decimal("42.50")))
        with self.sessions.begin() as db:
            site.publish_existing_revision_parts(db)
        first = self.publication(1)
        with self.sessions.begin() as db:
            site.publish_existing_revision_parts(db)
        self.assertEqual(first, self.publication(1))
        self.assertNotEqual(first["warehouse_id"], 2)
        with self.sessions() as db:
            self.assertEqual(db.query(site.RevisionPublication).count(), 1)
            self.assertEqual(db.get(site.Warehouse, first["warehouse_id"]).markup_percent, 0)

    def test_revision_warehouse_supports_existing_avtopro_autoupdate(self):
        counted = self.create()
        part = self.publication(counted["id"])
        self.client.get("/admin/avtopro-feeds")
        with self.client.session_transaction() as state:
            csrf = state["avtopro_feed_form_token"]
        response = self.client.post("/admin/avtopro-feeds", data={"csrf_token": csrf,
                                    "warehouse_id": part["warehouse_id"], "action": "enable"})
        self.assertEqual(response.status_code, 302)
        with self.sessions() as db:
            token = db.query(site.ApiSetting).filter_by(setting_key=site.avtopro_feed_setting(part["warehouse_id"])).one().setting_value
        response = self.client.get(f"/feeds/avtopro/{part['warehouse_id']}/{token}/prices.csv")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"VAG;REV-001;12.50;2;", response.data)

    def test_original_template_edits_do_not_overwrite_counted_publication(self):
        counted = self.create(part_number="06G-127-028H", price_usd="42.10")
        with self.sessions.begin() as db:
            template, _ = site.upsert_part_template(db, "06G-127-028H", {"name": "Інша назва", "price_usd": 99})
            site.apply_template_to_parts(db, template)
        part = self.publication(counted["id"])
        self.assertEqual(part["name"], "Перевірена деталь")
        self.assertEqual(part["price"], Decimal("42.10"))
        response = self.client.post(f"/admin/part/{part['id']}/update", data={"price_usd": 1, "storage_location": "A-12"})
        self.assertTrue(response.location.endswith("/admin/revision"))

    def test_long_manufacturer_barcode_stays_in_revision_and_publication_uses_ean8(self):
        counted = self.create(barcode="1234567890123")
        part = self.publication(counted["id"])
        with self.sessions() as db:
            self.assertEqual(db.get(site.RevisionPart, counted["id"]).barcode, "1234567890123")
            self.assertRegex(db.get(site.Part, part["id"]).barcode, r"^[0-9]{8}$")

    def test_export_condition_matches_the_existing_storefront_condition(self):
        self.create(part_number="NEW", name="Нова деталь")
        self.create(part_number="USED", name="Вживана деталь")
        override = self.create(part_number="OVERRIDE", name="Деталь Б/В", part_condition="NEW")
        rows = {row[1]: row for row in self.export_rows()}
        self.assertEqual(rows["NEW"][5], "")
        self.assertEqual(rows["USED"][5], "+")
        self.assertEqual(rows["OVERRIDE"][5], "")
        with self.sessions() as db:
            self.assertEqual(site.part_condition_code(db.get(site.RevisionPart, override["id"])), "NEW")
            published = db.get(site.Part, self.publication(override["id"])["id"])
            self.assertEqual(site.part_condition_code(published), "NEW")

    def test_missing_brand_is_resolved_from_one_catalog_manufacturer_without_inventory_writes(self):
        counted = self.create(part_number="06G-127-028H", brand="")
        rows = self.export_rows()
        self.assertEqual(rows[0][0], "VAG")
        with self.sessions() as db:
            self.assertEqual(db.get(site.RevisionPart, counted["id"]).brand, "")
            self.assertEqual(db.get(site.Part, 1).qty, 40)

    def test_ambiguous_brand_is_not_guessed_and_partial_download_is_explicit(self):
        bad = self.create(part_number="06G-127-028H", brand="")
        self.create(part_number="READY", brand="BMW")
        with self.sessions.begin() as db:
            db.get(site.Part, 1).brand_export = "OTHER"
        response = self.client.get("/admin/revision/export/avtopro")
        self.assertIn("Виправити картку", response.text)
        self.assertIn(f"edit={bad['id']}", response.text)
        self.assertNotIn("attachment", response.headers.get("Content-Disposition", ""))
        response = self.client.get("/admin/revision/export/avtopro?ready=1")
        self.assertIn("attachment", response.headers["Content-Disposition"])
        self.assertEqual(response.headers["X-Skipped-Card-Count"], "1")
        rows = list(csv.reader(io.StringIO(response.data.decode("utf-8-sig")), delimiter=";"))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[1][1], "READY")

    def test_photos_use_commas_and_dimension_columns_match_the_sample(self):
        self.create(photo_urls=["https://example.com/a.jpg", "https://example.com/b.jpg"])
        row = self.export_rows()[0]
        self.assertEqual(row[7], "https://example.com/a.jpg,https://example.com/b.jpg")
        self.assertEqual(row[8:], ["", "", "", ""])

    def test_fix_link_opens_an_older_card_outside_the_first_page(self):
        first = self.create(brand="")
        for index in range(51):
            self.create(part_number=f"LATER-{index}")
        response = self.client.get(f"/admin/revision?edit={first['id']}")
        self.assertEqual(response.status_code, 200)
        self.assertIn(f'data-edit-id="{first["id"]}"', response.text)
        self.assertIn('"part_number": "REV-001"', response.text)


if __name__ == "__main__":
    unittest.main()
