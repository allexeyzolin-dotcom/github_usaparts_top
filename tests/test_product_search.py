"""SEO behavior with a disposable database; no production inventory mutations."""
import json
import re
import unittest
from datetime import datetime
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import app.app as site
from app.product_search import generate_product_search, query_list


class ProductSearchTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite://")
        site.Base.metadata.create_all(self.engine)
        self.sessions = sessionmaker(bind=self.engine)
        self.db_patch = patch.object(site, "SessionLocal", self.sessions)
        self.db_patch.start()
        self.track_patch = patch.object(site, "track_daily_guest_visit")
        self.track_patch.start()
        site.public_cache_clear()
        self.client = site.app.test_client()
        with self.sessions.begin() as db:
            db.add_all([site.Warehouse(id=1, name="Склад", markup_percent=0),
                        site.Part(id=1, warehouse_id=1, part_number="06G127028H", name="Насос паливний VAG",
                                  brand="VAG", qty=2, in_stock=True, price_usd=20, description="Службова нотатка SECRET"),
                        site.Part(id=2, warehouse_id=1, part_number="06G127028H", name="Насос паливний VAG",
                                  brand="VAG", qty=1, in_stock=True, price_usd=20),
                        site.Part(id=3, warehouse_id=1, part_number="SOLD-001", name="Крило", qty=0, in_stock=False),
                        site.Part(id=4, warehouse_id=1, part_number="DELETED", name="Видалений", is_deleted=True),
                        site.PartTemplate(id=1, part_number="06G127028H", name="Насос паливний VAG", brand="VAG",
                                          cross_numbers_json='["06G127028J"]'),
                        site.PartTemplate(id=2, part_number="TEMPLATE", name="Нова деталь", brand="BMW")])

    def tearDown(self):
        self.db_patch.stop()
        self.track_patch.stop()
        self.engine.dispose()
        site.public_cache_clear()

    def authenticate(self):
        with self.client.session_transaction() as session:
            session["admin_auth"] = True
            session["product_search_token"] = "test-token"

    def seo(self, action=None, number="06G127028H", **values):
        if action is None:
            return self.client.get("/admin/parts/seo", query_string={"part_number": number})
        return self.client.post("/admin/parts/seo", json={"part_number": number,
                                "action": action, "token": "test-token", **values})

    def card(self, identifier=1):
        with self.sessions() as db, site.app.test_request_context("/"):
            url = site.part_detail_url(db.get(site.Part, identifier))
        return self.client.get(url, headers={"User-Agent": "Googlebot"})

    def schema(self, html):
        return json.loads(re.search(r'<script type="application/ld\+json">(.*?)</script>', html, re.S).group(1))["@graph"]

    def test_every_existing_card_has_server_rendered_search_and_matching_schema(self):
        for identifier in [1, 2, 3]:
            response = self.card(identifier)
            self.assertEqual(response.status_code, 200)
            self.assertIn("Про запчастину", response.text)
            self.assertIn("Назви та артикули для пошуку", response.text)
            self.assertIn('data-copy-part-number=', response.text)
            graph = self.schema(response.text)
            page = next(item for item in graph if item["@type"] == "WebPage")
            self.assertIn(f'<title>{page["name"]}</title>', response.text)
            product = next(item for item in graph if item["@type"] == "Product")
            self.assertNotIn("SECRET", product["description"])
            self.assertIn(product["description"], response.text)
        self.assertNotIn('name="keywords"', response.text)

    def test_out_of_stock_copy_and_offer_are_consistent(self):
        response = self.card(3)
        self.assertIn("Наразі немає в наявності.", response.text)
        product = next(item for item in self.schema(response.text) if item["@type"] == "Product")
        self.assertEqual(product["offers"]["availability"], "https://schema.org/OutOfStock")

    def test_admin_and_csrf_are_required(self):
        self.assertEqual(self.seo().status_code, 302)
        self.authenticate()
        response = self.client.post("/admin/parts/seo", json={"part_number": "06G127028H", "action": "save"})
        self.assertEqual(response.status_code, 403)
        with self.sessions() as db:
            self.assertEqual(db.query(site.PartSearchProfile).count(), 0)

    def test_auto_mode_never_persists_generated_copy(self):
        self.authenticate()
        payload = self.seo().json
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["custom"]["title"], "")
        self.assertTrue(any("06G127028J" in phrase for phrase in payload["generated"]["queries"]))
        self.assertEqual(self.seo("generate").status_code, 200)
        with self.sessions() as db:
            self.assertEqual(db.query(site.PartSearchProfile).count(), 0)

    def test_save_applies_to_both_warehouse_cards_without_inventory_changes(self):
        self.authenticate()
        response = self.seo("save", title="Насос VAG 06G127028H | USAparts.top",
                            description="Паливний насос VAG. Перевірка за артикулом.",
                            queries="насос VAG\nнасос VAG\n06G127028H насос")
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(len(response.json["effective"]["queries"]), 2)
        for identifier in [1, 2]:
            html = self.card(identifier).text
            self.assertIn("Паливний насос VAG. Перевірка за артикулом.", html)
            self.assertIn('<title>Насос VAG 06G127028H | USAparts.top</title>', html)
        with self.sessions() as db:
            self.assertEqual([p.qty for p in db.query(site.Part).filter(site.Part.id.in_([1, 2])).order_by(site.Part.id)], [2, 1])
            self.assertEqual(db.get(site.Part, 1).price_usd, 20)

    def test_reset_and_empty_fields_restore_dynamic_auto_mode(self):
        self.authenticate()
        self.seo("save", title="Власний заголовок", description="", queries="")
        self.assertEqual(self.seo("reset").status_code, 200)
        with self.sessions.begin() as db:
            self.assertEqual(db.query(site.PartSearchProfile).count(), 0)
            db.get(site.Part, 1).name = "Оновлений паливний насос"
        self.assertIn("Оновлений паливний насос", self.card().text)

    def test_cross_page_keeps_its_identity_with_custom_main_title(self):
        self.authenticate()
        self.seo("save", title="Головна картка насоса", description="Власний опис", queries="насос VAG")
        with self.sessions() as db, site.app.test_request_context("/"):
            url = site.cross_part_detail_url(db.get(site.Part, 1), "06G127028J")
        response = self.client.get(url, headers={"User-Agent": "Googlebot"})
        self.assertEqual(response.status_code, 200)
        graph = self.schema(response.text)
        product = next(item for item in graph if item["@type"] == "Product")
        self.assertEqual(product["mpn"], "06G127028J")
        self.assertNotIn("Головна картка насоса", response.text)

    def test_unknown_and_deleted_products_not_editable(self):
        self.authenticate()
        for number in ["UNKNOWN", "DELETED"]:
            self.assertEqual(self.seo(number=number).status_code, 404)
        self.assertEqual(self.seo(number="TEMPLATE").status_code, 200)

    def test_limits_and_text_safety(self):
        self.authenticate()
        self.assertEqual(self.seo("save", title="x" * 101).status_code, 400)
        self.assertEqual(self.seo("save", description="x" * 161).status_code, 400)
        self.assertEqual(self.seo("save", queries=["not text"]).status_code, 400)
        response = self.seo("save", title='<script>alert(1)</script> Насос', queries="<b>насос</b>\nнасос")
        self.assertEqual(response.status_code, 200)
        self.assertNotIn("<script>", response.json["effective"]["title"])
        self.assertEqual(response.json["effective"]["queries"], ["насос"])

    def test_widget_in_all_editors_without_inventory_field_names(self):
        self.authenticate()
        with site.app.test_request_context("/admin/parts"):
            site.session["admin_auth"] = True
            html = site.render_template("_part_seo_editor.html")
            macro = site.app.jinja_env.get_template("_part_seo_editor.html").module.product_seo_editor("OEM")
            self.assertNotIn('name="', str(macro))
        for template in ["admin_parts.html", "warehouse_detail.html"]:
            source, _, _ = site.app.jinja_env.loader.get_source(site.app.jinja_env, template)
            self.assertIn("product_seo_editor(", source)


class MerchantSchemaTests(unittest.TestCase):
    def test_parts_use_google_taxonomy_without_losing_shop_categories(self):
        part = site.Part(id=1, part_number="RADIATOR", name="Радіатор охолодження",
                         brand="BMW", price_usd=50, qty=1, in_stock=True,
                         created_at=datetime(2026, 10, 1), updated_at=datetime(2026, 10, 20))
        with site.app.test_request_context("/"):
            graph = json.loads(site.build_part_product_schema(part, None))["@graph"]
        product = next(item for item in graph if item["@type"] == "Product")
        self.assertEqual(product["category"], {
            "@type": "CategoryCode",
            "inCodeSet": "https://www.google.com/basepages/producttype/taxonomy-with-ids.en-US.txt",
            "codeValue": "899",
        })
        self.assertTrue(any(prop["name"] == "Категорія" for prop in product["additionalProperty"]))
        self.assertEqual(product["offers"]["validFrom"], "2026-10-20")
        self.assertLessEqual(product["offers"]["validFrom"], product["offers"]["priceValidUntil"])
        self.assertEqual(product["offers"]["priceCurrency"], "UAH")
        self.assertNotIn("image", product)

    def test_car_offer_uses_car_taxonomy_and_actual_publication_date(self):
        car = site.Car(id=1, brand="Ford", model="Escape", year=2018,
                       price_usd=10000, status="in_stock", created_at=datetime(2026, 10, 2))
        with site.app.test_request_context("/"):
            graph = json.loads(site.build_car_product_schema(car, []))["@graph"]
        product = next(item for item in graph if item["@type"] == "Product")
        self.assertEqual(product["category"]["codeValue"], "916")
        self.assertEqual(product["offers"]["validFrom"], "2026-10-02")

    def test_missing_dates_do_not_fabricate_an_offer_start_date(self):
        part = site.Part(id=1, part_number="UNDATED", name="Крило", price_usd=20,
                         qty=1, in_stock=True)
        with site.app.test_request_context("/"):
            graph = json.loads(site.build_part_product_schema(part, None))["@graph"]
        product = next(item for item in graph if item["@type"] == "Product")
        self.assertNotIn("validFrom", product["offers"])


class SearchGenerationTests(unittest.TestCase):
    def test_long_names_keep_the_oem_in_title_and_search(self):
        data = generate_product_search(name="Дуже довга назва автозапчастини " * 12, number="06G127028H")
        self.assertIn("06G127028H", data["title"])
        self.assertIn("06G127028H", data["queries"][0])
        self.assertLessEqual(len(data["title"]), 100)
        self.assertLessEqual(len(data["description"]), 160)

    def test_ukrainian_variant_preserves_side_model_and_oem(self):
        data = generate_product_search(name="Крыло переднее правое BMW X5", number="BMW123", brand="BMW")
        self.assertIn("крило переднє праве BMW X5 BMW123", data["queries"])

    def test_relevance_no_guessed_vehicle_or_brand_repetition(self):
        data = generate_product_search(name="06G127028H - Насос VAG", number="06G127028H", brand="VAG", available=True)
        self.assertNotIn("VAG VAG", data["title"])
        self.assertNotIn("06G127028H 06G127028H", data["title"])
        self.assertNotIn("BMW", json.dumps(data))
        self.assertLessEqual(len(data["queries"]), 6)

    def test_queries_are_bounded_unique_and_handle_unicode(self):
        queries = query_list("Фара\nфара\n  BMW  X5 \n" + "\n".join(str(i) * 160 for i in range(10)))
        self.assertEqual(len(queries), 6)
        self.assertTrue(all(len(item) <= 120 for item in queries))
        self.assertEqual(queries[1], "BMW X5")


if __name__ == "__main__":
    unittest.main()
