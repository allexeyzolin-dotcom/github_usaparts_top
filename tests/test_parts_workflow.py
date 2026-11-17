import json
import unittest
from urllib.parse import parse_qs, urlsplit
from unittest.mock import patch
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import app.app as site
from app import parts_tools


class PartsWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite://')
        site.Base.metadata.create_all(self.engine)
        self.sessions = sessionmaker(bind=self.engine, expire_on_commit=False)
        patch.object(site, 'SessionLocal', self.sessions).start()
        patch.object(site, 'track_daily_guest_visit').start()
        self.addCleanup(patch.stopall)
        self.addCleanup(self.engine.dispose)
        self.client = site.app.test_client()
        with self.client.session_transaction() as state:
            state['admin_auth'] = True
        with self.sessions.begin() as db:
            db.add_all([site.Warehouse(id=1, name='Перший'), site.Warehouse(id=2, name='Другий')])
            db.flush()
            db.add(site.Part(id=1, warehouse_id=2, part_number='5Q0-123', barcode='12345678', name='Деталь', qty=3,
                             in_stock=True, storage_location='A-12', photo_urls='/uploads/export.jpg',
                             showcase_photo_urls='["/uploads/a.jpg", "/uploads/b.jpg"]'))
            db.add_all([site.PartTemplate(part_number='5Q0-123', name='Деталь', storage_location='A-12',
                       cross_numbers_json='["CROSS-123"]', photo_urls='/uploads/export.jpg',
                       showcase_photo_urls='["/uploads/a.jpg", "/uploads/b.jpg"]'),
                       site.PartTemplate(part_number='ONLY-456', name='Без складу', storage_location='B-4')])
        site.public_cache_clear()
        self.addCleanup(site.public_cache_clear)

    def test_search_finds_other_warehouse_by_oem_barcode_compact_or_cross(self):
        for query in ('5Q0-123', '5q0 123', '12345678', 'CROSS-123'):
            response = self.client.get('/admin/parts', query_string={'q': query, 'warehouse_id': 1, 'locate': '1'})
            target = parse_qs(urlsplit(response.location).query)
            self.assertEqual(target['warehouse_id'], ['2'])
            self.assertEqual(target['q'], ['5Q0-123'])

    def test_template_without_stock_opens_editor_and_photo_controls(self):
        response = self.client.get('/admin/parts?q=ONLY-456&locate=1', follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn('data-auto-open="1"', response.text)
        self.assertIn('Зберегти головне фото', response.text)
        self.assertIn('B-4', response.text)

    def test_multiple_warehouses_offer_links_without_picking_one(self):
        with self.sessions.begin() as db:
            db.add(site.Part(warehouse_id=1, part_number='5Q0-123', name='Деталь', qty=1, storage_location='C-1'))
        response = self.client.get('/admin/parts?q=5Q0-123&locate=1', follow_redirects=True)
        self.assertIn('value="all"', response.text)
        self.assertIn('Перший: 1 шт.', response.text)
        self.assertIn('Другий: 3 шт.', response.text)
        self.assertIn('C-1', response.text)

    def test_explicit_warehouse_filter_is_preserved(self):
        response = self.client.get('/admin/parts?warehouse_id=1&q=5Q0-123&locate=0')
        self.assertEqual(response.status_code, 200)
        self.assertIn('У цьому складі поки немає', response.text)

    def test_master_autosave_keeps_master_scope_and_each_warehouse_location(self):
        response = self.client.post('/admin/api/parts/autosave', data={'part_number': '5Q0-123',
                    'warehouse_id': 'all', 'qty': '0', 'storage_location': 'MASTER-1'})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json['card']['primaryWarehouseId'], 'all')
        self.assertEqual(response.json['card']['assignedWarehouses'][0]['storageLocation'], 'A-12')
        with self.sessions() as db:
            self.assertEqual(db.get(site.Part, 1).storage_location, 'A-12')

    def test_no_match_does_not_create_a_card(self):
        response = self.client.get('/admin/parts?q=NOT-FOUND&locate=1', follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        with self.sessions() as db:
            self.assertEqual(db.query(site.PartTemplate).count(), 2)

    def test_exact_oem_wins_over_another_cards_cross_number_or_barcode(self):
        with self.sessions.begin() as db:
            db.add(site.PartTemplate(part_number='CROSS-OWNER', name='Інший товар', cross_numbers_json='["ONLY-456"]'))
            db.add(site.Part(warehouse_id=1, part_number='BARCODE-OWNER', name='Інший товар', qty=1, barcode='ONLY-456'))
            db.flush()
            scope, focus, numbers = parts_tools.locate(site, db, 'ONLY-456')
            self.assertEqual((scope, focus, numbers), ('all', 'ONLY-456', {'ONLY-456'}))

    def test_master_photo_selection_changes_cover_and_export_without_stock_changes(self):
        self.client.get('/admin/parts?warehouse_id=all')
        with self.client.session_transaction() as state:
            csrf = state['parts_photo_csrf']
        with patch.object(site, 'save_uploads', return_value=[]), patch.object(site, 'save_resized_export_photo', return_value='/uploads/new-export.jpg'):
            response = self.client.post('/admin/parts/photos', data={'part_number': '5Q0-123', 'csrf_token': csrf,
                                        'export_photo_existing': '/uploads/b.jpg'})
        self.assertEqual(response.status_code, 302)
        with self.sessions() as db:
            part = db.get(site.Part, 1)
            template = site.find_part_template(db, '5Q0-123')
            self.assertEqual(site.public_part_photo(part), '/uploads/b.jpg')
            self.assertEqual(site.primary_template_photo(template), '/uploads/b.jpg')
            self.assertEqual(part.photo_urls, '/uploads/new-export.jpg')
            self.assertEqual(part.qty, 3)
            self.assertEqual(part.storage_location, 'A-12')

    def test_photo_route_requires_csrf_and_existing_card(self):
        self.assertEqual(self.client.post('/admin/parts/photos', data={'part_number': '5Q0-123'}).status_code, 403)

    def test_cover_choice_also_updates_verified_stock_and_preserves_its_other_photos(self):
        with self.sessions.begin() as db:
            revision = site.RevisionPart(part_number='5Q0-123', part_key='5Q0123', name='Перевірена деталь',
                                         qty=2, location='REV-129', photo_urls='["/uploads/verified.jpg"]')
            db.add(revision)
            db.flush()
            verified = site.publish_revision_part(db, revision, warehouse=db.get(site.Warehouse, 1))
            template = site.find_part_template(db, '5Q0-123')
            template.showcase_photo_urls = '["/uploads/b.jpg", "/uploads/a.jpg"]'
            parts_tools.sync_photos(site, db, template)
            self.assertEqual(site.public_part_photo(verified), '/uploads/b.jpg')
            self.assertIn('/uploads/verified.jpg', site.part_gallery_urls(verified))
            self.assertEqual((verified.qty, verified.storage_location, revision.qty), (2, 'REV-129', 2))
            self.assertEqual(json.loads(revision.photo_urls)[0], '/uploads/b.jpg')
            # The next publication/server restart must retain the selected cover.
            site.publish_revision_part(db, revision)
            self.assertEqual(site.public_part_photo(verified), '/uploads/b.jpg')

    def test_selected_upload_becomes_cover_when_existing_choice_is_not_submitted(self):
        with self.sessions() as db, patch.object(site, 'save_uploads', return_value=['/uploads/new1.jpg', '/uploads/new2.jpg']), patch.object(site, 'save_resized_export_photo', return_value='/uploads/out.jpg'):
            _, gallery, _ = site.build_updated_part_media(db.get(site.Part, 1), [], 'qa', '1', '')
            self.assertEqual(json.loads(gallery)[0], '/uploads/new2.jpg')

    def test_locations_are_exposed_per_warehouse_and_in_mobile_tasks(self):
        with self.sessions() as db:
            lookup = site.build_mobile_lookup_payload(db, '5Q0-123')
            self.assertEqual(lookup['stocks'][0]['storageLocation'], 'A-12')
            self.assertTrue(lookup['matches'][0]['storageLocationHighlight'])
            self.assertEqual(site.serialize_part_card(db, db.get(site.Part, 1))['storageLocation'], 'A-12')
            self.assertEqual(site.serialize_part_picker_card(db, db.get(site.Part, 1))['storageLocation'], 'A-12')

    def test_telegram_location_entities_use_utf16_offsets_and_plain_text(self):
        text = site.build_inventory_telegram_message(part_number='OEM', title='Тест <b>', before_qty=1, after_qty=2,
                                                    storage_location='Полиця & 12')
        entities = site.location_entities(text)
        self.assertEqual(len(entities), 1)
        entity = entities[0]
        encoded = text.encode('utf-16-le')
        bold = encoded[entity['offset']*2:(entity['offset']+entity['length'])*2].decode('utf-16-le')
        self.assertEqual(bold, '📍 Місце на складі: Полиця & 12')
        self.assertEqual(site.location_entities('Без місця'), [])

    def test_locations_reach_packing_availability_orders_and_stock_bot(self):
        from app import telegram_stock as bot
        from types import SimpleNamespace
        with self.sessions.begin() as db:
            request = site.PackingRequest(status='open')
            available = site.AvailabilityRequest(warehouse_id=2, title='QA')
            order = site.Order(customer_name='QA')
            db.add_all([request, available, order])
            db.flush()
            packed = site.PackingRequestItem(request_id=request.id, part_id=1, part_number='5Q0-123', title='QA', expected_qty=1)
            checked = site.AvailabilityRequestItem(request_id=available.id, part_id=1, part_number='5Q0-123', title='QA', expected_qty=1)
            db.add_all([packed, checked])
            db.flush()
            self.assertEqual(site.serialize_packing_item(db, packed)['storageLocation'], 'A-12')
            self.assertEqual(site.serialize_availability_item(db, checked)['storageLocation'], 'A-12')
            command = SimpleNamespace(part_id=1, identifier='5Q0-123', before_qty=2, after_qty=3, delta=1, prom_message='OK')
            text = bot.current_text(command, db)
            self.assertIn('📍 Місце на складі: A-12', text)
            self.assertEqual(len(site.location_entities(text)), 1)

    def test_public_card_highlights_only_filled_location(self):
        with self.sessions() as db, site.app.test_request_context('/'):
            part = db.get(site.Part, 1)
            html = site.render_template('_product_card.html', part=part)
            self.assertIn('storage-location-badge', html)
            self.assertIn('A-12', html)
            part.storage_location = ''
            self.assertNotIn('storage-location-badge', site.render_template('_product_card.html', part=part))


if __name__ == '__main__':
    unittest.main()
