"""Exercise generated files and access controls against disposable inventory."""
import csv
import io
import unittest
from decimal import Decimal
from unittest.mock import patch
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import app.app as site


class AvtoproFeedTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite://')
        site.Base.metadata.create_all(self.engine)
        self.sessions = sessionmaker(bind=self.engine)
        self.db_patch = patch.object(site, 'SessionLocal', self.sessions)
        self.db_patch.start()
        self.client = site.app.test_client()
        with self.sessions.begin() as db:
            db.add_all([site.Warehouse(id=1, name='Склад & 1', markup_percent=30),
                        site.Warehouse(id=2, name='Інший склад'),
                        site.Part(id=1, warehouse_id=1, brand='VAG', part_number='00123',
                                  name='Трубка; "нова"', description='PRIVATE STOCK NOTE', qty=2,
                                  in_stock=True, price_usd=Decimal('12.35'), avtopro_flag_1='0',
                                  photo_urls='["https://example.com/a.jpg", "https://example.com/b.jpg"]'),
                        site.Part(id=2, warehouse_id=1, brand='VAG', part_number='00123',
                                  name='Трубка', qty=3, in_stock=True, price_usd=Decimal('12.35')),
                        site.Part(id=3, warehouse_id=1, brand='BMW', part_number='USED', qty=1,
                                  in_stock=True, price_usd=Decimal('50.90'), avtopro_flag_1='1'),
                        site.Part(id=4, warehouse_id=1, brand='BMW', part_number='SOLD', qty=0,
                                  in_stock=False, price_usd=1),
                        site.Part(id=5, warehouse_id=1, brand='BMW', part_number='DELETED', qty=4,
                                  in_stock=True, is_deleted=True, price_usd=1),
                        site.Part(id=6, warehouse_id=2, brand='BMW', part_number='OTHER', qty=8,
                                  in_stock=True, price_usd=10),
                        site.Part(id=7, warehouse_id=1, brand='BMW', part_number='UNAVAILABLE', qty=4,
                                  in_stock=False, price_usd=1)])

    def tearDown(self):
        self.db_patch.stop()
        self.engine.dispose()

    def login(self):
        with self.client.session_transaction() as state:
            state['admin_auth'] = True

    def token(self, warehouse=1):
        with self.sessions() as db:
            record = db.query(site.ApiSetting).filter_by(setting_key=site.avtopro_feed_setting(warehouse)).one_or_none()
            return record.setting_value if record else ''

    def change(self, action='enable', warehouse=1):
        self.login()
        self.assertEqual(self.client.get('/admin/avtopro-feeds').status_code, 200)
        with self.client.session_transaction() as state:
            csrf = state['avtopro_feed_form_token']
        return self.client.post('/admin/avtopro-feeds', data={'action': action, 'warehouse_id': warehouse, 'csrf_token': csrf})

    def feed(self, warehouse=1, token=None):
        return self.client.get(f'/feeds/avtopro/{warehouse}/{token or self.token(warehouse)}/prices.csv')

    def test_anonymous_access_and_csrf_cannot_enable_feeds(self):
        self.assertEqual(self.client.get('/admin/avtopro-feeds').status_code, 302)
        self.assertEqual(self.client.post('/admin/avtopro-feeds', data={'warehouse_id': 1, 'action': 'enable'}).status_code, 302)
        self.login()
        self.assertEqual(self.client.post('/admin/avtopro-feeds', data={'warehouse_id': 1, 'action': 'enable'}).status_code, 403)
        self.assertEqual(self.token(), '')
        self.assertEqual(self.feed(token='wrong').status_code, 404)

    def test_pricelist_preserves_cents_codes_state_and_csv_escaping(self):
        self.change()
        response = self.feed()
        self.assertEqual(response.status_code, 200)
        rows = list(csv.reader(io.StringIO(response.data.decode('utf-8-sig')), delimiter=';'))
        self.assertEqual(len(rows), 2)
        new = next(row for row in rows if row[1] == '00123')
        self.assertEqual(new[:6], ['VAG', '00123', '12.35', '5', '', ''])
        self.assertEqual(new[6], 'Трубка; "нова"')
        self.assertEqual(new[7], 'https://example.com/a.jpg;https://example.com/b.jpg')
        self.assertEqual(next(row for row in rows if row[1] == 'USED')[4], '1')
        for private in ['PRIVATE STOCK NOTE', 'OTHER', 'DELETED', 'SOLD', 'UNAVAILABLE']:
            self.assertNotIn(private, response.data.decode('utf-8-sig'))
        self.assertEqual(response.headers['Cache-Control'], 'no-store')
        self.assertIn('noindex', response.headers['X-Robots-Tag'])
        self.assertIn('attachment', response.headers['Content-Disposition'])

    def test_warehouse_token_is_scoped_and_can_be_revoked(self):
        self.change()
        token = self.token()
        self.assertGreaterEqual(len(token), 32)
        self.assertEqual(self.feed(2, token=token).status_code, 404)
        self.change()
        self.assertEqual(token, self.token())
        self.change('disable')
        self.assertEqual(self.feed(token=token).status_code, 404)
        self.change()
        self.assertNotEqual(token, self.token())

    def test_file_changes_with_inventory_and_never_mutates_it(self):
        self.change()
        first = self.feed()
        self.assertEqual(first.data, self.feed().data)
        with self.sessions.begin() as db:
            db.get(site.Part, 1).qty = 1
        second = self.feed()
        self.assertNotEqual(first.headers['ETag'], second.headers['ETag'])
        with self.sessions() as db:
            self.assertEqual(db.get(site.Part, 1).qty, 1)
            self.assertEqual(db.get(site.Part, 2).qty, 3)
        self.assertIn(b'12.35;4;', second.data)

    def test_invalid_price_and_conflicts_fail_entire_file(self):
        self.change()
        for price in [0, Decimal('15.40')]:
            with self.sessions.begin() as db:
                db.get(site.Part, 2).price_usd = price
            response = self.feed()
            self.assertEqual(response.status_code, 503)
            self.assertNotIn('text/csv', response.content_type)

    def test_empty_stock_fails_instead_of_publishing_empty_price_file(self):
        self.change()
        with self.sessions.begin() as db:
            for part in db.query(site.Part).filter_by(warehouse_id=1):
                part.qty = 0
        self.assertEqual(self.feed().status_code, 503)

    def test_admin_page_lists_scoped_urls_without_network_calls(self):
        self.change()
        response = self.client.get('/admin/avtopro-feeds')
        self.assertEqual(response.status_code, 200)
        self.assertIn('Склад &amp; 1', response.text)
        self.assertIn(self.token(), response.text)
        self.assertIn('Порожній прайс не видається', response.text)
        self.assertEqual(response.headers['Referrer-Policy'], 'no-referrer')


if __name__ == '__main__':
    unittest.main()
