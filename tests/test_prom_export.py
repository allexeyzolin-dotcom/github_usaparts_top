"""Prom exports use the agreed XLSX layout without changing stock or writing to Prom."""
import io
import json
import unittest
from decimal import Decimal
from unittest.mock import patch
import xml.etree.ElementTree as ET
import zipfile

import test_inventory_revision as fixtures
from app.prom_export import HEADERS, NS, prepare_products, download_bytes, read_reference

site = fixtures.site


def cells(payload):
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        assert archive.testzip() is None
        root = ET.fromstring(archive.read('xl/worksheets/sheet1.xml'))
        result = {}
        for cell in root.findall(f'.//{{{NS}}}c'):
            node = cell.find(f'{{{NS}}}v')
            value = node.text if node is not None else ''.join(cell.find(f'{{{NS}}}is').itertext())
            result[cell.get('r')] = (value or '', cell.get('t'), cell.get('s'))
        return result


class PromExportTests(unittest.TestCase):
    tearDown = fixtures.InventoryRevisionTests.tearDown
    login = fixtures.InventoryRevisionTests.login
    save = fixtures.InventoryRevisionTests.save

    def setUp(self):
        fixtures.InventoryRevisionTests.setUp(self)
        site.public_cache_clear()
        self.reference = {'source': 'Тестовий довідник.xlsx', 'records': []}
        self.reference_patch = patch('app.prom_revision_export.load_reference', side_effect=lambda *args: self.reference)
        self.reference_patch.start()
        self.addCleanup(self.reference_patch.stop)

    def create(self, **changes):
        self.login()
        response = self.save(brand='VAG', **changes)
        self.assertEqual(response.status_code, 200, response.text)
        return response.json['card']

    def download(self, **options):
        return self.client.post('/admin/revision/export/prom', data={
            'revision_token': 'revision-test-token', 'action': 'download',
            'rate': '46', 'markup': '25', 'chunk_size': '640', 'include_zero': '1', **options})

    def test_login_and_csrf_are_required(self):
        self.assertEqual(self.client.get('/admin/revision/export/prom').status_code, 302)
        self.login()
        self.assertEqual(self.client.post('/admin/revision/export/prom', data={'action': 'download'}).status_code, 403)
        self.assertEqual(self.client.get('/api/mobile/inventory-revision/export/prom').status_code, 401)

    def test_agreed_currency_markup_keywords_and_numeric_two_decimal_price(self):
        self.create(part_number='000123', name='Топливный насос VAG', price_usd='12.50', cross_numbers='CROSS-1')
        response = self.download()
        self.assertEqual(response.status_code, 200)
        self.assertIn('.xlsx', response.headers['Content-Disposition'])
        values = cells(response.data)
        self.assertEqual(values['A2'][0], '000123')
        self.assertEqual(values['A2'][1], 'inlineStr')
        self.assertEqual(values['I2'], ('718.75', 'n', '1'))
        self.assertEqual(values['J2'][0], 'UAH')
        self.assertEqual(values['N2'][0], '2')
        self.assertIn('паливний', values['B2'][0].casefold())
        self.assertIn('топливный', values['C2'][0].casefold())
        for coordinate in ['D2', 'E2']:
            self.assertIn('000123', values[coordinate][0])
            self.assertIn('CROSS-1', values[coordinate][0])
            self.assertLessEqual(len(values[coordinate][0]), 1024)
        self.assertIn('Артикул', values['F2'][0])
        self.assertIn('Производитель', values['G2'][0])
        with zipfile.ZipFile(io.BytesIO(response.data)) as archive:
            workbook = ET.fromstring(archive.read('xl/workbook.xml'))
            self.assertEqual([node.get('name') for node in workbook.findall(f'{{{NS}}}sheets/{{{NS}}}sheet')],
                             ['Export Products Sheet', 'Export Groups Sheet'])

    def test_existing_prom_id_and_internal_sku_are_reused(self):
        card = self.create()
        with self.sessions.begin() as db:
            part_id = db.get(site.RevisionPublication, card['id']).part_id
            db.add(site.marketplace_models.Link(part_id=part_id, remote_id='2795634464',
                                               external_id='original-0001', sku='0001'))
        values = cells(self.download().data)
        self.assertEqual(values['A2'][0], '0001')
        self.assertEqual(values['S2'][0], 'original-0001')
        self.assertEqual(values['T2'][0], '2795634464')

    def test_new_id_is_stable_when_price_quantity_or_manufacturer_changes(self):
        card = self.create()
        first = cells(self.download().data)['S2'][0]
        response = self.save(card, action='update', target_id=card['id'], brand='BMW', price_usd='20', qty='3')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(cells(self.download().data)['S2'][0], first)

    def test_category_and_catalog_photo_from_exact_reference(self):
        self.create(part_number='CAT-001')
        self.reference['records'] = [{'codes': ['CAT001'], 'sku': 'CAT-001', 'brand': 'VAG',
            'remote_id': '12345', 'external_id': 'external-cat', 'category_id': '341585',
            'category_url': 'https://prom.ua/Test', 'photos': ['https://images.prom.ua/example.jpg'],
            'name_uk': 'Насос паливний', 'name_ru': 'Насос топливный'}]
        values = cells(self.download().data)
        self.assertEqual(values['V2'][0], '341585')
        self.assertEqual(values['W2'][0], 'https://prom.ua/Test')
        self.assertEqual(values['O2'][0], 'https://images.prom.ua/example.jpg')
        self.assertIn('Ілюстративне', values['F2'][0])
        self.assertEqual(values['T2'][0], '12345')

    def test_zero_stock_is_exported_as_unavailable_and_can_be_excluded(self):
        card = self.create()
        self.save(card, action='update', target_id=card['id'], qty='0')
        self.assertEqual(cells(self.download().data)['M2'][0], '-')
        self.assertEqual(cells(self.download().data)['N2'][0], '0')
        response = self.download(include_zero='')
        self.assertNotIn('attachment', response.headers.get('Content-Disposition', ''))

    def test_duplicate_counts_merge_but_conflicting_prices_require_a_choice(self):
        self.create()
        self.create(duplicate_action='new_card', qty='3')
        self.assertEqual(cells(self.download().data)['N2'][0], '5')
        self.create(part_number='BAD', price_usd='0')
        blocked = self.download()
        self.assertNotIn('attachment', blocked.headers.get('Content-Disposition', ''))
        accepted = self.download(only_ready='1')
        self.assertIn('.xlsx', accepted.headers['Content-Disposition'])
        self.assertEqual(accepted.headers['X-Skipped-Card-Count'], '1')

    def test_export_does_not_change_counted_or_main_stock(self):
        card = self.create()
        with self.sessions() as db:
            before = [(row.id, row.qty, row.price_usd) for row in db.query(site.Part)]
            version = db.get(site.RevisionPart, card['id']).version
        self.download()
        with self.sessions() as db:
            self.assertEqual(before, [(row.id, row.qty, row.price_usd) for row in db.query(site.Part)])
            self.assertEqual(version, db.get(site.RevisionPart, card['id']).version)

    def test_invalid_pricing_options_never_download(self):
        self.create()
        for change in [{'rate': 'NaN'}, {'rate': '-1'}, {'markup': '-1'}, {'markup': '501'}, {'chunk_size': '0'}]:
            with self.subTest(change=change):
                self.assertNotIn('attachment', self.download(**change).headers.get('Content-Disposition', ''))

    def test_confirmed_category_is_retained_only_while_the_article_matches(self):
        card = self.create()
        self.assertEqual(cells(self.download(**{f'category_{card["id"]}': '12345'}).data)['V2'][0], '12345')
        self.assertEqual(cells(self.download().data)['V2'][0], '12345')
        self.save(card, action='update', target_id=card['id'], part_number='OTHER-ARTICLE')
        self.assertEqual(cells(self.download().data)['V2'][0], '')


class PromWorkbookTests(unittest.TestCase):
    def records(self):
        return [{'id': 1, 'code': '000123', 'brand': 'VAG', 'name': 'Насос паливний',
                 'price': Decimal('10'), 'qty': 1, 'used': True, 'photos': [], 'part_id': None}]

    def test_split_at_640_and_valid_xlsx_roundtrip(self):
        ready, issues = prepare_products(self.records(), [], [], Decimal('46'), Decimal('25'))
        self.assertFalse(issues)
        rows = [{**ready[0], 'code': f'CODE{i}', 'external_id': f'export-{i}'} for i in range(641)]
        payload, filename, mimetype = download_bytes(rows, 640)
        self.assertEqual(filename, 'prom-revision.zip')
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            self.assertEqual(archive.namelist(), ['prom-revision-01-640.xlsx', 'prom-revision-02-1.xlsx'])
            self.assertIn('A641', cells(archive.read(archive.namelist()[0])))
            self.assertNotIn('A3', cells(archive.read(archive.namelist()[1])))
        single, _, _ = download_bytes(ready, 640)
        reference = read_reference(single)
        self.assertEqual(reference['records'][0]['codes'], ['000123'])
        self.assertEqual(len(HEADERS), 37)

    def test_ambiguous_remote_identity_is_reported(self):
        links = [{'remote_id': '1', 'external_id': '', 'sku': '000123', 'part_id': None},
                 {'remote_id': '2', 'external_id': '', 'sku': '000123', 'part_id': None}]
        ready, issues = prepare_products(self.records(), [], links, Decimal('46'), Decimal('25'))
        self.assertFalse(ready)
        self.assertIn('Кілька відповідників', issues[0]['error'])

    def test_bad_reference_is_rejected(self):
        with self.assertRaises(ValueError):
            read_reference(b'not an xlsx')

    def test_same_code_with_different_manufacturer_never_updates_other_product(self):
        reference = [{'codes': ['000123'], 'brand': 'BMW', 'remote_id': '12345',
                      'category_id': '999', 'photos': ['https://example.com/other.jpg']}]
        ready, issues = prepare_products(self.records(), reference, [], Decimal('46'), Decimal('25'))
        self.assertFalse(ready)
        self.assertIn('Виробник не збігається', issues[0]['error'])
        reference[0]['brand'] = ''
        ready, issues = prepare_products(self.records(), reference, [], Decimal('46'), Decimal('25'))
        self.assertFalse(issues)
        self.assertEqual(ready[0]['remote_id'], '12345')
