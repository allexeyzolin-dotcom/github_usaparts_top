"""Customer waybills use disposable SQLite and a mocked Nova Poshta API."""
from datetime import datetime, timedelta
import unittest
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import app.app as site
from app import order_tracking as tracking


class OrderTrackingTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite://')
        site.Base.metadata.create_all(self.engine)
        self.sessions = sessionmaker(bind=self.engine, expire_on_commit=False)
        self.addCleanup(self.engine.dispose)
        self.addCleanup(patch.stopall)
        patch.object(site, 'SessionLocal', self.sessions).start()
        patch.object(site, 'engine', self.engine).start()
        patch.object(site, 'track_daily_guest_visit').start()
        self.np = patch.object(site, 'np_request', return_value=([], {'success': True})).start()
        self.clock = datetime(2026, 11, 17, 10, 0)
        self.client = site.app.test_client()
        with self.client.session_transaction() as state:
            state['admin_auth'] = True
        with self.sessions.begin() as db:
            db.add(site.Warehouse(id=1, name='Тестовий склад'))
            db.flush()
            db.add(site.Part(id=1, warehouse_id=1, part_number='OEM-1', name='Радіатор', qty=7, in_stock=True))

    def create(self, id=1, ttn='20450000000001', status='shipped', **fields):
        with self.sessions.begin() as db:
            order = site.Order(id=id, customer_name=f'Клієнт {id}', ttn=ttn, status=status,
                               delivery_type='nova_poshta', **fields)
            order.items.append(site.OrderItem(part_id=1, part_number='OEM-1', name='Радіатор', qty=1, price_usd=20))
            db.add(order)

    def snapshot(self, ttn='20450000000001', code='5', status='Відправлення прямує до міста'):
        return {'Number': ttn, 'StatusCode': code, 'Status': status}

    def run_check(self, snapshots=None, now=None):
        if snapshots is not None:
            self.np.return_value = (snapshots, {'success': True})
        tracking.run_once(now or self.clock)

    def test_received_status_never_writes_off_inventory_or_completes_order(self):
        self.create()
        self.run_check([self.snapshot(code='9', status='Відправлення отримано')])
        with self.sessions() as db:
            order = db.get(site.Order, 1)
            self.assertEqual(order.ttn_status, 'Відправлення отримано')
            self.assertEqual(order.status, 'shipped')
            self.assertFalse(order.stock_reserved)
            self.assertEqual(db.get(site.Part, 1).qty, 7)
            self.assertTrue(tracking.serialize(db, order)['received'])

    def test_received_prom_order_moves_to_history_without_writing_off_inventory(self):
        self.create(status='processing', external_source='prom', stock_reserved=True, is_processing=True)
        self.create(id=2, status='new', ttn='')
        start = self.client.get('/admin/orders?group=active')
        self.assertIn('data-order-tag>Prom</span>', start.text)
        self.assertIn('data-order-tag>Сайт</span>', self.client.get('/admin/orders?group=new').text)
        self.run_check([self.snapshot(code='9', status='Відправлення отримано')])
        status = self.client.get('/admin/orders/tracking-status?id=1').json['orders'][0]
        self.assertEqual(status['group'], 'history')
        self.assertTrue(status['promProcessed'])
        self.assertNotIn('data-tracking-order="1"', self.client.get('/admin/orders?group=active').text)
        history = self.client.get('/admin/orders?group=history')
        self.assertIn('data-tracking-order="1"', history.text)
        self.assertIn('Опрацьовано на Prom', history.text)
        with self.sessions() as db:
            order = db.get(site.Order, 1)
            self.assertTrue(order.stock_reserved)
            self.assertFalse(order.is_processing)
            self.assertEqual(db.get(site.Part, 1).qty, 7)

    def test_cached_received_prom_waybill_is_completed_and_cancelled_is_skipped(self):
        self.create(external_source='prom')
        self.create(id=2, external_source='prom', external_status='canceled')
        with self.sessions.begin() as db:
            db.add_all(tracking.MODELS.Tracking(order_id=id, ttn='20450000000001',
                           status_code='9', checked_at=self.clock - timedelta(days=1)) for id in (1, 2))
        self.run_check()
        self.np.assert_not_called()
        with self.sessions.begin() as db:
            order = db.get(site.Order, 1)
            self.assertEqual(order.status, 'done')
            self.assertEqual(db.get(site.Order, 2).status, 'shipped')
            order.ttn = '20450000000002'
            self.assertTrue(tracking.serialize(db, order)['promProcessed'])

    def test_batches_at_100_and_matches_response_numbers_instead_of_positions(self):
        for id in range(1, 103):
            self.create(id=id, ttn=f'2045{id:010d}')
        self.create(id=103, ttn='20450000000001')  # shared waybill
        def answer(db, model, method, props):
            docs = props['Documents']
            return ([self.snapshot(doc['DocumentNumber'], status='Статус '+doc['DocumentNumber']) for doc in reversed(docs)], {'success': True})
        self.np.side_effect = answer
        self.run_check()
        self.assertEqual([len(call.args[3]['Documents']) for call in self.np.call_args_list], [100, 2])
        with self.sessions() as db:
            for order in db.query(site.Order).all():
                self.assertEqual(order.ttn_status, 'Статус '+order.ttn)

    def test_polling_waits_five_minutes_and_retries_errors_without_losing_status(self):
        self.create()
        self.run_check([self.snapshot()])
        self.np.side_effect = RuntimeError('must not disclose credentials from exceptions')
        self.run_check(now=self.clock+timedelta(minutes=4, seconds=59))
        self.assertEqual(self.np.call_count, 1)
        self.run_check(now=self.clock+timedelta(minutes=5))
        self.assertEqual(self.np.call_count, 2)
        with self.sessions() as db:
            state = tracking.serialize(db, db.get(site.Order, 1))
            self.assertEqual(state['status'], 'Відправлення прямує до міста')
            self.assertIn('через 5 хвилин', state['error'])
            self.assertNotIn('credentials', state['error'])
            self.assertEqual(state['lastSuccessAt'], '17.11 12:00')

    def test_changed_waybill_resets_terminal_tracking(self):
        self.create()
        self.run_check([self.snapshot(code='9')])
        self.run_check(now=self.clock+timedelta(days=1))
        self.assertEqual(self.np.call_count, 1)
        with self.sessions.begin() as db:
            db.get(site.Order, 1).ttn = '20450000000002'
        self.run_check([self.snapshot(ttn='20450000000002', code='5', status='Нова ТТН')], now=self.clock+timedelta(days=1))
        with self.sessions() as db:
            state = tracking.serialize(db, db.get(site.Order, 1))
            self.assertEqual(state['status'], 'Нова ТТН')
            self.assertFalse(state['received'])

    def test_done_and_cancelled_orders_are_not_polled(self):
        self.create(status='done')
        self.create(id=2, status='cancelled')
        self.run_check()
        self.np.assert_not_called()

    def test_bad_waybill_does_not_make_a_network_call(self):
        self.create(ttn='123')
        self.run_check()
        self.np.assert_not_called()
        with self.sessions() as db:
            self.assertIn('14 цифр', tracking.serialize(db, db.get(site.Order, 1))['error'])

    def test_missing_or_unsuccessful_response_preserves_last_status(self):
        self.create()
        self.run_check([self.snapshot()])
        for offset, result in [(5, ([], {'success': True})), (10, ([self.snapshot(code='9')], {'success': False}))]:
            self.np.return_value = result
            self.run_check(now=self.clock+timedelta(minutes=offset))
            with self.sessions() as db:
                state = tracking.serialize(db, db.get(site.Order, 1))
                self.assertEqual(state['status'], 'Відправлення прямує до міста')
                self.assertTrue(state['error'])

    def test_waybill_replaced_while_carrier_is_responding_is_not_overwritten(self):
        self.create()
        def answer(*args):
            with self.sessions.begin() as db:
                db.get(site.Order, 1).ttn = '20450000000002'
            return ([self.snapshot(code='9', status='Старе відправлення отримано')], {'success': True})
        self.np.side_effect = answer
        self.run_check()
        with self.sessions() as db:
            self.assertEqual(db.get(site.Order, 1).ttn_status, '')

    def test_cancellation_during_carrier_request_is_respected(self):
        self.create()
        def answer(*args):
            with self.sessions.begin() as db:
                db.get(site.Order, 1).status = 'cancelled'
            return ([self.snapshot(code='9')], {'success': True})
        self.np.side_effect = answer
        self.run_check()
        with self.sessions() as db:
            self.assertEqual(db.get(site.Order, 1).status, 'cancelled')
            self.assertEqual(db.get(site.Order, 1).ttn_status, '')

    def test_read_endpoint_is_private_cached_and_does_not_call_np(self):
        self.create()
        response = self.client.get('/admin/orders/tracking-status?id=1&id=99999999999999999999')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['Cache-Control'], 'no-store')
        self.assertEqual(response.json['orders'][0]['id'], 1)
        self.np.assert_not_called()
        anonymous = site.app.test_client().get('/admin/orders/tracking-status?id=1')
        self.assertEqual(anonymous.status_code, 302)

    def test_new_layout_renders_all_groups_and_searches_real_order_fields(self):
        self.create(status='new')
        self.create(id=2, status='shipped')
        for group in ('new', 'active', 'manual', 'history'):
            response = self.client.get('/admin/orders', query_string={'group': group})
            self.assertEqual(response.status_code, 200)
            self.assertIn('cabinet.css', response.text)
            self.assertIn('Нова ручна видача', response.text)
        response = self.client.get('/admin/orders?group=active&q=OEM-1')
        self.assertIn('id="order-2"', response.text)
        self.assertNotIn('id="order-1"', response.text)
        response = self.client.get('/admin/orders?group=active&q=НЕМАЄ')
        self.assertNotIn('data-tracking-order=', response.text)

    def test_manual_refresh_uses_the_same_safe_tracking_path(self):
        self.create()
        self.np.return_value = ([self.snapshot(code='9', status='Відправлення отримано')], {'success': True})
        response = self.client.post('/admin/orders/1/refresh-ttn')
        self.assertEqual(response.status_code, 302)
        with self.sessions() as db:
            self.assertEqual(db.get(site.Part, 1).qty, 7)
            self.assertEqual(db.get(site.Order, 1).status, 'shipped')
            self.assertTrue(tracking.serialize(db, db.get(site.Order, 1))['received'])


if __name__ == '__main__':
    unittest.main()
