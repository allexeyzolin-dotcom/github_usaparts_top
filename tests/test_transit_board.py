import unittest
from datetime import date
from app.transit_board import build_batches


class TransitBoardTests(unittest.TestCase):
    def row(self, **changes):
        return {'id':1,'batchId':'batch','shortDescription':'Доставка','title':'Радіатор',
                'fullDescription':'','createdAt':'17.11.2026','delivery':{},'qty':2,'acceptedQty':0,
                'archivedAt':'','partNumber':'OEM-1','photoUrl':'','storageLocation':'A-1',**changes}

    def test_partial_receipt_combines_archived_stock_and_current_scans(self):
        batch = build_batches([self.row(id=2,qty=3,acceptedQty=1)],
            [self.row(qty=2,acceptedQty=2,archivedAt='17.11.2026')])[0]
        self.assertEqual((batch['qty'],batch['stocked'],batch['scanned']), (5,2,1))
        self.assertEqual(batch['stage'], 'receiving')
        self.assertEqual(batch['rowCount'], 1)
        self.assertEqual(batch['rows'][0]['stocked'], 2)
        self.assertEqual([row['id'] for row in batch['ready']], [2])

    def test_carrier_status_routes_batches_to_the_correct_columns(self):
        for metadata, expected in [({},'route'),({'npCode':'8'},'arrived'),({'npCode':'9','pickupNotified':True},'receiving')]:
            with self.subTest(expected=expected):
                batch = build_batches([self.row(delivery=metadata)], [], today=date(2026,11,17))[0]
                self.assertEqual(batch['stage'], expected)
        done = build_batches([], [self.row(archivedAt='17.11.2026')])[0]
        self.assertEqual(done['stage'], 'done')
        self.assertFalse(done['attention'])

    def test_arrival_does_not_claim_romny_when_another_city_is_returned(self):
        batch = build_batches([self.row(delivery={'npCode':'8','recipientCity':'Київ'})], [])[0]
        self.assertIn('Київ', batch['label'])
        self.assertNotIn('Ромн', batch['label'])

    def test_overdue_and_search_include_missing_position_codes(self):
        batch = build_batches([self.row(delivery={'expectedDate':'2026-11-16','ttn':'20450000000001'})],[],today=date(2026,11,17))[0]
        self.assertTrue(batch['late'])
        self.assertTrue(batch['attention'])
        self.assertIn('OEM-1', batch['search'])
        self.assertIn('20450000000001', batch['search'])


if __name__ == '__main__':
    unittest.main()
