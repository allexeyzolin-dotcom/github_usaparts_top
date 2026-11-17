"""Presentation for the inbound board, including already received batch rows."""
from datetime import datetime
from zoneinfo import ZoneInfo


def build_batches(active, archived, today=None):
    today = today or datetime.now(ZoneInfo('Europe/Kyiv')).date()
    batches = {}
    for item in [*active, *archived]:
        key = item['batchId'] or f"legacy-{item['id']}"
        if key not in batches:
            batches[key] = {'id': key, 'name': item['shortDescription'] or item['title'],
                'description': item['fullDescription'], 'createdAt': item['createdAt'],
                'delivery': item['delivery'], 'items': [], 'rows': {}, 'qty': 0, 'stocked': 0, 'scanned': 0}
        batch = batches[key]
        batch['items'].append(item)
        received = item['qty'] if item['archivedAt'] else 0
        scanned = min(item['acceptedQty'], item['qty']) if not item['archivedAt'] else 0
        batch['qty'] += item['qty']
        batch['stocked'] += received
        batch['scanned'] += scanned
        row = batch['rows'].setdefault(item['partNumber'], {'oem': item['partNumber'], 'title': item['title'],
            'photo': item['photoUrl'], 'qty': 0, 'stocked': 0, 'scanned': 0, 'locations': [], 'active': []})
        row['qty'] += item['qty']
        row['stocked'] += received
        row['scanned'] += scanned
        if item['storageLocation'] and item['storageLocation'] not in row['locations']:
            row['locations'].append(item['storageLocation'])
        if not item['archivedAt']:
            row['active'].append(item)
    for batch in batches.values():
        delivery = batch['delivery']
        complete = batch['stocked'] == batch['qty']
        picked = delivery.get('pickupNotified') or bool(delivery.get('pickedAt'))
        arrived = delivery.get('npCode') in {'7', '8'}
        stage = 'done' if complete else 'receiving' if picked or batch['scanned'] or batch['stocked'] else 'arrived' if arrived else 'route'
        batch['stage'] = stage
        expected = delivery.get('expectedDate', '')
        batch['late'] = bool(stage == 'route' and expected and expected < today.isoformat())
        batch['attention'] = bool(not complete and (delivery.get('error') or stage in {'arrived', 'receiving'} or (expected and expected <= today.isoformat())))
        recipient = delivery.get('recipientCity') or ''
        arrival_label = 'На НП у Ромнах' if recipient.casefold().startswith('ромн') else f'На НП · {recipient}' if recipient else 'На Новій пошті'
        batch['label'] = 'Дата минула' if batch['late'] else {'done': 'На складі', 'route': 'В дорозі',
            'arrived': arrival_label, 'receiving': 'Забрали з НП' if picked else 'Очікує прийому'}[stage]
        batch['icon'] = 'circle-alert' if batch['late'] else {'done': 'check-check', 'route': 'truck', 'arrived': 'package-check', 'receiving': 'check'}[stage]
        batch['tone'] = 'late' if batch['late'] else stage
        batch['dateLabel'] = expected[8:10] + '.' + expected[5:7] if expected else 'Дата не визначена'
        batch['rowCount'] = len(batch['rows'])
        batch['rows'] = list(batch['rows'].values())
        batch['shortId'] = 'TD-' + str(min(item['id'] for item in batch['items'])).zfill(3)
        batch['search'] = ' '.join([batch['name'], batch['shortId'], delivery.get('ttn', ''), delivery.get('carrierName', ''),
                                  *[row['oem'] + ' ' + row['title'] for row in batch['rows']]])
        batch['ready'] = [item for item in batch['items'] if not item['archivedAt'] and item['acceptedQty'] > 0]
    return list(batches.values())
