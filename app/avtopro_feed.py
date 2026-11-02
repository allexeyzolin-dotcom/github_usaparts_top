"""Deterministic Avtopro price files; no inventory mutations or network calls."""
import csv
import io
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP


class FeedError(ValueError):
    pass


def marked(value):
    return str(value or '').strip().casefold() not in {'', '0', 'false', 'no', 'ні', 'нет'}


def build_price_csv(records):
    groups = {}
    for item in records:
        if item['deleted'] or not item['in_stock'] or item['qty'] <= 0:
            continue
        brand, code = str(item['brand']).strip(), str(item['code']).strip()
        if not brand or not code or any(c in brand + code for c in '\r\n\t'):
            raise FeedError('У наявного товару відсутній або некоректний виробник чи артикул.')
        try:
            price = Decimal(str(item['price']))
            if not price.is_finite() or price <= 0:
                raise InvalidOperation
            price = price.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            if price <= 0:
                raise InvalidOperation
        except (InvalidOperation, ValueError):
            raise FeedError(f'Некоректна ціна товару {brand} {code}.') from None
        used, restored = marked(item.get('used')), marked(item.get('restored'))
        key = (brand.casefold(), code.casefold(), used, restored)
        description = str(item.get('name') or '').replace('\r', ' ').replace('\n', ' ')
        photos = list(dict.fromkeys(item.get('photos') or []))
        if key in groups:
            row = groups[key]
            if row[2] != format(price, '.2f'):
                raise FeedError(f'Різні ціни для однакового товару {brand} {code}.')
            row[3] += item['qty']
            row[7] = list(dict.fromkeys(row[7] + photos))
        else:
            groups[key] = [brand, code, format(price, '.2f'), item['qty'],
                           '1' if used else '', '1' if restored else '', description, photos]
    if not groups:
        raise FeedError('На складі немає товарів для прайсу. Перевірте залишки та зняття товарів на Автопро вручну.')
    output = io.StringIO(newline='')
    writer = csv.writer(output, delimiter=';', lineterminator='\r\n')
    for key in sorted(groups):
        row = groups[key]
        writer.writerow(row[:7] + [';'.join(row[7])])
    return output.getvalue().encode('utf-8-sig'), len(groups)
