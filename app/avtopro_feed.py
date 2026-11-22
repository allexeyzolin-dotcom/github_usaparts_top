"""Deterministic Avtopro price files; no inventory mutations or network calls."""
import csv
import io
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP


class FeedError(ValueError):
    pass


def marked(value):
    return str(value or '').strip().casefold() not in {'', '0', 'false', 'no', 'ні', 'нет'}


DOWNLOAD_COLUMNS = ('Виробник', 'Код', 'Ціна', 'Кількість', 'Опис', 'Б/В',
                    'Реставрація', 'Фото', 'Вага, кг', 'Глибина, см', 'Ширина, см', 'Висота, см')


def parse_markup(value):
    try:
        markup = Decimal(str(value).strip().replace(',', '.'))
        if not markup.is_finite() or not 0 <= markup <= 10000 or markup != markup.quantize(Decimal('0.01')):
            raise InvalidOperation
    except (InvalidOperation, ValueError):
        raise FeedError('Вкажіть націнку від 0 до 10000 % із точністю до сотих.') from None
    return markup


def _price_rows(records, *, include_photos=True, markup_percent=0, price_policy='strict'):
    multiplier = 1 + parse_markup(markup_percent) / 100
    groups = {}
    for item in records:
        if item['deleted'] or not item['in_stock'] or item['qty'] <= 0:
            continue
        brand, code = str(item['brand'] or '').strip(), str(item['code'] or '').strip()
        if not brand:
            raise FeedError('Не вказано виробника товару.')
        if not code:
            raise FeedError('Не вказано артикул товару.')
        if any(c in brand + code for c in '\r\n\t'):
            raise FeedError('У наявного товару відсутній або некоректний виробник чи артикул.')
        try:
            price = Decimal(str(item['price']))
            if not price.is_finite() or price <= 0:
                raise InvalidOperation
            price = (price * multiplier).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            if price <= 0:
                raise InvalidOperation
        except (InvalidOperation, ValueError):
            raise FeedError(f'Некоректна ціна товару {brand} {code}.') from None
        used, restored = marked(item.get('used')), marked(item.get('restored'))
        key = (brand.casefold(), code.casefold(), used, restored)
        description = str(item.get('name') or '').replace('\r', ' ').replace('\n', ' ')
        photos = list(dict.fromkeys(item.get('photos') or [])) if include_photos else []
        if key in groups:
            row = groups[key]
            if price_policy == 'highest':
                row[2] = format(max(Decimal(row[2]), price), '.2f')
            elif row[2] != format(price, '.2f'):
                raise FeedError(f'Різні ціни для однакового товару {brand} {code}.')
            row[3] += item['qty']
            row[7] = list(dict.fromkeys(row[7] + photos))
        else:
            groups[key] = [brand, code, format(price, '.2f'), item['qty'],
                           '1' if used else '', '1' if restored else '', description, photos]
    if not groups:
        raise FeedError('На складі немає товарів для прайсу. Перевірте залишки та зняття товарів на Автопро вручну.')
    return [groups[key] for key in sorted(groups)]


def build_price_csv(records, *, include_photos=True, markup_percent=0, price_policy='strict'):
    rows = _price_rows(records, include_photos=include_photos, markup_percent=markup_percent, price_policy=price_policy)
    output = io.StringIO(newline='')
    writer = csv.writer(output, delimiter=';', lineterminator='\r\n')
    for row in rows:
        if 'фото за запитом!' not in row[6].casefold():
            row[6] = f'{row[6]} фото за запитом!'.strip()
        writer.writerow(row[:7] + [';'.join(row[7])])
    return output.getvalue().encode('utf-8-sig'), len(rows)


def review_download_records(records):
    """Report every invalid card, including both sides of a conflicting price."""
    groups, issues = {}, []
    for record in records:
        try:
            row = _price_rows([record])[0]
        except FeedError as exc:
            issues.append({**record, 'error': str(exc)})
            continue
        key = (row[0].casefold(), row[1].casefold(), row[4], row[5])
        groups.setdefault(key, []).append((record, row[2]))
    ready = []
    for entries in groups.values():
        if len({price for _, price in entries}) > 1:
            issues.extend({**record, 'error': 'Різні ціни для однакового виробника, артикула та стану.'}
                          for record, _ in entries)
        else:
            ready.extend(record for record, _ in entries)
    return ready, issues


def build_download_csv(records):
    """The 12-column manual export matches the user's Avtopro export example."""
    rows = _price_rows(records)
    output = io.StringIO(newline='')
    writer = csv.writer(output, delimiter=';', lineterminator='\r\n')
    writer.writerow(DOWNLOAD_COLUMNS)
    for row in rows:
        writer.writerow(row[:4] + [row[6], '+' if row[4] else '', '+' if row[5] else '',
                                  ','.join(row[7])] + [''] * 4)
    return output.getvalue().encode('utf-8-sig'), len(rows)
