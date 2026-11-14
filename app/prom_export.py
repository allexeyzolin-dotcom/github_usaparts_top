"""Prom's XLSX import contract. Pure transformations; no marketplace writes."""
from collections import Counter
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import hashlib
from html import escape
import io
import re
import xml.etree.ElementTree as ET
import zipfile

from .product_search import clean_text, ukrainian_variant, UKRAINIAN_TERMS

NS = 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'
HEADERS = ['Код_товару', 'Назва_позиції_укр', 'Назва_позиції', 'Пошукові_запити_укр',
           'Пошукові_запити', 'Опис_укр', 'Опис', 'Тип_товару', 'Ціна', 'Валюта',
           'Одиниця_виміру', 'Мінімальний_обсяг_замовлення', 'Наявність', 'Кількість',
           'Посилання_зображення', 'Назва_групи', 'Назва_групи_укр', 'Ідентифікатор_групи',
           'Ідентифікатор_товару', 'Унікальний_ідентифікатор', 'Виробник',
           'Ідентифікатор_підрозділу', 'Посилання_підрозділу', 'HTML_заголовок_укр',
           'HTML_заголовок', 'HTML_опис_укр', 'HTML_опис', 'Номер_пристрою_(MPN)']
for _ in range(3):
    HEADERS += ['Назва_Характеристики', 'Одиниця_виміру_Характеристики', 'Значення_Характеристики']
GROUP_HEADERS = ['Номер_групи', 'Назва_групи', 'Назва_групи_укр', 'Ідентифікатор_групи']


def key(value):
    return ''.join(c for c in clean_text(value).upper() if c.isalnum())


def decimal_option(value, label, minimum, maximum):
    try:
        result = Decimal(str(value).replace(',', '.'))
        if not result.is_finite() or not minimum <= result <= maximum:
            raise InvalidOperation
        return result
    except (ValueError, InvalidOperation):
        raise ValueError(f'{label}: допустиме значення від {minimum} до {maximum}.') from None


def read_reference(content, filename='Prom.xlsx'):
    """Keep public product metadata only; never retain notes, tokens or customer data."""
    if len(content) > 20 * 1024 * 1024:
        raise ValueError('Файл Prom має бути не більшим за 20 МБ.')
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            if sum(item.file_size for item in archive.infolist()) > 80 * 1024 * 1024:
                raise ValueError('Завеликий розпакований XLSX.')
            strings = []
            if 'xl/sharedStrings.xml' in archive.namelist():
                strings = [''.join(node.itertext()) for node in ET.fromstring(archive.read('xl/sharedStrings.xml')).findall(f'{{{NS}}}si')]
            workbook = ET.fromstring(archive.read('xl/workbook.xml'))
            first = workbook.find(f'{{{NS}}}sheets/{{{NS}}}sheet')
            rel_id = first.get('{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id')
            rels = ET.fromstring(archive.read('xl/_rels/workbook.xml.rels'))
            target = next(node.get('Target') for node in rels if node.get('Id') == rel_id)
            path = target.lstrip('/') if target.startswith('/') else 'xl/' + target
            sheet = ET.fromstring(archive.read(path))
            data = []
            for row in sheet.findall(f'{{{NS}}}sheetData/{{{NS}}}row'):
                values = {}
                for cell in row:
                    letter = re.sub(r'\d', '', cell.get('r', ''))
                    node = cell.find(f'{{{NS}}}v')
                    value = node.text or '' if node is not None else ''.join(cell.find(f'{{{NS}}}is').itertext()) if cell.find(f'{{{NS}}}is') is not None else ''
                    if cell.get('t') == 's':
                        value = strings[int(value)]
                    values[letter] = value
                data.append(values)
            if not data or 'Код_товару' not in data[0].values():
                raise ValueError('Потрібен XLSX-експорт товарів Prom із заголовками колонок.')
            header = data[0]
            records = []
            for values in data[1:]:
                row = {title: values.get(letter, '') for letter, title in header.items()}
                codes = {key(row.get('Код_товару')), key(row.get('Номер_пристрою_(MPN)'))} - {''}
                # Prom's SKU can be an internal counter; the part-code characteristic is also authoritative.
                for letter, title in header.items():
                    if title == 'Назва_Характеристики' and values.get(letter, '').strip().casefold() in {'код запчастини', 'код детали', 'артикул'}:
                        letters = list(header)
                        index = letters.index(letter)
                        if index + 2 < len(letters):
                            codes.add(key(values.get(letters[index + 2])))
                if not codes:
                    continue
                records.append({'codes': sorted(codes - {''}), 'sku': row.get('Код_товару', ''),
                    'brand': row.get('Виробник', ''), 'remote_id': row.get('Унікальний_ідентифікатор', ''),
                    'external_id': row.get('Ідентифікатор_товару', ''),
                    'name_uk': row.get('Назва_позиції_укр', ''), 'name_ru': row.get('Назва_позиції', ''),
                    'category_id': row.get('Ідентифікатор_підрозділу', ''),
                    'category_url': row.get('Посилання_підрозділу', ''),
                    'photos': [url.strip() for url in row.get('Посилання_зображення', '').split(',') if url.strip().startswith(('https://', 'http://'))][:10]})
            if not records:
                raise ValueError('У файлі не знайдено товарів Prom.')
            return {'source': filename, 'records': records}
    except (zipfile.BadZipFile, KeyError, ET.ParseError, IndexError, StopIteration, AttributeError):
        raise ValueError('Не вдалося прочитати XLSX-експорт Prom.') from None


def russian_variant(value):
    reverse = {uk: ru for ru, uk in UKRAINIAN_TERMS.items() if uk != ru}
    pattern = re.compile(r'\b(?:' + '|'.join(re.escape(term) for term in sorted(reverse, key=len, reverse=True)) + r')\b', re.I)
    return pattern.sub(lambda match: reverse[match.group().casefold()], value)


def title(name, code, brand, used, language):
    text = ukrainian_variant(clean_text(name)) if language == 'uk' else russian_variant(clean_text(name))
    if code.casefold() not in text.casefold():
        text = f'{text} {code}'
    if brand and brand.casefold() not in text.casefold():
        text += ' ' + brand
    suffix = (' Б/В' if language == 'uk' else ' Б/У') if used else (' Новий' if language == 'uk' else ' Новый')
    limit = 130 if language == 'uk' else 110
    # Retain the identifying code even for long source names.
    if len(text + suffix) > limit:
        text = clean_text(name)[:max(10, limit - len(code) - len(suffix) - 1)].rstrip() + ' ' + code
    return text + suffix


def search_queries(name, code, brand, crosses, used, language):
    subject = ukrainian_variant(clean_text(name)) if language == 'uk' else russian_variant(clean_text(name))
    buy = 'купити' if language == 'uk' else 'купить'
    part = 'автозапчастина' if language == 'uk' else 'автозапчасть'
    state = ('б/в' if language == 'uk' else 'б/у') if used else ('новий' if language == 'uk' else 'новый')
    candidates = [code, f'{brand} {code}', f'{subject} {code}', f'{subject} {brand}',
                  f'{subject} {state}', f'{buy} {code}', f'{buy} {subject}', f'{part} {code}']
    candidates += [f'{subject} {cross}' for cross in crosses[:4]]
    result, seen = [], set()
    for query in candidates:
        query = clean_text(query)
        if any(len(word) > 50 for word in query.split()) or query.casefold() in seen:
            continue
        if len(', '.join(result + [query])) > 1024:
            continue
        result.append(query)
        seen.add(query.casefold())
    return ', '.join(result)


def description(name, code, brand, crosses, used, language, illustration=False):
    uk = language == 'uk'
    label = ['Артикул', 'Виробник', 'Стан', 'Додаткові номери'] if uk else ['Артикул', 'Производитель', 'Состояние', 'Дополнительные номера']
    state = ('Б/В' if uk else 'Б/У') if used else ('Новий' if uk else 'Новый')
    details = list(zip(label[:3], [code, brand, state]))
    if crosses:
        details.append((label[3], ', '.join(crosses)[:1000]))
    html = '<p><strong>' + escape(name) + '</strong></p><ul>'
    html += ''.join(f'<li>{key}: {escape(value)}</li>' for key, value in details if value)
    html += '</ul><p>' + ('Перед замовленням звірте артикул і сумісність за VIN у продавця.' if uk else 'Перед заказом сверьте артикул и совместимость по VIN у продавца.') + '</p>'
    if illustration:
        html += '<p>' + ('Ілюстративне фото з каталогу. Стан і комплектацію уточнюйте у продавця.' if uk else 'Иллюстративное фото из каталога. Состояние и комплектацию уточняйте у продавца.') + '</p>'
    return html


def _tokens(value):
    stop = {'для', 'новий', 'нова', 'стан', 'гарний', 'відмінний', 'деталь', 'запчастина', 'перевірений', 'права', 'лівий', 'ліва', 'правий', 'usa', 'vag', 'vw'}
    return {word[: -2] if len(word) >= 7 else word for word in re.findall(r'[а-яіїєґ]{3,}', ukrainian_variant(value).casefold()) if word not in stop}


def infer_category(name, references):
    words = _tokens(name)
    best = {}
    for ref in references:
        if not ref.get('category_id'):
            continue
        known = ref.get('_tokens') if '_tokens' in ref else _tokens(ref.get('name_uk') or ref.get('name_ru') or '')
        overlap = words & known
        if len(overlap) < 2:
            continue
        score = len(overlap) / max(len(words), 1)
        if score >= .75:
            category = ref['category_id']
            if score > best.get(category, (0, None))[0]:
                best[category] = (score, ref)
    ranked = sorted(best.values(), key=lambda item: item[0], reverse=True)
    if ranked and (len(ranked) == 1 or ranked[0][0] - ranked[1][0] >= .2):
        return ranked[0][1]
    return None


def prepare_products(records, references, links, rate, markup, category_overrides=None):
    """Validate and aggregate counted cards; reuse exact Prom IDs before creating new identities."""
    category_overrides = category_overrides or {}
    reference_index = {}
    references = [{**ref, '_tokens': _tokens(ref.get('name_uk') or ref.get('name_ru') or '')} for ref in references]
    for ref in references:
        for code in ref.get('codes', []):
            reference_index.setdefault(code, []).append(ref)
    groups, issues = {}, []
    for record in records:
        reason = ''
        code = str(record['code']).strip()
        if not re.fullmatch(r'[\w .\-/]{1,25}', code):
            reason = 'Артикул Prom має містити 1–25 символів: літери, цифри, пробіл, . - _ /.'
        try:
            price = Decimal(str(record['price']))
            total = (price * rate * (1 + markup / 100)).quantize(Decimal('.01'), rounding=ROUND_HALF_UP)
            if not price.is_finite() or price <= 0 or not Decimal('.01') <= total <= Decimal('9999999999'):
                reason = 'Вкажіть додатну ціну; кінцева ціна Prom має бути від 0.01 до 9999999999 грн.'
        except (InvalidOperation, ValueError):
            reason = 'Некоректна ціна товару.'
            total = Decimal(0)
        if not any(char.isalpha() for char in record['name']):
            reason = 'Додайте текстову назву товару.'
        if type(record['qty']) is not int or record['qty'] < 0:
            reason = 'Кількість має бути цілим невід’ємним числом.'
        if reason:
            issues.append({**record, 'error': reason})
            continue
        group = (key(record['brand']), key(code), bool(record['used']))
        groups.setdefault(group, []).append({**record, 'price_uah': total})
    products = []
    for identity, cards in groups.items():
        item = cards[0].copy()
        if len({card['price_uah'] for card in cards}) > 1:
            issues.extend({**card, 'error': 'Різні ціни карток того самого артикула й виробника.'} for card in cards)
            continue
        item['qty'] = sum(card['qty'] for card in cards)
        matching = reference_index.get(key(item['code']), [])
        branded = [ref for ref in matching if key(ref.get('brand')) == key(item['brand'])]
        if item['brand'] and matching:
            compatible = branded or [ref for ref in matching if not key(ref.get('brand'))]
            if not compatible:
                issues.extend({**card, 'error': 'Виробник не збігається з довідником Prom. Звірте картку та оновіть довідник.'} for card in cards)
                continue
            matching = compatible
        else:
            matching = branded or matching
        # Duplicate Prom listings are never chosen arbitrarily.
        remote_ids = {ref.get('remote_id') for ref in matching if ref.get('remote_id')}
        if len(remote_ids) > 1:
            issues.extend({**card, 'error': 'У довіднику Prom кілька карток цього артикула. Звірте ID у Prom.'} for card in cards)
            continue
        reference = matching[0] if matching else None
        item['brand'] = item['brand'] or (reference.get('brand', '') if reference else '')
        part_ids = {card.get('part_id') for card in cards} - {None}
        mapped = [link for link in links if link.get('part_id') in part_ids]
        if not mapped:
            candidate_codes = {key(item['code'])} | ({key(reference.get('sku'))} if reference else set())
            mapped = [link for link in links if key(link.get('sku')) in candidate_codes]
        if len({link['remote_id'] for link in mapped}) > 1:
            issues.extend({**card, 'error': 'Кілька відповідників Prom. Звірте прив’язку товару в «Маркетплейси».'} for card in cards)
            continue
        link = mapped[0] if mapped else reference
        item['remote_id'] = str(link.get('remote_id') or '') if link else ''
        item['external_id'] = (str(link.get('external_id') or '') if link else '') or ('revision-' + hashlib.sha256(key(item['code']).encode()).hexdigest()[:24])
        item['sku'] = str(link.get('sku') or item['code']) if link else item['code']
        category = reference if reference and reference.get('category_id') else infer_category(item['name'], references)
        item['category_id'] = str(category_overrides.get(str(item['id'])) or (category.get('category_id') if category else '') or '')
        item['category_url'] = category.get('category_url', '') if category and item['category_id'] == str(category.get('category_id')) else ''
        item['photos'] = list(dict.fromkeys(url for card in cards for url in card['photos']))[:10]
        item['illustration'] = False
        if not item['photos'] and reference and reference.get('photos'):
            item['photos'], item['illustration'] = reference['photos'][:10], True
        item['warnings'] = []
        if not item['photos']:
            item['warnings'].append('Немає фото: додайте власне фото у картку ревізії.')
        if not item['category_id'] and not item['remote_id']:
            item['warnings'].append('Категорію не підтверджено: задайте ID нижче або уточніть у Prom.')
        crosses = item.get('cross_numbers', [])
        item['name_uk'] = title(item['name'], item['code'], item['brand'], item['used'], 'uk')
        item['name_ru'] = title(item['name'], item['code'], item['brand'], item['used'], 'ru')
        item['queries_uk'] = search_queries(item['name'], item['code'], item['brand'], crosses, item['used'], 'uk')
        item['queries_ru'] = search_queries(item['name'], item['code'], item['brand'], crosses, item['used'], 'ru')
        item['description_uk'] = description(item['name_uk'], item['code'], item['brand'], crosses, item['used'], 'uk', item['illustration'])
        item['description_ru'] = description(item['name_ru'], item['code'], item['brand'], crosses, item['used'], 'ru', item['illustration'])
        products.append(item)
    ids = Counter((item['remote_id'] or item['external_id']) for item in products)
    ready = []
    for item in products:
        if ids[item['remote_id'] or item['external_id']] > 1:
            issues.append({**item, 'error': 'Один ID Prom відповідає різним товарам. Потрібна звірка.'})
        else:
            ready.append(item)
    return ready, issues


def product_row(item):
    return [item['sku'], item['name_uk'], item['name_ru'], item['queries_uk'], item['queries_ru'],
        item['description_uk'], item['description_ru'], 'r', item['price_uah'], 'UAH', 'шт.', 1,
        '+' if item['qty'] > 0 else '-', item['qty'], ', '.join(item['photos']), 'Ревізія', 'Ревізія',
        '900000000014', item['external_id'], item['remote_id'], item['brand'], item['category_id'],
        item['category_url'], item['name_uk'], item['name_ru'], clean_text(item['name_uk'])[:270],
        clean_text(item['name_ru'])[:250], item['code'], 'Код запчастини', '', item['code'],
        'Стан', '', 'Б/В' if item['used'] else 'Новий', 'Кросс-номери', '', ', '.join(item.get('cross_numbers', []))]


def _column(index):
    result = ''
    while index:
        index, remainder = divmod(index - 1, 26)
        result = chr(65 + remainder) + result
    return result


def workbook_bytes(products):
    """Small dependency-free OOXML writer used by the Flask server, with numeric price cells."""
    def sheet(headers, rows):
        root = ET.Element('worksheet', xmlns=NS)
        ET.SubElement(root, 'dimension', ref=f'A1:{_column(len(headers))}{len(rows) + 1}')
        views = ET.SubElement(root, 'sheetViews')
        ET.SubElement(ET.SubElement(views, 'sheetView', workbookViewId='0'), 'pane', ySplit='1', topLeftCell='A2', activePane='bottomLeft', state='frozen')
        cols = ET.SubElement(root, 'cols')
        for index in range(1, len(headers) + 1):
            ET.SubElement(cols, 'col', min=str(index), max=str(index), width=str(48 if index in (2, 3, 4, 5, 6, 7, 15) else 20), customWidth='1')
        data = ET.SubElement(root, 'sheetData')
        for index, values in enumerate([headers] + rows, 1):
            row = ET.SubElement(data, 'row', r=str(index))
            for number, value in enumerate(values, 1):
                attrs = {'r': f'{_column(number)}{index}'}
                if isinstance(value, (int, Decimal)) and not isinstance(value, bool):
                    cell = ET.SubElement(row, 'c', **attrs, t='n', s='1' if isinstance(value, Decimal) else '0')
                    ET.SubElement(cell, 'v').text = str(value)
                else:
                    cell = ET.SubElement(row, 'c', **attrs, t='inlineStr', s='2' if index == 1 else '0')
                    ET.SubElement(ET.SubElement(cell, 'is'), 't').text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', str(value or ''))
        ET.SubElement(root, 'autoFilter', ref=f'A1:{_column(len(headers))}{len(rows) + 1}')
        return ET.tostring(root, encoding='utf-8', xml_declaration=True)
    output = io.BytesIO()
    with zipfile.ZipFile(output, 'w', zipfile.ZIP_DEFLATED) as archive:
        archive.writestr('[Content_Types].xml', '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>' + ''.join(f'<Override PartName="/xl/worksheets/sheet{i}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>' for i in (1, 2)) + '</Types>')
        archive.writestr('_rels/.rels', '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>')
        archive.writestr('xl/workbook.xml', f'<workbook xmlns="{NS}" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Export Products Sheet" sheetId="1" r:id="rId1"/><sheet name="Export Groups Sheet" sheetId="2" r:id="rId2"/></sheets></workbook>')
        archive.writestr('xl/_rels/workbook.xml.rels', '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">' + ''.join(f'<Relationship Id="rId{i}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{i}.xml"/>' for i in (1, 2)) + '<Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/></Relationships>')
        archive.writestr('xl/styles.xml', f'<styleSheet xmlns="{NS}"><fonts count="2"><font><sz val="11"/><name val="Calibri"/></font><font><b/><color rgb="FFFFFFFF"/><sz val="11"/><name val="Calibri"/></font></fonts><fills count="3"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill><fill><patternFill patternType="solid"><fgColor rgb="FF123044"/><bgColor indexed="64"/></patternFill></fill></fills><borders count="1"><border/></borders><cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs><cellXfs count="3"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/><xf numFmtId="2" fontId="0" fillId="0" borderId="0" xfId="0" applyNumberFormat="1"/><xf numFmtId="0" fontId="1" fillId="2" borderId="0" xfId="0" applyFill="1"/></cellXfs><cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles></styleSheet>')
        archive.writestr('xl/worksheets/sheet1.xml', sheet(HEADERS, [product_row(item) for item in products]))
        archive.writestr('xl/worksheets/sheet2.xml', sheet(GROUP_HEADERS, [['', 'Ревізія', 'Ревізія', '900000000014']]))
    return output.getvalue()


def download_bytes(products, chunk_size):
    if len(products) <= chunk_size:
        return workbook_bytes(products), 'prom-revision.xlsx', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    output = io.BytesIO()
    with zipfile.ZipFile(output, 'w', zipfile.ZIP_DEFLATED) as archive:
        for index, start in enumerate(range(0, len(products), chunk_size), 1):
            chunk = products[start:start + chunk_size]
            archive.writestr(f'prom-revision-{index:02}-{len(chunk)}.xlsx', workbook_bytes(chunk))
    return output.getvalue(), 'prom-revision.zip', 'application/zip'
