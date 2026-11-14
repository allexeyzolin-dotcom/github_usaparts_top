"""Private Prom export UI for counted inventory; never calls Prom's write API."""
import json
import secrets
from decimal import Decimal
from flask import g, jsonify, render_template, request, session, Response

from .prom_export import decimal_option, read_reference, prepare_products, download_bytes, key as article_key


REFERENCE_SETTING = 'revision_prom_export_reference'
CATEGORY_SETTING = 'revision_prom_export_categories'


def load_reference(site, db):
    row = db.query(site.ApiSetting).filter_by(setting_key=REFERENCE_SETTING).first()
    if row and row.setting_value:
        return json.loads(row.setting_value)
    path = site.BASE_DIR / 'prom_export_reference.json'
    return json.loads(path.read_text(encoding='utf-8')) if path.exists() else {'source': '', 'records': []}


def register_prom_revision_export(site, revision_access, valid_token, collect_records):
    @site.app.route('/api/mobile/inventory-revision/export/prom', methods=['GET', 'POST'])
    @site.app.route('/admin/revision/export/prom', methods=['GET', 'POST'])
    @revision_access
    def inventory_revision_export_prom():
        session.setdefault('revision_token', secrets.token_urlsafe(32))
        if request.method == 'POST' and not valid_token(allow_form=True):
            return jsonify(ok=False, error='Сесію оновлено. Перезавантажте сторінку.'), 403
        values = request.form if request.method == 'POST' and not request.is_json else request.get_json(silent=True) or request.args
        options = {'rate': str(values.get('rate', session.get('prom_export_rate', '46'))),
                   'markup': str(values.get('markup', session.get('prom_export_markup', '25'))),
                   'chunk_size': str(values.get('chunk_size', '640')),
                   'include_zero': (request.method == 'GET' and 'include_zero' not in values) or str(values.get('include_zero', '')).lower() in {'1', 'on', 'true'}}
        error = ''
        with site.SessionLocal() as db:
            reference = load_reference(site, db)
            try:
                rate = decimal_option(options['rate'], 'Курс USD', Decimal('.01'), Decimal('10000'))
                markup = decimal_option(options['markup'], 'Націнка', Decimal('0'), Decimal('500'))
                if not options['chunk_size'].isdigit() or not 1 <= int(options['chunk_size']) <= 1000:
                    raise ValueError('Кількість позицій у файлі має бути від 1 до 1000.')
                if request.method == 'POST' and values.get('action') == 'reference':
                    upload = request.files.get('reference')
                    if not upload or not upload.filename:
                        raise ValueError('Оберіть XLSX-експорт товарів із Prom.')
                    reference = read_reference(upload.read(20 * 1024 * 1024 + 1), upload.filename)
                    site.set_setting(db, REFERENCE_SETTING, json.dumps(reference, ensure_ascii=False))
                    db.commit()
                overrides = {}
                for field in values:
                    if field.startswith('category_') and values[field]:
                        category = str(values[field]).strip()
                        if not category.isdigit() or len(category) > 18 or int(category) <= 0:
                            raise ValueError('ID категорії має бути додатним цілим числом.')
                        overrides[field.removeprefix('category_')] = category
            except ValueError as exc:
                error = str(exc)
                rate, markup, overrides = Decimal('46'), Decimal('25'), {}
            records = collect_records(db, include_zero=options['include_zero'])
            category_setting = db.query(site.ApiSetting).filter_by(setting_key=CATEGORY_SETTING).first()
            saved_categories = json.loads(category_setting.setting_value) if category_setting and category_setting.setting_value else {}
            stored_overrides = {str(row['id']): saved_categories[article_key(row['code'])] for row in records
                if article_key(row['code']) in saved_categories}
            applied_overrides = {**stored_overrides, **overrides}
            model = site.marketplace_models.Link
            links = [{field: getattr(row, field) for field in ('part_id', 'remote_id', 'external_id', 'sku')}
                     for row in db.query(model)]
            ready, issues = prepare_products(records, reference['records'], links, rate, markup, applied_overrides)
            if request.method == 'POST' and values.get('action') == 'download' and not error:
                if ready and (not issues or values.get('only_ready') == '1'):
                    payload, filename, mimetype = download_bytes(ready, int(options['chunk_size']))
                    for row in ready:
                        if str(row['id']) in overrides:
                            saved_categories[article_key(row['code'])] = row['category_id']
                    if overrides:
                        site.set_setting(db, CATEGORY_SETTING, json.dumps(saved_categories, ensure_ascii=False))
                        db.commit()
                    session['prom_export_rate'], session['prom_export_markup'] = str(rate), str(markup)
                    return Response(payload, mimetype=mimetype, headers={
                        'Content-Disposition': f'attachment; filename="{filename}"', 'Cache-Control': 'no-store',
                        'X-Product-Count': str(len(ready)), 'X-Skipped-Card-Count': str(len(issues))})
                error = 'Виправте картки з помилками або явно оберіть експорт лише готових карток.' if issues else 'У ревізії немає товарів для експорту.'
            categories = {str(row['category_id']): row.get('category_url', '') for row in reference['records'] if row.get('category_id')}
            if getattr(g, 'native_revision_authorized', False):
                response = jsonify(ok=not error, error=error, ready_count=len(ready), issue_count=len(issues),
                    options=options, reference=reference['source'])
                if error:
                    response.status_code = 422
            else:
                response = site.app.make_response(render_template('revision_export_prom.html',
                    products=ready, issues=issues, options=options, error=error, reference=reference,
                    categories=sorted(categories.items()), revision_token=session['revision_token'],
                    existing_count=sum(bool(row['remote_id']) for row in ready)))
            response.headers['Cache-Control'] = 'no-store'
            response.headers['Referrer-Policy'] = 'no-referrer'
            return response
