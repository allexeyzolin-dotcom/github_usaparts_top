"""Locate inventory across warehouses and share the selected product cover."""
from sqlalchemy import or_


def locate(site, db, query):
    needle = query.strip().casefold()
    compact = site.compact_part_code(query)
    exact, barcodes, crosses = set(), set(), set()
    for number, barcode in db.query(site.Part.part_number, site.Part.barcode):
        if number.casefold() == needle or (compact and site.compact_part_code(number) == compact):
            exact.add(number)
        if barcode == query:
            barcodes.add(number)
    for row in db.query(site.PartTemplate.part_number, site.PartTemplate.barcode, site.PartTemplate.cross_numbers_json):
        if row.part_number.casefold() == needle or (compact and site.compact_part_code(row.part_number) == compact):
            exact.add(row.part_number)
        if row.barcode == query:
            barcodes.add(row.part_number)
        if compact and any(site.compact_part_code(code) == compact for code in site.template_cross_numbers(row)):
            crosses.add(row.part_number)
    numbers = exact or barcodes or crosses
    if not numbers:
        like = '%' + query.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_') + '%'
        numbers = set()
        for model in (site.Part, site.PartTemplate):
            fields = (model.part_number, model.name, model.description, model.brand, model.barcode, model.storage_location)
            numbers.update(number for number, in db.query(model.part_number).filter(or_(*[field.ilike(like, escape='\\') for field in fields])))
    stocked = (db.query(site.Part).filter(site.Part.part_number.in_(numbers), site.Part.is_deleted.is_(False),
                                        site.Part.qty > 0).all()) if numbers else []
    warehouse_ids = {row.warehouse_id for row in stocked}
    scope = str(next(iter(warehouse_ids))) if len(warehouse_ids) == 1 else 'all'
    focus = next(iter(numbers)) if len(numbers) == 1 else ''
    return scope, focus, numbers


def sync_photos(site, db, template, removed=()):
    """The cover is common; keep additional photos on individual warehouse rows."""
    gallery = site.template_gallery_urls(template)
    removed = set(removed)
    for part in db.query(site.Part).filter_by(part_number=template.part_number, is_deleted=False).all():
        extra = [url for url in site.part_gallery_urls(part) if url not in removed]
        part.showcase_photo_urls = site.dump_media_urls(gallery + extra)
        part.photo_urls = template.photo_urls
        part.has_photo = bool(site.part_gallery_urls(part))
        part.updated_at = site.now()
        if part.revision_publication:
            revision = db.get(site.RevisionPart, part.revision_publication.revision_id)
            if revision:
                other = [url for url in site.parse_media_urls(revision.photo_urls) if url not in removed]
                revision.photo_urls = site.dump_media_urls(gallery + other)
                revision.version = (revision.version or 0) + 1
