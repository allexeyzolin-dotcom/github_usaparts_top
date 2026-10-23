"""Reviewed catalog references for stock cards which have no seller photos.

Keep provenance with each asset. Never infer matches from a name or a cross-code
at runtime, and never persist these illustrations into warehouse/export photos.
"""

CATALOG_PHOTOS = {
    4238: {
        "part_number": "20K1082",
        "brand": "POLCAR",
        "image": "/static/catalog-photos/20k1082-171056992a.webp",
        "source_name": "SRLine / Partoso",
        "source_url": "https://www.ebay.de/itm/307147665930",
        "source_image": "https://i.ebayimg.com/images/g/NDMAAeSw0fJqkfbk/s-l1600.webp",
        "match": "20K108-2 / 20K1082; OEM 17118655753",
        "verified_on": "2026-10-23",
    },
    4240: {
        "part_number": "561823031G",
        "brand": "MARKET (OEM)",
        "image": "/static/catalog-photos/561823031g-d119bf5a6e.webp",
        "source_name": "CarParts.com",
        "source_url": "https://www.carparts.com/details/hood/replacement/rv13010011q",
        "source_image": "https://cld.partsimg.com/image/upload/v1/images/RV13010011Q_1",
        "match": "Replacement RV13010011Q; replaces OEM 561823031G",
        "verified_on": "2026-10-23",
    },
    3869: {
        "part_number": "04627892AB",
        "brand": "CHRYSLER",
        "image": "/static/catalog-photos/04627892ab-60775ddb66.webp",
        "source_name": "Чайка-AVTO / RIA.com",
        "source_url": "https://zapchasti.ria.com/toplyvnaya-reyka-jeep-cherokee-04627892ab-2329-5000-238195036.html",
        "source_image": "https://cdn.riastatic.com/photosnew/general/adv_photo/toplyvnaya-reyka-jeep-cherokee-04627892ab-2329-5000__37157219m.jpg",
        "match": "04627892AB; fuel rail without injectors",
        "verified_on": "2026-10-23",
    },
}
