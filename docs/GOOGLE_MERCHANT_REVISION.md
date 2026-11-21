# Google Merchant: revision queue

Open `/admin/revision/google` (also linked from Revision and API settings).
The existing Merchant account, API data source and service-account credentials are
reused. Credentials stay on the server; a blank credential field preserves them.

Enable automatic delivery: default 20 cards every 300 seconds. The worker runs in
the existing application process, with a process lock and PostgreSQL advisory
lock. Queue state lives in `merchant_revision_jobs`, created with the existing
startup schema initialization. No destructive schema migration is needed.

## Delivery and recovery

- Only revision cards become new Google offers automatically.
- A source fingerprint includes the actual storefront price, attributes, images,
  destination and revision version. Each successful item is committed separately.
  Unchanged cards are refreshed after 14 days to prevent expiry.
- HTTP errors, timeouts and quota limits do not lose the position. Transient
  failures use exponential backoff and Retry-After. Invalid data is visible on the
  card, retried daily, or immediately with the retry control after corrections.
- A new destination has an independent queue. Existing offer IDs keep their
  previous normalization. Identical OEM/brand/condition cards use the exact
  preferred public card and selling price; other duplicates are marked skipped.
  Conflicting manufacturer identities or normalized-ID collisions are reported.
- Previously sent cards becoming unavailable are sent as OUT_OF_STOCK, including
  when photo or price was subsequently removed. A changed OEM first withdraws
  the previous offer. Removed cards retain enough queue data for withdrawal.
- The worker limits each pass to a configured slice and stops starting new items
  after two minutes; an in-flight image or API request finishes within its timeout.
- Insert success means accepted by the API, not approved by Google. A separate
  bounded poll reads processed-product destination statuses and item-level issues.
  Pending statuses are revisited after 15 minutes; other statuses after six hours.

“Зіставити існуючі товари Google” reads processed products 100 at a time. It only
matches the configured primary data source, language and feed label. Matched
existing catalog offers get negative queue revision IDs (minus the published part
ID) and are repaired after revision cards. It does not create new offers for the
rest of the unverified catalog. Warehouse labels remain unchanged for these
legacy offers. The cursor survives process restarts.

## Photos

The worker accepts existing uploads and HTTPS images on i.avto.pro. Arbitrary
origins, unsafe redirects, credentials in URLs, nonstandard ports, path traversal,
placeholders, corrupt files, images below 100 pixels and files above 16 MB are
rejected. Actual bytes are decoded before submission. Images above 64 MP are
rejected. EXIF orientation and transparency are normalized; no watermark, product
alteration or upscaling is added. The result is a real JPEG with a content hash in
its URL under the existing persistent `/uploads/merchant/` volume. Source cache
metadata expires after seven days. A valid secondary photo can replace a broken
primary photo; missing secondary photos are reported as warnings.

Images below 500×500 get a warning for the minimum effective 2027-01-31. Replacing
the source image changes the content hash, allowing a new Google image crawl.

## Export column mapping

The supplied 2026-11-21 TSV contains 3,134 rows and 22 columns. Snapshot audit:
33 missing prices, 26 placeholder images, 42 populated Google categories, 49
product types (27 contain an inappropriate numeric category ID). It contains no
approval/error status. Google diagnostics read separately showed 84 inaccessible
images, 43 missing prices and four vehicle-policy classifications; those counts
are account diagnostics, not counts calculated from the TSV.

| Export column | Merchant API field / treatment |
| --- | --- |
| назва | productAttributes.title, real name + OEM, 150 characters |
| ідентифікатор | offerId, retain existing format, detect collisions |
| ціна | price.amountMicros/currencyCode, exact storefront UAH selling price |
| стан | condition NEW/USED from the card |
| наявність | availability IN_STOCK/OUT_OF_STOCK |
| канал | Online, defined by the API data source |
| мітка фіду | feedLabel, existing API setting (UA) |
| мова | contentLanguage, existing API setting (uk) |
| mpn | mpn, manufacturer number from the card |
| update type | Service/export result; use full productInputs:insert |
| бренд | brand, real manufacturer, no OEM/store-name substitution |
| дата набуття чинності ціни зі знижкою | Omit without an actual sale price; none exists in revision |
| додаткове посилання на зображення | additionalImageLinks, up to ten validated images |
| доставка(країна) | shipping: UA; Nova Poshta 300 UAH, 0–1 handling + 1–3 transit business days, cutoff 12:00 Europe/Kyiv, matching the public policy |
| категорія продуктів google | googleProductCategory 899, Motor Vehicle Parts |
| має ідентифікатор | identifierExists true with real brand + MPN; no invented GTIN |
| опис | description, public card description, plain text, up to 5,000 characters |
| посилання | link, canonical public part URL |
| посилання на зображення | imageLink, validated HTTPS JPEG on own domain |
| спеціальна мітка 0 | customLabel0 verified availability; customLabel1 revision; legacy warehouse label preserved |
| тип продукту | productTypes, readable category hierarchy rather than category ID |
| усі кліки | Output analytics; never uploaded as a product attribute |

## Verification

Run `python -m unittest discover -s tests -p test_merchant_revision.py` against a
disposable SQLite database (`DATABASE_URL=sqlite://`). Tests mock all Google
requests and cover checkpointing, bounded batches, status separation, retry
timing, inventory withdrawal, edits during delivery, destination isolation,
scoped existing-offer reconciliation, image validation, authentication and CSRF.

References: [Merchant product ingestion](https://developers.google.com/merchant/api/guides/products/add-manage),
[processed product statuses](https://developers.google.com/merchant/api/reference/rest/products_v1/accounts.products),
[product attributes](https://developers.google.com/merchant/api/reference/rest/products_v1/ProductAttributes),
[image specification](https://support.google.com/merchants/answer/6324350?hl=en),
[2026 specification changes](https://support.google.com/merchants/answer/16989427?hl=en).
