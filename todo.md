# todo.md - Оптимізація сайту usaparts.top

## 1. SEO та індексація

- [x] Додати унікальні `<title>` та `<meta description>` для публічних сторінок: головна, каталог, товар, авто, SEO-розділи.
- [x] Використати дружні URL для товарів: `/part/<id>/<oem-translit-description>`.
- [x] Налаштувати `robots.txt` та `sitemap.xml` з окремими sitemap для сторінок, товарів, крос-номерів, категорій, авто і фото.
- [x] Додати `canonical` для уникнення дублювання.
- [x] Додати `hreflang` для української версії: `uk-UA`, `uk`, `x-default`.
- [x] Прибрати індексацію пошуку `/catalog?q=...`, щоб Google індексував канонічні картки товарів, а не службові результати пошуку.
- [x] Перенаправити дублікати головної `/?page=...` на `/catalog?page=...`.
- [ ] Перевірити індексацію через Google Search Console після деплою.

## 2. Schema.org Product (JSON-LD)

- [x] Додати блок `<script type="application/ld+json">` у `<head>` кожної картки товару через `base_public.html`.
- [x] Поля Product: `name`, `alternateName`, `url`, `productID`, `sku`, `mpn`, `identifier`, `brand`, `description`, `image`, `category`, `additionalProperty`.
- [x] Поля Offer: `url`, `priceCurrency`, `price`, `priceValidUntil`, `availability`, `itemCondition`, `inventoryLevel`, `seller`.
- [x] Додати `shippingDetails` та `hasMerchantReturnPolicy` в `offers`.
- [x] Додати `aggregateRating` і `review`, якщо в товару є схвалені відгуки.
- [x] Додати `BreadcrumbList` для картки товару.
- [x] Додати fallback `image` для товарів без фото, щоб Product structured data не падав через відсутнє поле `image`.
- [ ] Перевірити валідність через Rich Results Test після деплою.

## 3. Приклад поточної логіки JSON-LD

```html
<script type="application/ld+json">
{
  "@context": "https://schema.org",
  "@graph": [
    {
      "@type": "WebPage",
      "@id": "https://usaparts.top/part/10/06g127028h-nasos-toplivnyy-vysokogo-davleniya-tnvd#webpage"
    },
    {
      "@type": "BreadcrumbList"
    },
    {
      "@type": "Product",
      "name": "06G127028H - Насос топливный высокого давления",
      "sku": "06G127028H",
      "brand": {
        "@type": "Brand",
        "name": "VAG"
      },
      "description": "06G127028H Насос топливный высокого давления...",
      "offers": {
        "@type": "Offer",
        "url": "https://usaparts.top/part/10/06g127028h-nasos-toplivnyy-vysokogo-davleniya-tnvd",
        "priceCurrency": "UAH",
        "price": "1676.60",
        "availability": "https://schema.org/InStock",
        "shippingDetails": {
          "@type": "OfferShippingDetails"
        },
        "hasMerchantReturnPolicy": {
          "@type": "MerchantReturnPolicy"
        }
      }
    }
  ]
}
</script>
```

## 4. Оптимізація швидкодії

- [x] Впровадити lazy-load для списків товарів, авто, thumbnail та прихованих modal-зображень.
- [x] Увімкнути browser caching для `/static/*` та базове кешування `/uploads/*`.
- [ ] Конвертувати фото у WebP.
- [ ] Виконати CSS/JS minification.
- [ ] Перевірити Core Web Vitals через PageSpeed Insights після деплою.

## 5. Мобільна адаптивність

- [x] Базовий responsive design реалізований у CSS сайту.
- [x] Мобільну проблему верхньої шторки було виправлено раніше.
- [ ] Провести ручний прогін на смартфоні та планшеті після наступного деплою.

## 6. Контент карток товарів

- [x] OEM-код є першим сигналом у `title`, `description`, `H1`, картках і Product JSON-LD.
- [x] Вказується наявність на складі.
- [x] Вказується доставка/оплата в публічному футері та structured data.
- [x] Додано блок відгуків і `aggregateRating` / `review` для схвалених відгуків.
- [ ] Додати якісні фото мінімум 3-4 на ключові товари.
- [ ] Доповнювати описи характеристиками та застосуванням для пріоритетних OEM.

## 7. Безпека та довіра

- [x] HTTPS примусово використовується для `usaparts.top`.
- [x] Додано HSTS для основного домену.
- [x] Додано favicon та логотип.
- [x] Додано сторінкові блоки `Обмін та повернення` і `Оплата і доставка`.
- [ ] Додати окремі URL-сторінки Privacy Policy та Terms of Service, якщо потрібно для реклами/маркетплейсів.
