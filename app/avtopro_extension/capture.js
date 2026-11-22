(() => {
  const origins = new Set(["avtopro.ua", "www.avtopro.ua", "avto.pro", "www.avto.pro"]);
  const orderId = /^\/orders\/(\d{3,20})\/?$/.exec(location.pathname)?.[1];
  if (location.protocol !== "https:" || !origins.has(location.hostname) || !orderId) {
    return { error: "Відкрийте окреме замовлення з розділу «Продаж» Автопро." };
  }

  const visible = (el) => el && el.getClientRects().length > 0 && getComputedStyle(el).visibility === "visible";
  const clean = (text) => (text || "").replace(/\s+/g, " ").trim();
  const read = (el) => visible(el) ? clean(el.innerText) : "";
  const heading = read(document.querySelector("main h1"));
  if (/^(?:Замовлення|Заказ)\s*№?\s*(\d+)$/i.exec(heading)?.[1] !== orderId) {
    return { error: "Картка ще завантажується або номер замовлення не збігається." };
  }

  const terms = [...document.querySelectorAll("main dt")].filter(visible);
  const valueFor = (label) => {
    const term = terms.find((el) => label.test(read(el)));
    const sibling = term?.nextElementSibling;
    return sibling?.tagName === "DD" && visible(sibling) ? sibling : null;
  };
  const buyerField = valueFor(/^(?:ПІБ|ФИО)\s+(?:покупця|покупателя)$/i);
  if (!buyerField) {
    return { error: "Не знайдено картку продавця з полем «ПІБ покупця»." };
  }
  const money = (text) => {
    const found = [...text.matchAll(/(\d[\d \u00a0\u202f]*(?:[.,]\d{1,2})?)\s*(?:грн|₴|UAH)/gi)];
    return found.length === 1 ? found[0][1].replace(/\s/g, "").replace(",", ".") : "";
  };
  const items = [...document.querySelectorAll("main .item")].filter((row) => visible(row.querySelector(".product .brand-and-code")))
    .map((row) => {
      const brandCode = row.querySelector(".product .brand-and-code");
      const brand = read(brandCode.querySelector(".brand"));
      const code = read(brandCode);
      const partNumber = brand && code.startsWith(brand) ? code.slice(brand.length).trim() : code;
      const name = read(row.querySelector(".product__info-name")) || read(row.querySelector(".product__info-description"));
      const qty = Number(/^([1-9]\d*)\s*(?:шт|од)\.?$/i.exec(read(row.querySelector(".info__quantity")))?.[1] || 0);
      const unit = money(read(row.querySelector(".total-sum-per-one")));
      const sum = money(read(row.querySelector(".info__sum .total-sum")));
      // A line amount is a unit price only when exactly one item was ordered.
      return { partNumber, brand, name, qty, unitUah: unit || (qty === 1 ? sum : "") };
    });
  if (!items.length || items.some((item) => !item.partNumber || !item.name || !item.qty || !item.unitUah)) {
    return { error: "У картці не вдалося однозначно зчитати артикул, кількість і ціну всіх позицій." };
  }

  const totalField = valueFor(/^(?:Сума замовлення покупця|Сумма заказа покупателя)$/i);
  const totalUah = money(read(totalField?.querySelector(".nowrap")) || read(totalField));
  const status = read(document.querySelector("main .order-progressbar-status"));
  if (!totalUah || !status) return { error: "Не знайдено статус або підсумкову суму у гривнях." };

  const buyer = read(buyerField.querySelector(".wrapper-name > div > span:first-child"));
  const visiblePhone = read(valueFor(/^Телефон$/i));
  const phone = /^\+?[0-9 ()-]{9,24}$/.test(visiblePhone) ? visiblePhone : "";
  return { orderId, sourceUrl: `${location.origin}/orders/${orderId}/`, status, currency: "UAH", totalUah,
    customer: buyer, phone,
    delivery: read(valueFor(/^(?:Умови доставки|Условия доставки)$/i)?.querySelector(".text-delivery")),
    payment: read(valueFor(/^(?:Спосіб оплати|Способ оплаты)$/i)), items };
})();
