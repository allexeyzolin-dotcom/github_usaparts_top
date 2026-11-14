(() => {
  const button = document.getElementById('prom-check-photos');
  if (!button) return;
  const summary = document.getElementById('prom-photo-summary');
  function checkImage(url) {
    return new Promise(resolve => {
      const image = new Image();
      const timer = setTimeout(() => { image.onload = image.onerror = null; resolve(false); }, 12000);
      image.onload = () => { clearTimeout(timer); resolve(image.naturalWidth > 0); };
      image.onerror = () => { clearTimeout(timer); resolve(false); };
      image.referrerPolicy = 'no-referrer';
      image.src = url;
    });
  }
  button.addEventListener('click', async () => {
    button.disabled = true;
    document.getElementById('prom-product-details').open = true;
    const cards = [...document.querySelectorAll('[data-prom-photos]')];
    let cursor = 0, checked = 0, failed = 0;
    summary.textContent = 'Перевіряємо відкриття фото…';
    async function worker() {
      while (cursor < cards.length) {
        const card = cards[cursor++];
        const urls = JSON.parse(card.dataset.promPhotos);
        const status = card.querySelector('[data-photo-status]');
        if (!urls.length) { status.textContent = 'Немає фото. Додайте власні фото у картку ревізії та повторіть експорт.'; continue; }
        let broken = 0;
        for (const url of urls) { if (!(await checkImage(url))) broken++; checked++; }
        failed += broken;
        status.textContent = broken ? `Не відкрилися фото: ${broken} із ${urls.length}. Замініть посилання або завантажте власне фото у картці ревізії.` : `Відкриваються всі ${urls.length} фото.`;
        status.classList.toggle('revision-export-error', broken > 0);
      }
    }
    try {
      await Promise.all(Array.from({length: Math.min(4, cards.length)}, worker));
      summary.textContent = `Перевірено посилань: ${checked}. Не відкрилися: ${failed}. Це перевірка у вашому браузері; доступ із боку завантажувача Prom може відрізнятися.`;
    } finally { button.disabled = false; }
  });
})();
