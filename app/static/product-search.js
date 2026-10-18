(() => {
  'use strict';
  document.querySelectorAll('[data-product-seo]').forEach(panel => {
    const title = panel.querySelector('[data-seo-title]');
    const description = panel.querySelector('[data-seo-description]');
    const queries = panel.querySelector('[data-seo-queries]');
    const status = panel.querySelector('[data-seo-status]');
    const buttons = [...panel.querySelectorAll('[data-seo-action]')];
    let loaded = false, busy = false, generated = {}, saved = {};
    function renderPreview() {
      panel.querySelector('[data-seo-preview-title]').textContent = title.value.trim() || generated.title || '';
      panel.querySelector('[data-seo-preview-description]').textContent = description.value.trim() || generated.description || '';
      panel.querySelector('[data-seo-title-count]').textContent = `${title.value.length} / 100`;
      panel.querySelector('[data-seo-description-count]').textContent = `${description.value.length} / 160`;
    }
    function render(data) {
      generated = data.generated;
      saved = data.custom;
      title.value = saved.title || '';
      description.value = saved.description || '';
      queries.value = (saved.queries || []).join('\n');
      queries.placeholder = generated.queries.join('\n');
      title.placeholder = generated.title;
      description.placeholder = generated.description;
      panel.querySelector('[data-seo-mode]').textContent = saved.title || saved.description || saved.queries.length ? 'Налаштовано' : 'Автоматично';
      // Keep the preview readable even on local preview hosts.
      panel.querySelector('[data-seo-url]').textContent = data.url ? data.url.replace(/^https?:\/\//, '') : 'usaparts.top · Товар ще не додано на склад';
      renderPreview();
    }
    async function request(action) {
      if (busy) return;
      busy = true;
      buttons.forEach(button => button.disabled = true);
      status.textContent = 'Завантаження…';
      try {
        let response;
        if (!action) {
          response = await fetch(`${panel.dataset.endpoint}?${new URLSearchParams({part_number: panel.dataset.partNumber})}`);
        } else {
          response = await fetch(panel.dataset.endpoint, {method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({part_number: panel.dataset.partNumber, token: panel.dataset.token, action,
              title: title.value, description: description.value, queries: queries.value})});
        }
        if (response.redirected) throw new Error('Сесія завершилась. Перезавантажте сторінку.');
        const data = await response.json();
        if (!response.ok || !data.ok) throw new Error(data.error || 'Не вдалося зберегти SEO.');
        if (action === 'generate') {
          // Regenerate phrases from saved product data; leave title/description in auto mode.
          generated = data.generated;
          queries.value = generated.queries.join('\n');
          renderPreview();
          status.textContent = 'Фрази сформовано. Натисніть «Зберегти SEO», щоб застосувати.';
        } else {
          render(data);
          status.textContent = action === 'save' ? 'SEO збережено. Сторінка товару вже оновлена.' :
            action === 'reset' ? 'Автоматичне оновлення відновлено.' : 'Порожні поля працюють автоматично. Приклад видачі наведено вище.';
        }
        loaded = true;
        status.removeAttribute('data-error');
      } catch (error) {
        status.textContent = error.message || 'Помилка мережі. Спробуйте ще раз.';
        status.dataset.error = 'true';
      } finally {
        busy = false;
        buttons.forEach(button => button.disabled = !loaded);
      }
    }
    panel.addEventListener('toggle', () => { if (panel.open && !loaded) request(); });
    // SEO controls never trigger inventory autosave or submit the parent form.
    ['input', 'change'].forEach(type => panel.addEventListener(type, event => {
      event.stopPropagation();
      renderPreview();
      status.textContent = 'Є незбережені зміни SEO.';
    }));
    buttons.forEach(button => button.addEventListener('click', event => {
      event.preventDefault();
      request(button.dataset.seoAction);
    }));
    [title, description, queries].forEach(input => input.addEventListener('keydown', event => {
      if (event.key === 'Enter' && input.tagName === 'INPUT') { event.preventDefault(); event.stopPropagation(); }
    }));
  });
  document.querySelectorAll('[data-copy-part-number]').forEach(button => {
    button.addEventListener('click', async () => {
      const status = button.closest('.product-search-card').querySelector('[data-copy-status]');
      try {
        await navigator.clipboard.writeText(button.dataset.copyPartNumber);
        status.textContent = 'Артикул скопійовано';
      } catch (_) {
        status.textContent = `Артикул: ${button.dataset.copyPartNumber}. Виділіть і скопіюйте його.`;
      }
    });
  });
})();
