(() => {
  const root = document.getElementById('inventory-revision');
  if (!root) return;
  const input = document.getElementById('revision-number');
  const results = document.getElementById('revision-results');
  const searchStatus = document.getElementById('revision-search-status');
  const editor = document.getElementById('revision-editor');
  const form = document.getElementById('revision-card-form');
  const saveButton = document.getElementById('revision-save');
  const saveStatus = document.getElementById('revision-save-status');
  const choice = document.getElementById('revision-choice');
  const pageCards = JSON.parse(document.getElementById('revision-page-cards').textContent);
  let controller, searchTimer, state, busy = false, choiceResolver;
  const textFields = ['part_number', 'name', 'brand', 'producer_type', 'price_usd', 'barcode',
    'warehouse_name', 'location', 'description', 'youtube_url', 'cross_numbers'];
  function requestId() {
    if (crypto.randomUUID) return crypto.randomUUID();
    // Local warehouse addresses may use HTTP; getRandomValues is also available there.
    const bytes = crypto.getRandomValues(new Uint8Array(16));
    bytes[6] = (bytes[6] & 15) | 64; bytes[8] = (bytes[8] & 63) | 128;
    const hex = Array.from(bytes, value => value.toString(16).padStart(2, '0')).join('');
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
  }

  function ask(title, message, actions, items = []) {
    document.getElementById('revision-choice-title').textContent = title;
    document.getElementById('revision-choice-text').textContent = message;
    const options = document.getElementById('revision-choice-items');
    const buttons = document.getElementById('revision-choice-actions');
    options.replaceChildren(); buttons.replaceChildren();
    items.forEach((item, index) => {
      const label = document.createElement('label');
      label.className = 'revision-choice-option';
      const radio = document.createElement('input');
      radio.type = 'radio'; radio.name = 'revision-target'; radio.value = item.id;
      radio.checked = index === 0;
      label.append(radio, document.createTextNode(`#${item.id} · ${item.part_number} · ${item.name} · ${item.qty} шт.${item.created_by ? ' · Додав: ' + item.created_by : ''}${item.warehouse_name ? ' · ' + item.warehouse_name : ''}${item.location ? ' / ' + item.location : ''}`));
      options.append(label);
    });
    [...actions, { value: 'cancel', label: 'Скасувати' }].forEach(action => {
      const button = document.createElement('button');
      button.type = 'button'; button.className = 'btn ' + (action.primary ? 'primary' : 'ghost');
      button.textContent = action.label;
      button.addEventListener('click', () => {
        const selected = options.querySelector('input:checked');
        choice.close();
        choiceResolver?.({ action: action.value, targetId: selected ? Number(selected.value) : null });
        choiceResolver = null;
      });
      buttons.append(button);
    });
    choice.showModal();
    return new Promise(resolve => { choiceResolver = resolve; });
  }
  choice.addEventListener('cancel', () => { choiceResolver?.({ action: 'cancel' }); choiceResolver = null; });

  function previewPhotos() {
    const target = document.getElementById('revision-photo-preview');
    target.replaceChildren();
    form.elements.photo_urls.value.split('\n').map(url => url.trim()).filter(Boolean).slice(0, 30).forEach(url => {
      if (!/^(https?:\/\/|\/uploads\/)/.test(url)) return;
      const image = document.createElement('img'); image.src = url; image.alt = 'Фото товару';
      target.append(image);
    });
  }
  form.elements.photo_urls.addEventListener('input', previewPhotos);

  function openEditor(data = {}, action = 'create', duplicateAction = '') {
    state = { action, target_id: action === 'create' ? null : data.id, version: data.version,
      request_id: requestId(), duplicate_action: duplicateAction };
    form.reset();
    textFields.forEach(field => { form.elements[field].value = data[field] ?? (field === 'producer_type' ? 'OEM' : field === 'price_usd' ? '0' : ''); });
    form.elements.part_number.value ||= input.value.trim().toUpperCase();
    form.elements.photo_urls.value = (data.photo_urls || []).join('\n');
    form.elements.qty.value = action === 'update' ? data.qty : 1;
    form.elements.qty.min = action === 'update' ? '0' : '1';
    const increment = action === 'increment';
    editor.dataset.mode = action;
    form.querySelectorAll('.revision-fields > label').forEach(label => { label.hidden = increment && !label.querySelector('[name="qty"]'); });
    form.querySelectorAll('input, textarea').forEach(element => { element.readOnly = increment && element.name !== 'qty'; });
    document.getElementById('revision-photo-files').disabled = increment;
    document.getElementById('revision-editor-title').textContent = increment ? 'Додати кількість' : action === 'update' ? 'Редагувати картку ревізії' : 'Нова картка ревізії';
    document.getElementById('revision-editor-help').textContent = increment
      ? `Картка #${data.id}: зараз ${data.qty} шт. Введіть, скільки додатково знайшли.`
      : action === 'update' ? 'Вкажіть актуальну загальну кількість товару в цій картці.'
      : 'Вкажіть кількість, яку фактично знайшли. Картку буде збережено в окремій базі ревізії.';
    saveButton.textContent = increment ? 'Додати кількість' : 'Зберегти перевірений товар';
    saveStatus.textContent = ''; previewPhotos(); editor.showModal();
    document.getElementById('revision-photo-preview').hidden = increment;
    (increment || action === 'update' || data.name ? form.elements.qty : form.elements.name).focus();
  }

  async function useCard(item) {
    if (item.source === 'revision') {
      const answer = await ask('Товар уже є в ревізії', `${item.part_number} · ${item.name}. У картці вже ${item.qty} шт.`, [
        { value: 'increment', label: 'Додати кількість', primary: true }, { value: 'new', label: 'Створити нову картку' }
      ]);
      if (answer.action === 'increment') openEditor(item, 'increment');
      if (answer.action === 'new') openEditor(item, 'create', 'new_card');
    } else {
      const answer = await ask('Використати дані картки?', `${item.part_number} · ${item.name}. Фактичну кількість ви вкажете окремо.`, [
        { value: 'copy', label: 'Використати дані', primary: true }, { value: 'blank', label: 'Створити нову' }
      ]);
      if (answer.action === 'copy') openEditor(item);
      if (answer.action === 'blank') openEditor({ part_number: item.part_number });
    }
  }

  function renderGroup(title, items) {
    if (!items.length) return;
    const heading = document.createElement('div'); heading.className = 'revision-result-heading'; heading.textContent = title; results.append(heading);
    items.forEach(item => {
      const button = document.createElement('button'); button.type = 'button'; button.className = 'revision-result';
      if (item.photo_urls.length) { const img = document.createElement('img'); img.src = item.photo_urls[0]; img.alt = ''; button.append(img); }
      const content = document.createElement('span'); content.className = 'revision-result-text';
      const code = document.createElement('strong'); code.textContent = item.part_number + ' · ' + item.name;
      const meta = document.createElement('small');
      meta.textContent = [item.brand, item.warehouse_name, item.location, item.source === 'revision' ? `${item.qty} шт. · Додав: ${item.created_by} · Перевірена наявність` : 'Основна база · ' + (item.source === 'template' ? 'картотека' : 'склад')].filter(Boolean).join(' · ');
      content.append(code, meta); button.append(content);
      button.addEventListener('click', () => useCard(item)); results.append(button);
    });
  }

  async function search() {
    const query = input.value.trim();
    if (query.length < 3) return;
    controller = new AbortController();
    searchStatus.textContent = 'Шукаємо картки…';
    try {
      const response = await fetch(root.dataset.searchUrl + '?q=' + encodeURIComponent(query), { signal: controller.signal });
      if (response.redirected) throw new Error('Увійдіть у кабінет та оновіть сторінку.');
      if (!response.ok) throw new Error('Не вдалося виконати пошук. Спробуйте ще раз.');
      const data = await response.json();
      if (input.value.trim() !== query) return;
      results.replaceChildren();
      renderGroup('Уже перевірені товари — база ревізії', data.revision);
      renderGroup('Картки з основної бази', data.catalog);
      results.hidden = !data.revision.length && !data.catalog.length;
      searchStatus.textContent = results.hidden ? 'Збігів немає. Натисніть «Створити картку», щоб додати товар вручну.' : 'Оберіть картку або створіть нову. Основна база використовується для підказок.';
    } catch (err) { if (err.name !== 'AbortError') searchStatus.textContent = err.message; }
  }
  input.addEventListener('input', () => {
    clearTimeout(searchTimer); controller?.abort(); results.hidden = true;
    if (input.value.trim().length < 3) { searchStatus.textContent = 'Введіть щонайменше 3 символи для пошуку.'; return; }
    searchTimer = setTimeout(search, 250);
  });
  input.addEventListener('keydown', event => {
    if (event.key === 'ArrowDown') { const first = results.querySelector('button'); if (!results.hidden && first) { event.preventDefault(); first.focus(); } }
    if (event.key === 'Escape') results.hidden = true;
  });
  document.getElementById('revision-search-form').addEventListener('submit', event => {
    event.preventDefault(); openEditor({ part_number: input.value.trim().toUpperCase() });
  });
  root.querySelectorAll('[data-revision-add]').forEach(button => button.addEventListener('click', () => openEditor(pageCards[Number(button.dataset.revisionAdd)], 'increment')));
  root.querySelectorAll('[data-revision-edit]').forEach(button => button.addEventListener('click', () => openEditor(pageCards[Number(button.dataset.revisionEdit)], 'update')));
  document.querySelectorAll('[data-close-editor]').forEach(button => button.addEventListener('click', () => { if (!busy) editor.close(); }));
  editor.addEventListener('cancel', event => { if (busy) event.preventDefault(); });

  document.getElementById('revision-photo-files').addEventListener('change', async event => {
    if (busy || !event.target.files.length) return;
    busy = true; saveButton.disabled = true; saveStatus.textContent = 'Завантажуємо фото…';
    try {
      const body = new FormData();
      Array.from(event.target.files).forEach(file => body.append('photos', file));
      const response = await fetch(root.dataset.uploadUrl, { method: 'POST', headers: { 'X-Revision-Token': root.dataset.token }, body });
      if (response.redirected) throw new Error('Увійдіть у кабінет та оновіть сторінку.');
      const data = await response.json();
      if (!response.ok || !data.ok) throw new Error(data.error || 'Не вдалося завантажити фото.');
      form.elements.photo_urls.value = [form.elements.photo_urls.value, ...data.urls].filter(Boolean).join('\n');
      previewPhotos(); saveStatus.textContent = 'Фото додано. Збережіть картку.';
    } catch (err) { saveStatus.textContent = err.message; }
    finally { busy = false; saveButton.disabled = false; event.target.value = ''; }
  });

  form.addEventListener('submit', async event => {
    event.preventDefault();
    if (busy) return;
    busy = true; saveButton.disabled = true; saveStatus.textContent = 'Зберігаємо…';
    const payload = { ...state, qty: form.elements.qty.value,
      photo_urls: form.elements.photo_urls.value.split('\n').map(value => value.trim()).filter(Boolean) };
    textFields.forEach(field => { payload[field] = form.elements[field].value; });
    try {
      for (;;) {
        const response = await fetch(root.dataset.saveUrl, { method: 'POST', headers: {
          'Content-Type': 'application/json', 'X-Revision-Token': root.dataset.token
        }, body: JSON.stringify(payload) });
        if (response.redirected) throw new Error('Увійдіть у кабінет та оновіть сторінку.');
        const data = await response.json();
        if (response.status === 409 && data.code === 'duplicate') {
          const updating = payload.action === 'update';
          const actions = updating ? [{ value: 'new', label: 'Зберегти з цим номером', primary: true }] : [
            { value: 'increment', label: 'Додати кількість', primary: true }, { value: 'new', label: 'Створити нову картку' }
          ];
          const answer = await ask('Товар уже є в ревізії', updating
            ? 'Цей номер уже має інша картка. Зберегти змінений номер окремо?'
            : 'Виберіть картку, до якої додати введену кількість, або створіть окрему картку.', actions, data.duplicates);
          if (answer.action === 'cancel') { saveStatus.textContent = 'Збереження скасовано. Дані форми збережені.'; return; }
          if (answer.action === 'increment') { payload.action = 'increment'; payload.target_id = answer.targetId; }
          else payload.duplicate_action = 'new_card';
          continue;
        }
        if (!response.ok || !data.ok) throw new Error(data.error || 'Не вдалося зберегти товар.');
        saveStatus.textContent = 'Збережено. Перевірена наявність.';
        window.location.assign(window.location.pathname + window.location.search);
        return;
      }
    } catch (err) { saveStatus.textContent = err.message || 'Не вдалося зберегти товар. Спробуйте ще раз.'; }
    finally { busy = false; saveButton.disabled = false; }
  });
})();
