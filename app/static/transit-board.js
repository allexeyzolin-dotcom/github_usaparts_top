(() => {
  const root = document.querySelector('[data-transit-board]');
  if (!root) return;
  const cards = [...root.querySelectorAll('[data-board-card]')];
  const details = [...root.querySelectorAll('[data-board-detail]')];
  let filter = 'active', selected = '', query = '';
  const remember = () => { try { sessionStorage.setItem('transit-board-selection', selected); } catch (_) {} };
  try { selected = sessionStorage.getItem('transit-board-selection') || ''; } catch (_) {}
  function select(id) {
    selected = id;
    cards.forEach(card => card.setAttribute('aria-pressed', String(card.dataset.boardCard === id)));
    details.forEach(panel => panel.hidden = panel.dataset.boardDetail !== id);
    remember();
  }
  function render() {
    const visible = [];
    cards.forEach(card => {
      const archived = card.dataset.stage === 'done';
      card.hidden = !(filter === 'archive' ? archived : !archived && (filter !== 'attention' || card.dataset.attention === '1')) ||
        !card.dataset.search.toLocaleLowerCase('uk').includes(query);
      if (!card.hidden) visible.push(card);
    });
    root.querySelectorAll('[data-board-filter]').forEach(button => button.setAttribute('aria-pressed', String(button.dataset.boardFilter === filter)));
    for (const key of ['active', 'attention', 'archive']) root.querySelector('[data-board-count="' + key + '"]').textContent = cards.filter(card =>
      key === 'archive' ? card.dataset.stage === 'done' : card.dataset.stage !== 'done' && (key !== 'attention' || card.dataset.attention === '1')).length;
    root.querySelectorAll('[data-board-lane]').forEach(lane => {
      lane.hidden = (lane.dataset.boardLane === 'done') !== (filter === 'archive');
      const count = [...lane.querySelectorAll('[data-board-card]')].filter(card => !card.hidden).length;
      lane.querySelector('[data-lane-count]').textContent = count;
      lane.querySelector('[data-lane-empty]').hidden = count > 0;
    });
    root.querySelector('[data-board-empty]').hidden = visible.length > 0;
    select(visible.some(card => card.dataset.boardCard === selected) ? selected : visible[0]?.dataset.boardCard || '');
  }
  root.addEventListener('click', event => {
    const card = event.target.closest('[data-board-card]');
    if (card) select(card.dataset.boardCard);
    const button = event.target.closest('[data-board-filter]');
    if (button) {filter = button.dataset.boardFilter; render();}
  });
  root.querySelector('[data-board-search]').addEventListener('input', event => {query = event.target.value.trim().toLocaleLowerCase('uk');render();});
  root.addEventListener('change', event => {
    if (event.target.matches('[data-missing-only]')) event.target.closest('[data-board-detail]').querySelectorAll('[data-stock-complete]').forEach(row => {
      row.hidden = event.target.checked && row.dataset.stockComplete === '1';
    });
  });
  document.addEventListener('click', event => {
    const print = event.target.closest('[data-print-batch]');
    if (print) promptTransitBatchLabels(print.dataset.printBatch);
  });
  render();
})();

(() => {
  document.querySelectorAll('[data-transit-form]').forEach(form => {
    const code = form.querySelector('[data-role="part-number"]');
    const title = form.querySelector('[data-role="title"]');
    const price = form.querySelector('[data-role="price"]');
    const description = form.querySelector('[data-role="service-info"]');
    const photo = form.querySelector('[data-role="existing-photo-url"]');
    const status = form.querySelector('[data-role="status"]');
    const create = form.hasAttribute('data-create-transit');
    if (!code || !title || !price || !status) return;
    let timer, sequence = 0, previous = code.value.trim(), resolved = '', pending;
    const message = (text, tone = '') => {status.textContent = text;status.className = 'transit-lookup ' + tone;};
    async function lookup() {
      const value = code.value.trim().toUpperCase(), attempt = ++sequence;
      code.value = value;
      if (!value) {resolved = '';message('Введіть OEM — опис і ціна підставляться з картки товару.');return;}
      message('Перевіряємо картку товару…');
      try {
        const response = await fetch(form.dataset.prefillUrl + '?part_number=' + encodeURIComponent(value), {credentials:'same-origin'});
        if (!response.ok) throw new Error('lookup_failed');
        const data = await response.json();
        if (attempt !== sequence || code.value.trim().toUpperCase() !== value) return;
        resolved = value;
        if (!data.found) {price.min = '0.01';message('Новий товар · вкажіть опис і ціну. Після збереження буде створено картку товару.', 'is-new');return;}
        title.value = data.part.name || '';
        price.value = data.part.priceUsd ?? '';
        price.min = '0';
        if (description) description.value = data.part.description || '';
        if (photo) photo.value = data.part.photoUrl || '';
        message('Картку знайдено · опис і ціну підставлено.', 'is-found');
      } catch (_) {if (attempt === sequence) message('Не вдалося перевірити картку. Спробуйте ще раз.', 'is-error');}
    }
    code.addEventListener('input', () => {
      clearTimeout(timer);sequence++;resolved = '';
      const value = code.value.trim();
      if (previous && previous.toUpperCase() !== value.toUpperCase()) {title.value = '';price.value = '';if (photo) photo.value = '';if (description) description.value = '';}
      previous = value;
      timer = setTimeout(() => {pending = lookup();}, 280);
    });
    if (!create) return;
    form.addEventListener('submit', async event => {
      event.preventDefault();
      const add = event.submitter?.hasAttribute('data-draft-add');
      const hasItem = !!(code.value.trim() || title.value.trim());
      const hasDraft = !!form.querySelector('.transit-draft-line');
      if ((add || hasItem) && resolved !== code.value.trim().toUpperCase()) {clearTimeout(timer);await (pending = lookup());}
      if (add || hasItem || !hasDraft) {
        for (const field of [code, title, price, form.elements.qty]) {field.required = true;if (!field.reportValidity()) return;}
      } else {code.required = false;title.required = false;price.required = false;}
      if (!add) {
        for (const field of form.querySelectorAll('input,select,textarea')) {
          if (!field.disabled && !field.reportValidity()) return;
        }
      }
      if (add) form.action = event.submitter.formAction;
      HTMLFormElement.prototype.submit.call(form);
    });
  });
})();
