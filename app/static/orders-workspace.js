(() => {
  const feed = document.querySelector('[data-tracking-feed]');
  if (!feed) return;
  const cards = [...document.querySelectorAll('[data-tracking-order]')];
  const stage = document.querySelector('[data-order-stage]');
  const stageCount = document.querySelector('[data-stage-count]');
  const label = feed.querySelector('[data-tracking-feed-label]');
  const normalLabel = label.textContent;
  const group = feed.dataset.orderGroup;
  const moved = document.querySelector('[data-prom-moved]');
  const archived = new Set();
  let fetching = false;
  const board = setupBoard();
  function setupBoard() {
    if (!cards.length) return null;
    const list = document.querySelector('.orders-main-list');
    if (!list) return null;
    const history = group === 'history';
    const stages = history ? [['collecting','Виконані'],['ready','Скасовані'],['shipping','Інші']] :
      [['collecting','Нові / Комплектування'],['ready','Готові до видачі'],['shipping','Відправлені']];
    const node = document.createElement('div');
    node.className = 'delivery-board customer-order-board';
    list.before(node);
    const lanes = new Map();
    for (const [key, title] of stages) {
      const lane = document.createElement('section'); lane.className = 'board-lane';
      const heading = document.createElement('h3'); heading.textContent = title;
      const count = document.createElement('span');count.className = 'board-count';heading.append(count);
      const items = document.createElement('div');items.className = 'board-lane-cards';
      const empty = document.createElement('p');empty.className = 'board-lane-empty';empty.textContent = 'Немає замовлень';
      lane.append(heading,items,empty);node.append(lane);lanes.set(key,{items,count,empty});
    }
    let selected = '';
    const buttons = new Map();
    const key = 'orders-board-' + group;
    try {selected = sessionStorage.getItem(key) || '';} catch (_) {}
    selected = feed.dataset.selectedOrder || selected;
    function select(id) {
      selected = id;
      cards.forEach(card => card.hidden = card.dataset.trackingOrder !== selected);
      buttons.forEach((button, card) => button.setAttribute('aria-pressed', String(card.dataset.trackingOrder === selected)));
      try {sessionStorage.setItem(key, selected);} catch (_) {}
    }
    cards.forEach(card => {
      const summary = card.querySelector('summary'), disclosure = card.querySelector('details'), body = card.querySelector('.order-body');
      const status = card.dataset.orderStatus;
      const lane = history ? (status === 'done' ? 'collecting' : status === 'cancelled' ? 'ready' : 'shipping') :
        status === 'shipped' ? 'shipping' : status === 'awaiting_shipment' ? 'ready' : 'collecting';
      card.dataset.boardLane = lane;
      const button = document.createElement('button');button.type = 'button';button.className = 'delivery-card';
      const title = document.createElement('strong');title.className = 'delivery-card-title';title.textContent = '#' + card.dataset.trackingOrder + ' · ' + card.dataset.customer;
      const badge = document.createElement('span');badge.className = 'board-status tone-' + (status === 'done' ? 'done' : status === 'cancelled' ? 'late' : lane === 'ready' ? 'arrived' : 'route');
      const icon = document.createElement('i');icon.dataset.lucide = lane === 'shipping' ? 'truck' : lane === 'ready' ? 'package-check' : 'package';
      icon.setAttribute('aria-hidden','true');
      badge.append(icon, document.createTextNode(summary.querySelector('.badge')?.textContent || status));
      const sourceTags = [...summary.querySelectorAll('[data-order-tag]')];
      const tags = document.createElement('span');tags.className = 'order-board-tags';
      tags.append(badge, ...sourceTags.map(tag => tag.cloneNode(true)));
      const meta = document.createElement('span');meta.className = 'delivery-card-meta';
      meta.textContent = (card.dataset.carrier === 'nova_poshta' ? 'Нова Пошта' : 'Самовивіз') + ' · ' + card.dataset.created.slice(0,10);
      const bottom = document.createElement('span');bottom.className = 'delivery-card-bottom';
      const qty = document.createElement('span');qty.textContent = '#' + card.dataset.trackingOrder + ' · ' + card.dataset.positionCount + ' поз.';
      const price = document.createElement('span');price.textContent = summary.querySelector('.order-overview-end strong')?.textContent || '';
      bottom.append(qty,price);button.append(title,tags,meta,bottom);
      button.addEventListener('click',()=>select(card.dataset.trackingOrder));
      lanes.get(lane).items.append(button);buttons.set(card,button);
      card.classList.add('board-selection');
      const left = document.createElement('div');left.className = 'board-detail-left';
      const heading = document.createElement('div');heading.className = 'board-detail-heading';
      const hgroup = document.createElement('div');
      const eyebrow = document.createElement('span');eyebrow.className = 'workspace-eyebrow';eyebrow.textContent = 'Замовлення #' + card.dataset.trackingOrder;
      const h = document.createElement('h3');h.textContent = card.dataset.customer;
      const detailTags = document.createElement('div');detailTags.className = 'order-board-tags';
      detailTags.append(...sourceTags.map(tag => tag.cloneNode(true)));
      hgroup.append(eyebrow,h,detailTags);heading.append(hgroup);
      left.append(heading,summary.querySelector('.order-overview-meta').cloneNode(true));
      const shipping = body.querySelector('.order-ttn-form');
      if (shipping) left.append(shipping);
      summary.hidden = true;disclosure.open = true;disclosure.classList.add('board-detail-right');
      body.style.paddingTop = '0';
      card.prepend(left);
    });
    list.classList.add('order-board-details');
    if (window.lucide) window.lucide.createIcons({attrs:{width:17,height:17,'stroke-width':1.7}});
    return {render(eligible) {
      buttons.forEach((button,card)=>button.hidden = !eligible.includes(card));
      lanes.forEach((lane,key)=>{
        const count = eligible.filter(card=>card.dataset.boardLane === key).length;
        lane.count.textContent = count;lane.empty.hidden = count > 0;
      });
      select(eligible.some(card=>card.dataset.trackingOrder === selected) ? selected : eligible[0]?.dataset.trackingOrder || '');
    }};
  }
  function filterStage() {
    const eligible = [];
    let shown = 0;
    cards.forEach(card => {
      const match = !archived.has(card.dataset.trackingOrder) && (!stage || stage.value === 'all' ||
        (stage.value === 'issue' ? card.dataset.orderIssue === '1' : card.dataset.orderStatus === stage.value));
      if (!board) card.hidden = !match;
      if (match) {shown++;eligible.push(card);}
    });
    if (stageCount) stageCount.textContent = `${shown} замовлень`;
    board?.render(eligible);
  }
  stage?.addEventListener('change', filterStage);
  filterStage();
  function archiveCard(id) {
    if (archived.has(id)) return;
    archived.add(id);
    const currentCount = document.querySelector(`[data-order-count="${group}"]`);
    const historyCount = document.querySelector('[data-order-count="history"]');
    if (currentCount) currentCount.textContent = Math.max(0, Number(currentCount.textContent) - 1);
    if (historyCount) historyCount.textContent = Number(historyCount.textContent) + 1;
    if (!moved) return;
    moved.hidden = false;
    moved.querySelector('span').textContent = archived.size === 1
      ? `Замовлення #${id} отримано й переміщено в історію.`
      : `Отримані замовлення Prom переміщено в історію (${archived.size}).`;
    const link = moved.querySelector('a');
    const url = new URL(link.href);
    if (archived.size === 1) url.searchParams.set('selected', id);
    else url.searchParams.delete('selected');
    link.href = url.toString();
  }
  document.querySelectorAll('[data-prom-match]').forEach(form => {
    const input = form.querySelector('[data-prom-number]');
    const find = form.querySelector('[data-prom-search]');
    const choices = form.querySelector('[data-prom-choices]');
    const select = choices.querySelector('select');
    const status = form.querySelector('[data-prom-match-status]');
    const save = form.querySelector('[data-prom-save]');
    let pending;
    function clearChoices() {
      choices.hidden = true;
      select.disabled = true;
      select.replaceChildren();
      save.disabled = true;
    }
    async function lookup() {
      const number = input.value.trim();
      pending?.abort();
      clearChoices();
      if (!number) {
        status.textContent = 'Введіть складський номер.';
        input.focus();
        return;
      }
      const controller = new AbortController();
      pending = controller;
      status.textContent = 'Шукаємо на складі…';
      try {
        const url = new URL(form.dataset.searchUrl, window.location.origin);
        url.searchParams.set('q', number);
        url.searchParams.set('exact', '1');
        const response = await fetch(url, {credentials:'same-origin', cache:'no-store', signal:controller.signal});
        if (!response.ok) throw new Error('lookup_failed');
        const {results} = await response.json();
        if (pending !== controller) return;
        if (!results.length) {
          status.textContent = 'Цього номера немає на складі. Перевірте номер і повторіть пошук.';
          return;
        }
        if (results.length > 1) select.add(new Option('Оберіть позицію та склад', ''));
        for (const result of results) select.add(new Option(result.label, result.id));
        choices.hidden = false;
        select.disabled = false;
        save.disabled = !select.value;
        status.textContent = '';
      } catch (error) {
        if (error.name !== 'AbortError' && pending === controller) {
          status.textContent = 'Пошук тимчасово недоступний. Спробуйте ще раз.';
        }
      }
    }
    input.addEventListener('input', () => {
      pending?.abort();
      pending = null;
      clearChoices();
      status.textContent = '';
    });
    input.addEventListener('keydown', event => {
      if (event.key === 'Enter') {
        event.preventDefault();
        lookup();
      }
    });
    find.addEventListener('click', lookup);
    select.addEventListener('change', () => {save.disabled = !select.value;});
  });
  async function refresh() {
    if (fetching || document.hidden || !cards.length) return;
    fetching = true;
    const abort = new AbortController();
    const timer = setTimeout(() => abort.abort(), 12000);
    try {
      // Read the server's cached checks; this endpoint never calls the carrier.
      for (let start = 0; start < cards.length; start += 100) {
        const url = new URL(feed.dataset.trackingFeed, window.location.origin);
        cards.slice(start, start + 100).forEach(card => url.searchParams.append('id', card.dataset.trackingOrder));
        const response = await fetch(url, {credentials:'same-origin', cache:'no-store', signal:abort.signal});
        if (!response.ok) throw new Error('status_unavailable');
        const payload = await response.json();
        for (const item of payload.orders || []) {
          const card = cards.find(el => el.dataset.trackingOrder === String(item.id));
          if (!card) continue;
          if (group !== 'history' && item.group === 'history' && item.promProcessed) {
            archiveCard(String(item.id));
            continue;
          }
          card.querySelectorAll('[data-ttn-status]').forEach(node => {
            node.textContent = item.status || 'Очікує перевірки';
            node.classList.toggle('is-received', item.received);
            node.classList.toggle('needs-attention', item.attention);
          });
          const checked = card.querySelector('[data-ttn-checked]');
          if (checked) checked.textContent = item.checkedAt ? `Перевірено: ${item.checkedAt} · Київ` : 'Перша перевірка — автоматично протягом 5 хвилин.';
          const error = card.querySelector('[data-ttn-error]');
          if (error) {error.textContent = item.error || ''; error.hidden = !item.error;}
          card.dataset.orderIssue = card.dataset.packingIssue === '1' || item.attention ? '1' : '0';
        }
      }
      label.textContent = normalLabel;
      filterStage();
    } catch (_) {
      label.textContent = 'Не вдалося отримати свіжі статуси. Повторимо автоматично через 30 секунд.';
    } finally {clearTimeout(timer);fetching = false;}
  }
  if (cards.length) {
    refresh();
    setInterval(refresh, 30000);
    document.addEventListener('visibilitychange', () => {if (!document.hidden) refresh();});
  }
})();
