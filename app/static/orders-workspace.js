(() => {
  const feed = document.querySelector('[data-tracking-feed]');
  if (!feed) return;
  const cards = [...document.querySelectorAll('[data-tracking-order]')];
  const stage = document.querySelector('[data-order-stage]');
  const stageCount = document.querySelector('[data-stage-count]');
  const label = feed.querySelector('[data-tracking-feed-label]');
  const normalLabel = label.textContent;
  let fetching = false;
  function filterStage() {
    if (!stage) return;
    let shown = 0;
    cards.forEach(card => {
      const match = stage.value === 'all' || (stage.value === 'issue' ? card.dataset.orderIssue === '1' : card.dataset.orderStatus === stage.value);
      card.hidden = !match;
      if (match) shown++;
    });
    stageCount.textContent = `${shown} замовлень`;
  }
  stage?.addEventListener('change', filterStage);
  filterStage();
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
