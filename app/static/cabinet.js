(() => {
  document.querySelectorAll('.cabinet-primary .is-active, .admin-nav .is-active, .admin-header-links .is-active').forEach(link => link.setAttribute('aria-current', 'page'));
  if (window.lucide) window.lucide.createIcons({attrs:{width:17,height:17,'stroke-width':1.7}});
  document.addEventListener('click', event => {
    const opener = event.target.closest('[data-cabinet-open]');
    if (opener) {
      const dialog = document.getElementById(opener.dataset.cabinetOpen);
      if (dialog instanceof HTMLDialogElement && !dialog.open) dialog.showModal();
    }
    const closer = event.target.closest('[data-cabinet-close]');
    if (closer) closer.closest('dialog')?.close();
    const tools = document.querySelector('.cabinet-tools');
    if (tools?.open && !tools.contains(event.target)) tools.open = false;
  });
  document.querySelectorAll('dialog.cabinet-dialog').forEach(dialog => {
    dialog.addEventListener('close', () => {
      if (location.hash === '#' + dialog.id) history.replaceState(null, '', location.pathname + location.search);
    });
    dialog.addEventListener('click', event => {
      const rect = dialog.getBoundingClientRect();
      if (event.target === dialog && (event.clientX < rect.left || event.clientX > rect.right || event.clientY < rect.top || event.clientY > rect.bottom)) dialog.close();
    });
  });
  if (location.hash) {
    const dialog = document.getElementById(location.hash.slice(1));
    if (dialog instanceof HTMLDialogElement) dialog.showModal();
  }
  const ordersLink = document.querySelector('[data-new-orders-link]');
  const ordersCount = ordersLink?.querySelector('[data-new-orders-count]');
  if (ordersCount) {
    let refreshing = false;
    async function refreshNewOrders() {
      if (refreshing || document.hidden) return;
      refreshing = true;
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 12000);
      try {
        const response = await fetch(ordersLink.dataset.newOrdersUrl, {
          credentials:'same-origin', cache:'no-store', redirect:'error', signal:controller.signal,
        });
        if (!response.ok) return;
        const {count} = await response.json();
        if (!Number.isSafeInteger(count) || count < 0) return;
        ordersCount.textContent = String(count);
        ordersCount.hidden = count === 0;
        ordersLink.classList.toggle('has-new-orders', count > 0);
        ordersLink.setAttribute('aria-label', count ? `Замовлення. Нових: ${count}` : 'Замовлення');
        ordersLink.title = count ? `Нових замовлень: ${count}` : 'Замовлення';
      } catch (_) {
        // Keep the last known count until the next successful check.
      } finally {
        clearTimeout(timeout);
        refreshing = false;
      }
    }
    setInterval(refreshNewOrders, 30000);
    document.addEventListener('visibilitychange', () => {if (!document.hidden) refreshNewOrders();});
  }
})();
