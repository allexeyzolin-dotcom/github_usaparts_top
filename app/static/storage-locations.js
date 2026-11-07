(() => {
  let counter = 0;
  function enhance(input) {
    if (input.dataset.locationReady) return;
    input.dataset.locationReady = '1';
    const list = document.createElement('datalist');
    list.id = `storage-location-options-${++counter}`;
    input.setAttribute('list', list.id);
    input.after(list);
    let timer, controller;
    input.addEventListener('input', () => {
      clearTimeout(timer); controller?.abort(); list.replaceChildren();
      const query = input.value.trim();
      if (query.length < 3) return;
      timer = setTimeout(async () => {
        controller = new AbortController();
        try {
          const response = await fetch('/admin/api/storage-locations?q=' + encodeURIComponent(query), { signal: controller.signal });
          if (!response.ok || response.redirected) return;
          const data = await response.json();
          if (input.value.trim() !== query) return;
          list.replaceChildren(...data.items.map(value => {
            const option = document.createElement('option'); option.value = value; return option;
          }));
        } catch (_) { /* Manual entry remains available when suggestions fail. */ }
      }, 250);
    });
  }
  document.querySelectorAll('[data-storage-location]').forEach(enhance);
  new MutationObserver(records => records.forEach(record => record.addedNodes.forEach(node => {
    if (!(node instanceof Element)) return;
    if (node.matches('[data-storage-location]')) enhance(node);
    node.querySelectorAll('[data-storage-location]').forEach(enhance);
  }))).observe(document.body, { childList: true, subtree: true });
})();
