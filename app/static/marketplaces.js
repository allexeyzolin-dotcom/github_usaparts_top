(() => {
  const root = document.querySelector('.mp');
  if (!root) return;
  document.querySelectorAll('[data-part-search]').forEach(input => {
    let timer, controller;
    const form = input.closest('form');
    const results = form.querySelector('.mp-lookup-results');
    const hidden = form.querySelector('[name=part_id]');
    const submit = form.querySelector('button');
    input.addEventListener('input', () => {
      hidden.value = ''; submit.disabled = true;
      clearTimeout(timer); if (controller) controller.abort();
      results.replaceChildren();
      if (input.value.trim().length < 2) return;
      timer = setTimeout(async () => {
        controller = new AbortController();
        try {
          const response = await fetch('/admin/marketplaces/parts?q=' + encodeURIComponent(input.value),
            {signal: controller.signal, headers: {'Accept':'application/json'}});
          if (!response.ok) throw new Error();
          const data = await response.json();
          results.replaceChildren();
          for (const part of data.results) {
            const button = document.createElement('button');
            button.type = 'button'; button.textContent = part.label;
            button.setAttribute('role', 'option');
            button.addEventListener('click', () => {
              input.value = part.label; hidden.value = part.id;
              submit.disabled = false; results.replaceChildren();
            });
            results.append(button);
          }
          if (!data.results.length) {
            const message = document.createElement('button');
            message.type = 'button'; message.disabled = true;
            message.textContent = 'Відповідників не знайдено'; results.append(message);
          }
        } catch (error) {
          if (error.name !== 'AbortError') input.placeholder = 'Не вдалося виконати пошук';
        }
      }, 250);
    });
  });
  if (root.dataset.busy === '1') {
    setTimeout(() => {
      if (!document.activeElement || !['INPUT','TEXTAREA','SELECT'].includes(document.activeElement.tagName)) {
        window.location.reload();
      }
    }, 7000);
  }
})();
