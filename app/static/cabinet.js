(() => {
  document.querySelectorAll('.admin-nav .is-active, .admin-header-links .is-active').forEach(link => link.setAttribute('aria-current', 'page'));
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
    dialog.addEventListener('click', event => {
      const rect = dialog.getBoundingClientRect();
      if (event.target === dialog && (event.clientX < rect.left || event.clientX > rect.right || event.clientY < rect.top || event.clientY > rect.bottom)) dialog.close();
    });
  });
})();
