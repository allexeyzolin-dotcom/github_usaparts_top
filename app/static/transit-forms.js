
document.querySelectorAll('[data-delivery-fields]').forEach((block) => {
  const carrier = block.querySelector('[data-carrier-select]');
  const update = () => {
    const other = carrier.value === 'other';
    block.querySelector('[data-carrier-np]').classList.toggle('hidden', carrier.value !== 'nova_poshta');
    block.querySelector('[data-carrier-other]').classList.toggle('hidden', !other);
    block.querySelector('[name="ttn"]').disabled = carrier.value !== 'nova_poshta';
    block.querySelector('[name="carrier_name"]').disabled = !other;
    block.querySelector('[name="carrier_name"]').required = other;
    block.querySelector('[name="expected_date"]').required = other;
    block.querySelector('[data-date-required]').textContent = other ? '*' : '';
  };
  carrier.addEventListener('change', update);
  update();
});

function togglePanel(id) {
  const node = document.getElementById(id);
  if (node) node.classList.toggle('hidden');
}

function promptTransitLabels(orderId, defaultQty) {
  const answer = window.prompt('Кількість етикеток?', String(defaultQty || 0));
  if (answer === null) return;
  const qty = Number(answer);
  if (!Number.isFinite(qty) || qty <= 0) {
    window.alert('Вкажіть кількість більше нуля.');
    return;
  }
  const form = document.getElementById(`print-transit-form-${orderId}`);
  form.querySelector('input[name="label_qty"]').value = String(Math.floor(qty));
  form.submit();
}

function promptArchiveTransitLabels(orderId, defaultQty) {
  const answer = window.prompt('Кількість етикеток?', String(defaultQty || 0));
  if (answer === null) return;
  const qty = Number(answer);
  if (!Number.isFinite(qty) || qty <= 0) {
    window.alert('Вкажіть кількість більше нуля.');
    return;
  }
  const form = document.getElementById(`print-transit-archive-form-${orderId}`);
  form.querySelector('input[name="label_qty"]').value = String(Math.floor(qty));
  form.submit();
}

function confirmTransitDelete(form, partNumber) {
  const answer = window.prompt(`Щоб видалити ${partNumber}, введіть "так".`);
  if (answer === null) return false;
  form.querySelector('input[name="confirm_text"]').value = answer.trim();
  if (answer.trim().toLowerCase() !== 'так') {
    window.alert('Видалення скасовано.');
    return false;
  }
  return true;
}

function promptTransitBatchLabels(batchId) {
  const form = document.getElementById(`print-transit-batch-form-${batchId}`);
  if (!form) return;
  const answer = window.prompt('Кількість етикеток на кожну позицію? Залиште порожнім, щоб друкувати за кількістю товару.', '');
  if (answer === null) return;
  const trimmed = String(answer).trim();
  const qtyField = form.querySelector('input[name="label_qty"]');
  if (!trimmed) {
    qtyField.value = '';
    form.submit();
    return;
  }
  const qty = Number.parseInt(trimmed, 10);
  if (!Number.isFinite(qty) || qty <= 0) {
    window.alert('Вкажіть кількість більше нуля.');
    return;
  }
  qtyField.value = String(Math.floor(qty));
  form.submit();
}

function markTransitArrived(orderId, labelsPrinted, defaultQty) {
  const form = document.getElementById(`arrive-transit-form-${orderId}`);
  if (!form) return;
  const shouldPrint = labelsPrinted
    ? false
    : window.confirm('Штрихкоди ще не друкувались. Натисніть OK, щоб роздрукувати їх зараз, або Cancel, щоб просто перенести картку в архів.');
  if (shouldPrint) {
    const answer = window.prompt('Кількість етикеток?', String(defaultQty || 0));
    if (answer === null) return;
    const qty = Number(answer);
    if (!Number.isFinite(qty) || qty <= 0) {
      window.alert('Вкажіть кількість більше нуля.');
      return;
    }
    form.querySelector('input[name="label_qty"]').value = String(Math.floor(qty));
  }
  form.querySelector('input[name="print_labels"]').value = shouldPrint ? '1' : '0';
  form.submit();
}

