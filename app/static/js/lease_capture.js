(function () {
  'use strict';
  const form = document.getElementById('certificate-form');
  if (!form) return;
  const bank = document.getElementById('bank_detail_id');
  if (bank) {
    const data = document.getElementById('capture-banks');
    const accounts = data ? JSON.parse(data.textContent) : [];
    const originalId = bank.value;
    const historical = Array.from(document.querySelectorAll('[data-bank-field]')).map(cell => [cell, cell.textContent]);
    bank.addEventListener('change', function () {
      const account = accounts.find(row => String(row.bank_detail_id) === bank.value);
      // A historical unavailable choice keeps its saved display and snapshot.
      if (bank.value && !account) {
        if (bank.value === originalId) {
          historical.forEach(entry => { entry[0].textContent = entry[1]; });
          document.getElementById('refresh-bank-snapshot').value = '0';
          document.getElementById('bank-selection-status').textContent = 'Saved historical account snapshot preserved.';
        }
        return;
      }
      document.getElementById('refresh-bank-snapshot').value = '1';
      document.querySelectorAll('[data-bank-field]').forEach(function (cell) {
        const key = cell.dataset.bankField;
        cell.textContent = account && account[key] != null && String(account[key]).trim() !== '' ? account[key] : '[' + key.toUpperCase() + ']';
      });
      document.getElementById('bank-selection-status').textContent = account ? 'Selected register particulars shown. Save to preserve them for this lease.' : 'No lease account selected. Save to clear the selection.';
    });
  }
  const count = document.getElementById('lessee-count');
  const groups = document.getElementById('capture-lessees');
  const prototype = document.getElementById('capture-lessee-template');
  const data = document.getElementById('capture-tenants');
  if (!count || !groups || !prototype) return;
  const tenants = data ? JSON.parse(data.textContent) : [];
  function resize() {
    const total = Number(count.value);
    if (!Number.isInteger(total) || total < 1 || total > 50) return;
    while (groups.children.length > total) groups.lastElementChild.remove();
    while (groups.children.length < total) {
      const index = groups.children.length + 1;
      const holder = document.createElement('template');
      holder.innerHTML = prototype.innerHTML.replaceAll('LESSEE_1_', 'LESSEE_' + index + '_').replaceAll('Lessee 1', 'Lessee ' + index).replaceAll('tenant_register_id_1', 'tenant_register_id_' + index);
      const group = holder.content.firstElementChild;
      group.dataset.lesseeIndex = index;
      group.querySelector('legend').textContent = 'Lessee ' + index;
      groups.appendChild(group);
    }
    document.getElementById('add-lessee').disabled = total >= 50;
  }
  count.addEventListener('change', resize);
  document.getElementById('add-lessee').addEventListener('click', function () {
    if (Number(count.value) >= 50) return;
    count.value = String(Number(count.value) + 1);
    resize();
    groups.lastElementChild.querySelector('select,textarea').focus();
  });
  form.addEventListener('change', function (event) {
    const select = event.target.closest('.register-choice');
    if (!select || select.value === '') return;
    const tenant = tenants.find(row => String(row.id) === select.value);
    if (!tenant) return;
    const prefix = select.dataset.field.replace(/LEGAL_NAME$/, '');
    const fields = {LEGAL_NAME:'legal_name',IDENTITY_NUMBER:'identity_number',SIGNATORY_NAME:'signatory_name',SIGNATORY_IDENTITY_NUMBER:'signatory_identity_number'};
    Object.keys(fields).forEach(function (suffix) {
      const input = document.getElementById('field_' + prefix + suffix);
      if (input) input.value = tenant[fields[suffix]] || '';
    });
  });
}());
