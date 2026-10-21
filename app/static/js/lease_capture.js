(function () {
  'use strict';
  const form = document.getElementById('certificate-form');
  if (!form) return;
  const ignored = new Set(['csrf_token','version','registration_token','capture_return','bank_operation']);
  function state() {
    return JSON.stringify(Array.from(form.querySelectorAll('input[name],select[name],textarea[name]'))
      .filter(input => !ignored.has(input.name) && !(['checkbox','radio'].includes(input.type) && !input.checked) && input.value !== '')
      .map(input => [input.name,input.value]));
  }
  const originalState = state();
  let submitting = false;
  const dirty = () => form.dataset.hasErrors === '1' || state() !== originalState;
  const back = document.querySelector('[data-capture-back]');
  const unsaved = document.getElementById('capture-unsaved-dialog');
  if (back) back.addEventListener('click', function (event) {
    if (!dirty()) return;
    event.preventDefault();unsaved.showModal();
  });
  document.getElementById('capture-save-back').addEventListener('click', function () {
    unsaved.close();
    if (!form.reportValidity()) return;
    document.getElementById('capture-return').value = 'back';
    form.requestSubmit(document.getElementById('capture-save'));
  });
  document.getElementById('capture-discard-back').addEventListener('click', function () {
    submitting = true;window.location.assign(back.href);
  });
  form.addEventListener('submit', function () { submitting = true; });
  window.addEventListener('beforeunload', function (event) {
    if (!submitting && dirty()) { event.preventDefault();event.returnValue = ''; }
  });
  document.querySelectorAll('[data-close-dialog]').forEach(button => button.addEventListener('click', function () {
    document.getElementById(button.dataset.closeDialog).close();
  }));
  const lessors = document.getElementById('show-lessors');
  if (lessors) lessors.addEventListener('click', () => document.getElementById('lessors-dialog').showModal());
  const selectTenant = document.getElementById('select-tenant');
  if (selectTenant) selectTenant.addEventListener('click', () => document.querySelector('#capture-lessees select').focus());
  document.querySelectorAll('[data-bank-operation]').forEach(button => button.addEventListener('click', function () {
    const operation = button.dataset.bankOperation;
    document.getElementById('bank-operation').value = operation;
    ['select','add'].forEach(key => {
      const panel = document.getElementById('bank-' + key + '-operation');
      panel.hidden = key !== operation;panel.disabled = key !== operation;
    });
  }));
  const commencement = document.getElementById('field_COMMENCEMENT_DATE');
  const duration = document.getElementById('field_DURATION_MONTHS');
  if (commencement && duration) {
    let timer, requestNumber = 0;
    function calculate() {
      clearTimeout(timer);
      const current = ++requestNumber;
      timer = setTimeout(async function () {
        const url = new URL(form.getAttribute('action'), window.location.href);
        url.searchParams.set('calculate','1');url.searchParams.set('COMMENCEMENT_DATE',commencement.value);
        url.searchParams.set('DURATION_MONTHS',duration.value);
        try {
          const response = await fetch(url,{credentials:'same-origin'});
          if (!response.ok) return;
          const data = await response.json();if (current !== requestNumber) return;
          ['expiry_date','renewal_notice_date'].forEach(key => {
            const output = document.getElementById('display_' + key.toUpperCase());
            if (output) output.textContent = data.dates[key] || '[' + key.toUpperCase() + ']';
          });
          const conflicts = document.getElementById('period-conflicts');
          conflicts.hidden = !data.differences.length;
          document.getElementById('period-differences').textContent = data.differences.map(row => row.label + ': saved ' + row.saved + '; calculated ' + row.calculated).join('\n');
          conflicts.querySelector('input').checked = false;
        } catch (error) { /* Save still performs authoritative calculation and validation. */ }
      },200);
    }
    commencement.addEventListener('input',calculate);duration.addEventListener('input',calculate);
  }

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
