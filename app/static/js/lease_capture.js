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

  const scEditor = document.getElementById('sc-editor');
  if (scEditor) {
    const mode = document.getElementById('sc-mode');
    const list = document.getElementById('sc-clauses');
    const count = document.getElementById('sc-count');
    const entry = document.getElementById('sc-entry');
    const another = document.getElementById('sc-another');
    const readOnly = scEditor.dataset.frozen === '1';
    function scMode() {
      entry.hidden = mode.value !== 'add';
      document.getElementById('sc-none-note').hidden = mode.value !== 'none';
      document.getElementById('sc-none').setAttribute('aria-pressed',String(mode.value === 'none'));
      document.getElementById('sc-add').setAttribute('aria-pressed',String(mode.value === 'add'));
    }
    function numberClauses() {
      Array.from(list.children).forEach(function (clause,index) {
        const number = index + 1;
        clause.querySelector('legend').textContent = 'SC-' + String(number).padStart(2,'0');
        clause.querySelector('[data-sc-kind]').name = 'sc_heading_type_' + number;
        clause.querySelector('[data-sc-custom]').name = 'sc_custom_heading_' + number;
        clause.querySelector('[data-sc-text]').name = 'sc_text_' + number;
        clause.querySelector('[data-sc-custom-wrap]').hidden = clause.querySelector('[data-sc-kind]').value !== 'Custom';
      });
      count.value = String(list.children.length);
      another.disabled = readOnly || list.children.length >= 50;
    }
    function appendClause() {
      if (readOnly || list.children.length >= 50) return;
      list.appendChild(document.getElementById('sc-template').content.cloneNode(true));
      numberClauses();
      list.lastElementChild.querySelector('[data-sc-text]').focus();
    }
    document.getElementById('sc-none').addEventListener('click',function () {
      mode.value = 'none';scMode();
    });
    document.getElementById('sc-add').addEventListener('click',function () {
      mode.value = 'add';scMode();if (!list.children.length) appendClause();
    });
    another.addEventListener('click',appendClause);
    list.addEventListener('change',function (event) {
      if (event.target.matches('[data-sc-kind]')) {
        const clause = event.target.closest('.sc-clause');
        clause.querySelector('[data-sc-custom-wrap]').hidden = event.target.value !== 'Custom';
      }
    });
    list.addEventListener('click',function (event) {
      const remove = event.target.closest('[data-sc-remove]');
      if (!remove || readOnly) return;
      remove.closest('.sc-clause').remove();numberClauses();
      if (!list.children.length) { mode.value = 'none';scMode(); }
    });
    numberClauses();scMode();
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
  let operation = '';
  let newGroup = null;
  let selectedGroup = null;
  const choice = document.getElementById('party-tenant-choice');
  function show(operationName) {
    document.getElementById('party-initial-message').hidden = true;
    document.getElementById('party-lessors').hidden = operationName !== 'lessors';
    document.getElementById('party-tenant-selection').hidden = operationName !== 'select';
    groups.hidden = operationName !== 'add';
    Array.from(groups.children).forEach(group => { group.hidden = true; });
    operation = operationName;
  }
  function hasParticulars(group) {
    return Array.from(group.querySelectorAll('textarea')).some(input => input.value.trim() !== '');
  }
  function availableGroup() {
    const empty = Array.from(groups.children).find(group => !hasParticulars(group) && !group.querySelector('.register-choice')?.value);
    if (empty) return empty;
    if (groups.children.length >= 50) return null;
    const index = groups.children.length + 1;
    const holder = document.createElement('template');
    holder.innerHTML = prototype.innerHTML.replaceAll('LESSEE_1_', 'LESSEE_' + index + '_').replaceAll('Lessee 1', 'Lessee ' + index).replaceAll('tenant_register_id_1', 'tenant_register_id_' + index);
    const group = holder.content.firstElementChild;
    group.dataset.lesseeIndex = index;
    group.querySelector('legend').textContent = 'Lessee';
    groups.appendChild(group);
    count.value = String(groups.children.length);
    return group;
  }
  document.getElementById('show-lessors').addEventListener('click', () => show('lessors'));
  document.getElementById('select-tenant').addEventListener('click', function () {
    show('select');choice.focus();
  });
  document.getElementById('add-lessee').addEventListener('click', function () {
    if (!newGroup || (operation === 'add' && hasParticulars(newGroup))) newGroup = availableGroup();
    if (!newGroup) return;
    show('add');newGroup.hidden = false;
    const register = newGroup.querySelector('.register-choice');
    if (register) register.hidden = true;
    newGroup.querySelector('textarea')?.focus();
  });
  function showTenantDetails(tenant) {
    const host = document.getElementById('party-selected-details');
    host.replaceChildren();
    if (!tenant) return;
    const card = document.createElement('section');card.className = 'party-particulars';
    const heading = document.createElement('h3');heading.textContent = 'Selected tenant';card.appendChild(heading);
    const details = document.createElement('dl');
    const labels = {legal_name:'Name',party_type:'Party Type',entity_type:'Entity Type',identity_number:'Registration / ID Number',address:'Address',physical_address:'Physical Address',service_address:'Service Address',email:'Email',phone:'Phone',signatory_name:'Representative',signatory_identity_number:'Representative ID Number',signatory_capacity:'Representative Capacity (as recorded)',representative_email:'Representative Email',representative_phone:'Representative Phone'};
    Object.entries(labels).forEach(function (entry) {
      const value = tenant[entry[0]];
      if (value == null || String(value).trim() === '') return;
      const label = document.createElement('dt');label.textContent = entry[1];
      const content = document.createElement('dd');content.textContent = value;
      details.append(label,content);
    });
    card.appendChild(details);host.appendChild(card);
  }
  choice.addEventListener('change', function () {
    showTenantDetails(tenants.find(tenant => String(tenant.id) === choice.value));
    if (!choice.value) return;
    const existing = Array.from(groups.children).find(group => group.querySelector('.register-choice')?.value === choice.value);
    const target = existing || selectedGroup || availableGroup();
    if (!target) { document.getElementById('party-tenant-status').textContent = 'The lease already has 50 Lessees.';return; }
    if (!existing) selectedGroup = target;
    const register = target.querySelector('.register-choice');
    if (!register) return;
    register.value = choice.value;
    register.dispatchEvent(new Event('change', {bubbles:true}));
    document.getElementById('party-tenant-status').textContent = 'Tenant particulars selected. Save to keep them on this lease.';
  });
  if (form.dataset.hasErrors === '1') {
    newGroup = Array.from(groups.children).filter(hasParticulars).pop() || groups.firstElementChild;
    if (newGroup) {
      show('add');newGroup.hidden = false;
      const register = newGroup.querySelector('.register-choice');
      if (register) register.hidden = true;
    }
  }
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
