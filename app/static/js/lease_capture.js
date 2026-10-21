(function () {
  'use strict';
  const form = document.getElementById('certificate-form');
  if (!form) return;
  const ignored = new Set(['csrf_token','version','registration_token','capture_return','bank_operation','capture_return_section','lessor_count','lessee_count']);
  function state() {
    return JSON.stringify(Array.from(form.querySelectorAll('input[name],select[name],textarea[name]'))
      .filter(input => !input.closest('#bank-editor') && !ignored.has(input.name) && !(['checkbox','radio'].includes(input.type) && !input.checked) && input.value !== '')
      .map(input => [input.name,input.value]));
  }
  let originalState = state();
  let submitting = false;
  let bankEditorDirty = () => false;
  let scEntryDirty = () => false;
  const dirty = () => state() !== originalState || bankEditorDirty() || scEntryDirty();
  const back = document.querySelector('[data-capture-back]');
  const unsaved = document.getElementById('capture-unsaved-dialog');
  let destination = back;
  document.querySelectorAll('[data-capture-back],[data-capture-section]').forEach(link => link.addEventListener('click', function (event) {
    if (event.ctrlKey || event.metaKey || event.shiftKey || event.altKey || !dirty()) return;
    event.preventDefault();destination = link;unsaved.showModal();
  }));
  document.getElementById('capture-save-back').addEventListener('click', function () {
    unsaved.close();
    if (!form.reportValidity()) return;
    document.getElementById('capture-return').value = destination.dataset.captureSection ? 'section' : 'back';
    document.getElementById('capture-return-section').value = destination.dataset.captureSection || '';
    form.requestSubmit(document.getElementById('capture-save'));
  });
  document.getElementById('capture-discard-back').addEventListener('click', function () {
    submitting = true;window.location.assign(destination.href);
  });
  form.addEventListener('submit', function () { submitting = true; });
  window.addEventListener('beforeunload', function (event) {
    if (!submitting && dirty()) { event.preventDefault();event.returnValue = ''; }
  });
  document.querySelectorAll('[data-close-dialog]').forEach(button => button.addEventListener('click', function () {
    document.getElementById(button.dataset.closeDialog).close();
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
    const rows = document.getElementById('sc-table-rows');
    const entry = document.getElementById('sc-entry');
    const heading = document.getElementById('sc-heading');
    const custom = document.getElementById('sc-custom');
    const body = document.getElementById('sc-text');
    const status = document.getElementById('sc-entry-status');
    const add = document.getElementById('sc-add');
    const readOnly = scEditor.dataset.frozen === '1';
    const clauses = JSON.parse(document.getElementById('sc-saved-clauses').textContent);
    let editing = -1, entryInitial = '';
    const entryState = () => JSON.stringify([heading.value,custom.value,body.value]);
    scEntryDirty = () => !entry.hidden && entryState() !== entryInitial;
    function closeEntry() { entry.hidden = true;entry.disabled = true;status.textContent = ''; }
    function openEntry(index) {
      if (readOnly) return;
      editing = index;
      const clause = clauses[index];
      heading.value = clause ? clause.heading_type : '';
      custom.value = clause && clause.heading_type === 'Custom' ? clause.heading : '';
      body.value = clause ? clause.text : '';
      document.getElementById('sc-custom-wrap').hidden = heading.value !== 'Custom';
      document.getElementById('sc-entry-number').textContent = 'SC-' + String(index < 0 ? clauses.length + 1 : index + 1).padStart(2,'0');
      entry.hidden = false;entry.disabled = false;status.textContent = '';
      entryInitial = entryState();heading.focus();
    }
    function renderTable() {
      rows.replaceChildren();
      if (!clauses.length) {
        const cell = rows.insertRow().insertCell();cell.colSpan = 3;
        cell.textContent = 'No supplementary conditions recorded.';
      }
      clauses.forEach((clause,index) => {
        const row = rows.insertRow();
        row.insertCell().textContent = 'SC-' + String(index + 1).padStart(2,'0');
        row.insertCell().textContent = clause.heading_type === 'Custom' ? clause.heading : clause.heading_type;
        const button = document.createElement('button');button.type = 'button';button.textContent = 'Edit';button.disabled = readOnly;
        button.addEventListener('click',() => openEntry(index));row.insertCell().append(button);
      });
      add.disabled = readOnly || clauses.length >= 50;
    }
    function stageClauses() {
      list.replaceChildren();count.value = String(clauses.length);
      clauses.forEach((clause,index) => {
        Object.entries({sc_heading_type:clause.heading_type,sc_custom_heading:clause.heading_type === 'Custom' ? clause.heading : '',sc_text:clause.text}).forEach(([key,value]) => {
          const input = document.createElement('input');input.type = 'hidden';input.name = key + '_' + (index + 1);input.value = value;list.append(input);
        });
      });
    }
    add.addEventListener('click',() => openEntry(-1));
    heading.addEventListener('change',() => { document.getElementById('sc-custom-wrap').hidden = heading.value !== 'Custom'; });
    document.getElementById('sc-cancel').addEventListener('click',closeEntry);
    document.getElementById('sc-save-clause').addEventListener('click',function () {
      if (readOnly) return;
      if (!heading.value) {
        if (!clauses.length) mode.value = 'none';
        closeEntry();return;
      }
      const title = heading.value === 'Custom' ? custom.value.trim() : heading.value;
      const text = body.value.trim();
      if (!title || !text) { status.textContent = 'Enter a clause heading and clause text.';return; }
      const clause = {heading_type:heading.value,heading:title,text:text};
      const proposed = clauses.slice();
      if (editing < 0) proposed.push(clause);else proposed[editing] = clause;
      if (proposed.length > 50 || proposed.reduce((sum,item) => sum + item.heading.length + item.text.length,0) > 20000) {
        status.textContent = 'Use at most 50 clauses and 20000 characters in total.';return;
      }
      clauses.splice(0,clauses.length,...proposed);mode.value = 'add';
      stageClauses();renderTable();closeEntry();
      document.getElementById('sc-table-status').textContent = 'Clause added to the table. Click Save to keep these changes on the lease.';
    });
    form.addEventListener('submit',function (event) {
      if (scEntryDirty()) {
        event.preventDefault();submitting = false;
        status.textContent = 'Click Save Clause or Cancel before saving the lease.';
      }
    });
    renderTable();
  }
  const bank = document.getElementById('bank_detail_id');
  if (bank) {
    let accounts = JSON.parse(document.getElementById('capture-banks').textContent);
    const rows = document.getElementById('capture-bank-rows');
    const editor = document.getElementById('bank-editor');
    const readonly = document.getElementById('capture-banking').dataset.readonly === '1';
    const status = document.getElementById('bank-selection-status');
    let busy = false;
    let editorInitial = '';
    const editorState = () => JSON.stringify(Array.from(editor.querySelectorAll('input,select')).map(input => [input.name,input.value]));
    bankEditorDirty = () => !editor.hidden && editorState() !== editorInitial;
    function closeEditor() {
      editor.hidden = true;editor.disabled = true;
      editor.querySelectorAll('input,select').forEach(input => { input.value = ''; });
      document.getElementById('bank-editor-status').textContent = '';
    }
    function openEditor(account) {
      closeEditor();editor.hidden = false;editor.disabled = false;
      document.getElementById('bank-operation').value = account ? 'edit' : 'add';
      document.getElementById('bank-edit-id').value = account ? account.bank_detail_id : '';
      document.getElementById('bank-editor-title').textContent = account ? 'Edit Bank' : 'Add Bank';
      editor.querySelectorAll('input,select').forEach(input => {
        if (account && Object.hasOwn(account,input.name)) input.value = account[input.name] || '';
      });
      editorInitial = editorState();
      editor.querySelector('[name="bank_name"]').focus();
    }
    function renderAccounts() {
      rows.replaceChildren();
      if (!accounts.length) {
        const cell = rows.insertRow().insertCell();cell.colSpan = 4;
        cell.textContent = 'No bank accounts recorded for this property.';
      }
      accounts.forEach(account => {
        const row = rows.insertRow();const assigned = String(account.bank_detail_id) === bank.value;
        row.style.backgroundColor = assigned ? '#dcfce7' : '';
        [account.bank_name,account.account_holder,'\u2022\u2022\u2022\u2022' + String(account.account_number || '').slice(-4)].forEach(value => {
          row.insertCell().textContent = value || 'Not recorded';
        });
        const actions = row.insertCell();
        const edit = document.createElement('button');edit.type = 'button';edit.textContent = 'Edit';edit.disabled = readonly;
        edit.addEventListener('click',() => openEditor(account));actions.append(edit);
        const assign = document.createElement('button');assign.type = 'button';assign.textContent = 'Assign';assign.disabled = readonly;
        assign.addEventListener('click',function () {
          bank.value = String(account.bank_detail_id);
          document.getElementById('refresh-bank-snapshot').value = '1';
          renderAccounts();status.textContent = 'Assignment not yet saved. Click Save to keep it on this lease.';
        });actions.append(assign);
        if (assigned) { const label = document.createElement('strong');label.textContent = ' Assigned to this Lease';actions.append(label); }
      });
    }
    async function saveBank() {
      if (busy) return false;
      if (!form.reportValidity()) return false;
      busy = true;document.getElementById('bank-save').disabled = true;
      try {
        const data = new FormData(form);data.set('action','save_bank');
        const response = await fetch(form.getAttribute('action'),{method:'POST',body:data,credentials:'same-origin'});
        const result = await response.json().catch(() => ({}));
        if (!response.ok) throw new Error(result.error || 'Bank account could not be saved. Reload if the draft changed or your session expired.');
        accounts = result.accounts;closeEditor();renderAccounts();
        status.textContent = 'Bank account saved. Lease assignment has not changed; click Assign and Save to choose an account.';
        return true;
      } catch (error) {
        document.getElementById('bank-editor-status').textContent = error.message;return false;
      } finally { busy = false;document.getElementById('bank-save').disabled = false; }
    }
    // The main Save still uses the ordinary protected, version-checked draft save.
    form.addEventListener('submit',async function (event) {
      if (editor.hidden) return;
      event.preventDefault();submitting = false;
      if (await saveBank()) form.requestSubmit(document.getElementById('capture-save'));
    });
    document.getElementById('bank-save').addEventListener('click',saveBank);
    document.getElementById('bank-add').addEventListener('click',() => openEditor(null));
    document.getElementById('bank-cancel').addEventListener('click',closeEditor);
    closeEditor();renderAccounts();originalState = state();
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
    if (existing) { document.getElementById('party-tenant-status').textContent = '';return; }
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
    const fields = {LEGAL_NAME:'legal_name',IDENTITY_NUMBER:'identity_number',SIGNATORY_NAME:'signatory_name',SIGNATORY_IDENTITY_NUMBER:'signatory_identity_number',ADDRESS:'address',EMAIL:'email',PHONE:'phone'};
    Object.keys(fields).forEach(function (suffix) {
      const input = document.getElementById('field_' + prefix + suffix);
      if (input) input.value = tenant[fields[suffix]] || '';
    });
  });
}());
