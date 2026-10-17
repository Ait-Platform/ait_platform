/* Part A only: registered-owner selection and natural/juristic lessee display. */
(function () {
  'use strict';
  const byId = id => document.getElementById(id);
  const choice = byId('lease-owner-choice');
  if (!choice) return;
  const rows = Array.from(document.querySelectorAll('[data-lessor-row]'));
  const representative = byId('lessor-representative-owner');
  function synchronizeOwners() {
    const selected = new Set(rows.filter(row => !row.hidden).map(row => row.dataset.lessorRow));
    Array.from(choice.options).forEach(option => { if (option.value) option.disabled = selected.has(option.value); });
    Array.from(representative.options).forEach(option => {
      if (option.value) { option.hidden = !selected.has(option.value); option.disabled = option.hidden; }
    });
    if (representative.value && !selected.has(representative.value)) representative.value = '';
    byId('lease-lessor-empty').hidden = selected.size > 0;
  }
  byId('lease-add-owner').addEventListener('click', () => {
    const row = rows.find(item => item.dataset.lessorRow === choice.value);
    if (!row) { byId('lease-owner-message').textContent = 'Choose a registered property owner first.'; return; }
    row.hidden = false;
    row.querySelector('input[name="lessor_id"]').disabled = false;
    choice.value = '';
    byId('lease-owner-message').textContent = '';
    synchronizeOwners();
  });
  document.querySelectorAll('[data-remove-lessor]').forEach(button => button.addEventListener('click', () => {
    const row = rows.find(item => item.dataset.lessorRow === button.dataset.removeLessor);
    row.hidden = true;
    row.querySelector('input[name="lessor_id"]').disabled = true;
    synchronizeOwners();
  }));
  representative.addEventListener('change', () => {
    const option = representative.options[representative.selectedIndex];
    if (!option || !option.value || option.disabled) return;
    byId('lessor-signatory_name').value = option.dataset.name;
    byId('lessor-signatory_identity_number').value = option.dataset.identity;
    // Capacity is deliberately entered, not inferred from ownership.
  });
  function synchronizeLessee() {
    const juristic = byId('lessee-party_type').value === 'juristic_person';
    const fields = byId('lessee-representative-fields');
    fields.hidden = !juristic;
    fields.disabled = !juristic;
    document.querySelector('label[for="lessee-legal_name"]').textContent = juristic ? 'Registered entity name' : 'Full name';
    document.querySelector('label[for="lessee-identity_number"]').textContent = juristic ? 'Registration number' : 'ID number';
  }
  byId('lessee-party_type').addEventListener('change', synchronizeLessee);
  byId('lessor-service_address').addEventListener('input', () => {
    byId('lessor-notice-warning').hidden = Boolean(byId('lessor-service_address').value.trim());
  });
  synchronizeOwners();
  synchronizeLessee();
}());
