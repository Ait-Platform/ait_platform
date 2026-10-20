(function () {
  'use strict';
  const form = document.getElementById('certificate-form');
  const count = document.getElementById('lessee-count');
  const groups = document.getElementById('capture-lessees');
  const prototype = document.getElementById('capture-lessee-template');
  const data = document.getElementById('capture-tenants');
  if (!form || !count || !groups || !prototype) return;
  const tenants = data ? JSON.parse(data.textContent) : [];
  count.addEventListener('change', function () {
    const total = Number(count.value);
    if (!Number.isInteger(total) || total < 1 || total > 50) return;
    while (groups.children.length > total) groups.lastElementChild.remove();
    while (groups.children.length < total) {
      const index = groups.children.length + 1;
      const holder = document.createElement('template');
      holder.innerHTML = prototype.innerHTML.replaceAll('LESSEE_1_', 'LESSEE_' + index + '_').replaceAll('Lessee 1', 'Lessee ' + index);
      const group = holder.content.firstElementChild;
      group.dataset.lesseeIndex = index;
      group.querySelector('legend').textContent = 'Lessee ' + index;
      groups.appendChild(group);
    }
  });
  form.addEventListener('change', function (event) {
    const select = event.target.closest('.register-choice');
    if (!select || select.value === '') return;
    const tenant = tenants[Number(select.value)];
    if (!tenant) return;
    const prefix = select.dataset.field.replace(/LEGAL_NAME$/, '');
    const fields = {LEGAL_NAME:'legal_name',IDENTITY_NUMBER:'identity_number',SIGNATORY_NAME:'signatory_name',SIGNATORY_IDENTITY_NUMBER:'signatory_identity_number',SIGNATORY_CAPACITY:'signatory_capacity'};
    Object.keys(fields).forEach(function (suffix) {
      const input = document.getElementById('field_' + prefix + suffix);
      if (input) input.value = tenant[fields[suffix]] || '';
    });
  });
}());
