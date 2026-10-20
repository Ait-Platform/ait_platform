(function () {
  'use strict';
  const certificate = document.getElementById('approved-certificate');
  if (certificate) {
    let parent = certificate.parentElement;
    while (parent && parent !== document.documentElement) {
      parent.classList.add('certificate-print-ancestor');
      parent = parent.parentElement;
    }
  }
  const source = document.getElementById('certificate-registers');
  if (!source) return;
  const registers = JSON.parse(source.textContent);
  document.querySelectorAll('.register-choice').forEach(function (select) {
    select.addEventListener('change', function () {
      if (select.value === '') return;
      const record = registers[select.dataset.kind][Number(select.value)];
      const prefix = select.dataset.field.replace(/(?:LEGAL_)?NAME$/, '');
      const fields = select.dataset.kind === 'owner' ? {NAME: 'name', IDENTITY_NUMBER: 'identity_number'} : {
        LEGAL_NAME:'legal_name', IDENTITY_NUMBER:'identity_number', SIGNATORY_NAME:'signatory_name',
        SIGNATORY_IDENTITY_NUMBER:'signatory_identity_number', SIGNATORY_CAPACITY:'signatory_capacity'
      };
      Object.keys(fields).forEach(function (suffix) {
        const input = document.getElementById('field_' + prefix + suffix);
        if (input) input.value = record[fields[suffix]] || '';
      });
    });
  });
  document.querySelectorAll('#certificate-form textarea').forEach(function (input) {
    function resize() { input.style.height = 'auto'; input.style.height = input.scrollHeight + 'px'; }
    input.addEventListener('input', resize); resize();
  });
}());
