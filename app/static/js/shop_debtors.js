(() => {
  const add = document.getElementById('add-debtor');
  const area = document.getElementById('debtor-working-area');
  const form = document.getElementById('debtor-capture-form');
  if (!add || !area || !form) return;

  const picker=document.getElementById('existing-debtor-search');
  const existingId=document.getElementById('existing-debtor-id');
  if(picker) picker.addEventListener('input',()=>{
    const match=[...document.querySelectorAll('#existing-debtor-options option')].find(option=>option.value===picker.value);
    existingId.value=match?.dataset.id || '';
    for(const field of ['name','phone','email','address']) {
      const input=form.elements.namedItem(field);
      input.readOnly=!!match;
      input.value=match?.dataset[field] || '';
    }
  });
  add.addEventListener('click', () => {
    document.getElementById('existing-debtor-picker').hidden=false;
    picker.value='';existingId.value='';
    // Reuse the single form, clearing edit identity so Add cannot overwrite a debtor.
    const identity = form.elements.namedItem('debtor_id');
    identity.value = '';
    identity.disabled = true;
    for (const field of ['name', 'phone', 'email', 'address']) {
      form.elements.namedItem(field).value = '';
      form.elements.namedItem(field).readOnly = false;
    }
    const error = form.querySelector('[role="alert"]');
    if (error) error.hidden = true;
    document.getElementById('debtor-form-title').textContent = 'Add Debtor';
    area.hidden = false;
    add.setAttribute('aria-expanded', 'true');
    form.elements.namedItem('name').focus();
  });

  document.getElementById('close-debtor').addEventListener('click', () => {
    area.hidden = true;
    add.setAttribute('aria-expanded', 'false');
    add.focus();
  });
})();
