(() => {
  const add = document.getElementById('add-debtor');
  const area = document.getElementById('debtor-working-area');
  const form = document.getElementById('debtor-capture-form');
  if (!add || !area || !form) return;

  add.addEventListener('click', () => {
    // Reuse the single form, clearing edit identity so Add cannot overwrite a debtor.
    const identity = form.elements.namedItem('debtor_id');
    identity.value = '';
    identity.disabled = true;
    for (const field of ['name', 'phone', 'email', 'address']) {
      form.elements.namedItem(field).value = '';
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
