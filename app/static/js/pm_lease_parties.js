/* Part A selections only; persisted canonical identities are resolved server-side. */
(function () {
  'use strict';
  const owners = Array.from(document.querySelectorAll('input[name="representative_owner_id"]'));
  owners.forEach(box => box.addEventListener('change', () => {
    if (box.disabled) return;
    if (box.checked) owners.forEach(other => { if (other !== box) other.checked = false; });
    // Allow clearing a checkbox; Save still requires exactly one eligible owner.
  }));
  const body = document.getElementById('selected-tenants-body');
  if (!body) return;
  const tenants = Array.from(document.querySelectorAll('input[name="tenant_ids"]'));
  const templates = new Map(Array.from(document.querySelectorAll('template[data-tenant-row]')).map(node => [node.dataset.tenantRow, node]));
  const empty = document.getElementById('selected-tenants-empty');
  function synchronizeTenants() {
    const selected = tenants.filter(box => box.checked);
    body.replaceChildren(...selected.map(box => templates.get(box.value)).filter(Boolean).map(template => template.content.cloneNode(true)));
    if (empty) empty.hidden = selected.length > 0;
  }
  tenants.forEach(box => box.addEventListener('change', synchronizeTenants));
  body.addEventListener('click', event => {
    const button = event.target.closest('[data-remove-tenant]');
    if (!button || !body.contains(button)) return;
    const box = tenants.find(candidate => candidate.value === button.dataset.removeTenant);
    if (box) { box.checked = false; synchronizeTenants(); }
  });
  synchronizeTenants();
}());
