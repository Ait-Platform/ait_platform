/* Part A: one representative from existing owner records; no owner mutations. */
(function () {
  'use strict';
  const byId = id => document.getElementById(id);
  const boxes = Array.from(document.querySelectorAll('input[name="representative_owner_id"]'));
  const confirmation = byId('owner-capacity-confirmation');
  if (!confirmation) return;
  function showRepresentative(resetConfirmation) {
    if (resetConfirmation) confirmation.checked = false;
    const selected = boxes.find(box => box.checked && !box.disabled);
    if (!selected) confirmation.checked = false;
    const data = selected ? selected.dataset : {};
    byId('owner-representative-name').textContent = data.ownerName || 'Select an individual owner above.';
    byId('owner-representative-identity').textContent = data.ownerIdentity || 'Not available';
    byId('owner-representative-address').textContent = data.ownerAddress || 'Not available in the selected owner record.';
    const capacity = confirmation.checked ? byId('owner-capacity-proposal').textContent : data.ownerCapacity;
    byId('owner-representative-capacity').textContent = capacity || 'Not confirmed';
    byId('lessor-capacity-warning').hidden = Boolean(capacity);
    byId('lessor-notice-warning').hidden = Boolean((data.ownerAddress || '').trim());
  }
  boxes.forEach(box => box.addEventListener('change', () => {
    if (box.checked) boxes.forEach(other => { if (other !== box) other.checked = false; });
    else if (!boxes.some(other => other.checked)) box.checked = true;
    showRepresentative(true);
  }));
  confirmation.addEventListener('change', () => showRepresentative(false));
  function synchronizeLessee() {
    const juristic = byId('lessee-party_type').value === 'juristic_person';
    const fields = byId('lessee-representative-fields');
    fields.hidden = !juristic; fields.disabled = !juristic;
    document.querySelector('label[for="lessee-legal_name"]').textContent = juristic ? 'Registered entity name' : 'Full name';
    document.querySelector('label[for="lessee-identity_number"]').textContent = juristic ? 'Registration number' : 'ID number';
  }
  byId('lessee-party_type').addEventListener('change', synchronizeLessee);
  showRepresentative(false); synchronizeLessee();
}());
