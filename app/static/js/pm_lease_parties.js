/* Part A selections only; persisted canonical identities are resolved server-side. */
(function () {
  'use strict';
  const owners = Array.from(document.querySelectorAll('input[name="representative_owner_id"]'));
  owners.forEach(box => box.addEventListener('change', () => {
    if (box.disabled) return;
    if (box.checked) owners.forEach(other => { if (other !== box) other.checked = false; });
    // Allow clearing a checkbox; Save still requires exactly one eligible owner.
  }));
}());
