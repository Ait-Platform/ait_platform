(() => {
  const form = document.getElementById('property-ledger-form');
  const button = document.getElementById('open-property-ledger');
  if (!form || !button) return;
  const radios = document.querySelectorAll('input[type="radio"][name="property_id"][form="property-ledger-form"]');
  const selected = () => Array.from(radios).some(radio => radio.checked && !radio.disabled);
  const sync = () => { button.disabled = !selected(); };
  radios.forEach(radio => radio.addEventListener('change', sync));
  form.addEventListener('submit', event => {
    sync();
    if (button.disabled) event.preventDefault();
  });
  window.addEventListener('pageshow', sync);
  sync();
})();
