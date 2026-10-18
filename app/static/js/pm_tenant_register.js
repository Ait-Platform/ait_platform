/* Representatives apply only to juristic tenants. Server validation remains authoritative. */
(function () {
  'use strict';
  const type = document.getElementById('tenant-party_type');
  const representative = document.getElementById('tenant-representative');
  if (!type || !representative) return;
  function synchronize() {
    const juristic = type.value === 'juristic_person';
    representative.hidden = !juristic;
    representative.disabled = !juristic;
  }
  type.addEventListener('change', synchronize);
  synchronize();
}());
