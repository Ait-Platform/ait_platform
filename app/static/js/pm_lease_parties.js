/* Existing owner records: select exactly one representative. */
(function () {
  'use strict';
  const boxes = Array.from(document.querySelectorAll('input[name="representative_owner_id"]'));
  boxes.forEach(box => box.addEventListener('change', () => {
    if (box.checked) boxes.forEach(other => { if (other !== box) other.checked = false; });
    else if (!boxes.some(other => other.checked)) box.checked = true;
  }));
}());
