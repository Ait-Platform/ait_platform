// Shared by all ProTrade pages, including numeric controls added dynamically.
(() => {
  function selectNumber(input) {
    if (input instanceof HTMLInputElement && input.type === 'number'
        && !input.matches(':disabled') && !input.readOnly) {
      input.select();
    }
  }

  document.addEventListener('focusin', event => selectNumber(event.target));
  // Mouse caret placement can collapse the selection made during focus.
  // Reselect after the click without cancelling native spinner behaviour.
  document.addEventListener('click', event => selectNumber(event.target));
  selectNumber(document.activeElement);
})();
