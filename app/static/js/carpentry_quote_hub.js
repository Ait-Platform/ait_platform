(() => {
  const nav=document.getElementById('quote-hub-navigation');
  if (!nav) return;
  const areas={debtor:document.getElementById('quote-area-debtor'),quote:document.getElementById('quote-area-form'),register:document.getElementById('quote-area-register')};
  const buttons=Array.from(nav.querySelectorAll('[data-quote-area]'));
  let active=null;
  function show(name) {
    active=name;
    Object.entries(areas).forEach(([key,area])=>{area.hidden=key!==name;});
    buttons.forEach(button=>button.setAttribute('aria-expanded',String((button.dataset.quoteArea==='new'?'quote':button.dataset.quoteArea)===name)));
  }
  buttons.forEach(button=>button.addEventListener('click',()=>{
    const name=button.dataset.quoteArea;
    show(name==='new'?'quote':name);
    if (name==='new') document.dispatchEvent(new Event('carpentry:new-quote'));
  }));
  document.querySelectorAll('[data-close-quote-area]').forEach(button=>button.addEventListener('click',()=>{
    const prior=active;show(null);
    buttons.find(candidate=>(candidate.dataset.quoteArea==='new'?'quote':candidate.dataset.quoteArea)===prior)?.focus();
  }));
  show(nav.dataset.initialArea);
})();
