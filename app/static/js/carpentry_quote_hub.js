(() => {
  const nav=document.getElementById('quote-hub-navigation');
  if (!nav) return;
  const areas={quote:document.getElementById('quote-area-form'),register:document.getElementById('quote-area-register')};
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
  const details=Array.from(document.querySelectorAll('[data-quote-details]'));
  const expandLinks=Array.from(document.querySelectorAll('[data-expand-quote]'));
  expandLinks.forEach(link=>link.addEventListener('click',event=>{
    event.preventDefault();
    const id=link.dataset.expandQuote;
    const current=details.find(row=>row.dataset.quoteDetails===id);
    const opening=current?.hidden;
    details.forEach(row=>{row.hidden=!opening || row.dataset.quoteDetails!==id;});
    expandLinks.forEach(candidate=>candidate.setAttribute('aria-expanded',String(opening && candidate.dataset.expandQuote===id)));
    show('register');
    const target=new URL(link.href);
    if(!opening) target.searchParams.delete('expanded_quote_id');
    window.history.replaceState(null,'',target.pathname+target.search);
  }));
  show(nav.dataset.initialArea);
})();
