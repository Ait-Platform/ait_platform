/* MAR-only workspace presentation. Existing forms and endpoints perform all writes. */
(() => {
  const workspaces=document.getElementById('mar-workspaces');
  if (!workspaces) return;
  const sections=Array.from(workspaces.querySelectorAll('[data-mar-section]'));
  const menu=Array.from(document.querySelectorAll('#mar-menu [data-mar-open]'));
  const filter=document.getElementById('statement-account-filter');
  function contextualURL(href) {
    const target=new URL(href,document.baseURI);
    if (filter) target.searchParams.set('view_item',filter.value);
    return target;
  }
  function remember(target) {
    if (target.origin===window.location.origin) window.history.replaceState(null,'',target.href);
  }
  function show(name) {
    workspaces.hidden=!name;
    sections.forEach(section=>{section.hidden=section.dataset.marSection!==name;});
    menu.forEach(link=>link.setAttribute('aria-expanded',String(link.dataset.marOpen===name)));
    if (name) {
      const section=sections.find(section=>section.dataset.marSection===name);
      section?.querySelector('[autofocus]')?.focus();
    }
  }
  menu.forEach(link=>link.addEventListener('click',event=>{
    const target=contextualURL(link.href);
    link.href=target.href;
    // An Edit form must be reopened as a clean Add form by its existing GET endpoint.
    if (link.dataset.marOpen==='form' && workspaces.dataset.formMode==='edit') return;
    event.preventDefault();show(link.dataset.marOpen);remember(target);
  }));
  workspaces.querySelectorAll('[data-mar-close]').forEach(link=>link.addEventListener('click',event=>{
    event.preventDefault();show('');remember(contextualURL(link.href));
  }));
  workspaces.querySelectorAll('[data-mar-record]').forEach(link=>link.addEventListener('click',()=>{
    link.href=contextualURL(link.href).href;
  }));
  show(workspaces.dataset.initialWorkspace || '');
})();
