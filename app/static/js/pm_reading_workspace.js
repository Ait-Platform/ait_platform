/* Meter Readings presentation only; existing endpoints validate and save all records. */
(() => {
  const workspaces=document.getElementById('reading-workspaces');
  if (!workspaces) return;
  const sections=Array.from(workspaces.querySelectorAll('[data-reading-section]'));
  const menu=Array.from(document.querySelectorAll('#reading-menu [data-reading-open]'));
  const filter=document.getElementById('reading-meter-filter');
  const month=document.getElementById('municipal-reading-month');
  function contextualURL(href) {
    const target=new URL(href,document.baseURI);
    if (filter) target.searchParams.set('view_item',filter.value);
    if (month) target.searchParams.set('billing_month',month.value);
    return target;
  }
  function remember(target) {
    if (target.origin===window.location.origin) window.history.replaceState(null,'',target.href);
  }
  function show(name) {
    workspaces.hidden=!name;
    sections.forEach(section=>{section.hidden=section.dataset.readingSection!==name;});
    menu.forEach(link=>link.setAttribute('aria-expanded',String(link.dataset.readingOpen===name)));
    const captureMonth=document.querySelector('#utilities-capture [name="billing_month"]');
    if (captureMonth && month) captureMonth.value=month.value;
    if (name) sections.find(section=>section.dataset.readingSection===name)?.querySelector('[autofocus]')?.focus();
  }
  menu.forEach(link=>link.addEventListener('click',event=>{
    const target=contextualURL(link.href);link.href=target.href;
    // Reopen the existing GET endpoint when leaving Edit for a clean Capture form.
    if (link.dataset.readingOpen==='capture' && workspaces.dataset.formMode==='edit') return;
    event.preventDefault();show(link.dataset.readingOpen);remember(target);
  }));
  workspaces.querySelectorAll('[data-reading-close]').forEach(link=>link.addEventListener('click',event=>{
    event.preventDefault();show('');remember(contextualURL(link.href));
  }));
  workspaces.querySelectorAll('[data-reading-record]').forEach(link=>link.addEventListener('click',()=>{
    link.href=contextualURL(link.href).href;
  }));
  show(workspaces.dataset.initialWorkspace || '');
})();
