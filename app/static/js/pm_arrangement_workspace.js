/* Payment Arrangements presentation only; existing routes own forms and writes. */
(() => {
  const workspaces=document.getElementById('arrangement-workspaces');
  if (!workspaces) return;
  const sections=Array.from(workspaces.querySelectorAll('[data-arrangement-section]'));
  const menu=Array.from(document.querySelectorAll('#arrangement-menu [data-arrangement-open]'));
  function remember(href) {
    const target=new URL(href,document.baseURI);
    if (target.origin===window.location.origin) window.history.replaceState(null,'',target.href);
  }
  function show(name) {
    workspaces.hidden=!name;
    sections.forEach(section=>{section.hidden=section.dataset.arrangementSection!==name;});
    menu.forEach(link=>link.setAttribute('aria-expanded',String(link.dataset.arrangementOpen===name)));
    if (name) sections.find(section=>section.dataset.arrangementSection===name)?.querySelector('[autofocus]')?.focus();
  }
  menu.forEach(link=>link.addEventListener('click',event=>{
    if (link.getAttribute('aria-disabled')==='true') {event.preventDefault();return;}
    // Existing GET endpoints supply the appropriate create/edit/detail/register response.
    if (!sections.some(section=>section.dataset.arrangementSection===link.dataset.arrangementOpen)) return;
    // Add must never reuse an Edit form or submit a create form to a detail endpoint.
    if (link.dataset.arrangementOpen==='form' && !document.getElementById('arrangement-create')) return;
    event.preventDefault();show(link.dataset.arrangementOpen);remember(link.href);
  }));
  workspaces.querySelectorAll('[data-arrangement-close]').forEach(link=>link.addEventListener('click',event=>{
    event.preventDefault();show('');remember(link.href);
  }));
  show(workspaces.dataset.initialWorkspace || '');
})();
