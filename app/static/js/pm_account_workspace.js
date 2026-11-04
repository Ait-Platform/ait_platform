/* Accounts-only workspace presentation; existing endpoints perform all saves. */
(() => {
  const workspaces=document.getElementById('account-workspaces');
  if (!workspaces) return;
  const sections=Array.from(workspaces.querySelectorAll('[data-account-section]'));
  const menu=Array.from(document.querySelectorAll('#account-menu [data-account-open]'));
  function remember(href) {
    const target=new URL(href,document.baseURI);
    if (target.origin===window.location.origin) window.history.replaceState(null,'',target.href);
  }
  function show(name) {
    workspaces.hidden=!name;
    sections.forEach(section=>{section.hidden=section.dataset.accountSection!==name;});
    menu.forEach(link=>link.setAttribute('aria-expanded',String(link.dataset.accountOpen===name)));
    if (name) sections.find(section=>section.dataset.accountSection===name)?.querySelector('[autofocus]')?.focus();
  }
  menu.forEach(link=>link.addEventListener('click',event=>{
    const form=document.getElementById('municipal-account-form');
    // The list response omits the form; existing records must reopen a clean Add via GET.
    if (link.dataset.accountOpen==='form' && (!form || workspaces.dataset.formMode==='edit' || form.querySelector('[name="account_id"]').value)) return;
    event.preventDefault();show(link.dataset.accountOpen);remember(link.href);
  }));
  workspaces.querySelectorAll('[data-account-close]').forEach(link=>link.addEventListener('click',event=>{
    event.preventDefault();show('');remember(link.href);
  }));
  show(workspaces.dataset.initialWorkspace || '');
})();
