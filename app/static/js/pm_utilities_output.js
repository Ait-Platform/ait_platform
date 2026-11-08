(() => {
  const form=document.getElementById('output-filters');if(!form || form.dataset.outputInitialized)return;
  form.dataset.outputInitialized='true';
  const source=document.getElementById('output-source');
  const item=document.getElementById('output-item');
  const print=document.getElementById('output-print');
  const email=document.getElementById('output-email');
  const emailForm=document.getElementById('output-email-form');
  const download=document.getElementById('output-download');
  const workspace=document.getElementById('output-workspace');
  document.getElementById('output-hub-open')?.addEventListener('click',event=>{event.preventDefault();workspace.hidden=false;});
  document.getElementById('output-hub-close')?.addEventListener('click',event=>{event.preventDefault();workspace.hidden=true;emailForm.hidden=true;});
  const choices=JSON.parse(document.getElementById('output-choices').textContent);
  function dirty(){if(download)download.hidden=true;print.disabled=true;email.disabled=true;emailForm.hidden=true;email.setAttribute('aria-expanded','false');}
  source.addEventListener('change',()=>{
    item.replaceChildren(new Option(source.value==='readings'?'All Meters':'All Accounts',''));
    (choices[source.value] || []).forEach(row=>item.add(new Option(source.value==='readings'?row.meter_number:row.account_number,String(row.id))));
    dirty();
  });
  form.querySelectorAll('input,select').forEach(control=>control.addEventListener('change',dirty));
  print.addEventListener('click',()=>{if(!print.disabled)window.print();});
  email.addEventListener('click',()=>{if(email.disabled)return;emailForm.hidden=!emailForm.hidden;email.setAttribute('aria-expanded',String(!emailForm.hidden));if(!emailForm.hidden)document.getElementById('email-to').focus();});
})();
