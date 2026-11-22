/* ProTrade export controls; local saving uses the existing shared folder API. */
(() => {
  'use strict';
  const email=document.getElementById('output-email'),form=document.getElementById('protrade-output-select');
  const message=document.getElementById('output-save-message');
  email?.addEventListener('click',()=>{const target=document.getElementById('output-email-form');target.hidden=!target.hidden;email.setAttribute('aria-expanded',String(!target.hidden));});
  const emailForm=document.getElementById('output-email-form');
  if(emailForm)emailForm.addEventListener('submit',()=>{emailForm.querySelector('button[type=submit]').disabled=true;});
  document.getElementById('output-print')?.addEventListener('click',()=>window.print());
  document.getElementById('output-shop')?.addEventListener('change',()=>{form.querySelector('[name=selection]')?.removeAttribute('required');window.location.href='/mechanic/output?shop_id='+encodeURIComponent(document.getElementById('output-shop').value);});
  for(const id of ['output-selection','output-recipient'])document.getElementById(id)?.addEventListener('change',()=>{
    document.getElementById('output-save')?.setAttribute('disabled','');
    email.disabled=true;document.getElementById('output-print').disabled=true;
    if(message)message.textContent='Selection changed. Click Preview before printing, downloading or emailing.';
    const target=document.getElementById('output-email-form');if(target)target.hidden=true;
    const download=document.getElementById('output-download');if(download)download.hidden=true;
  });
})();
