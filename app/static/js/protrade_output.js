/* ProTrade export controls; local saving uses the existing shared folder API. */
(() => {
  'use strict';
  const email=document.getElementById('output-email'),form=document.getElementById('protrade-output-select');
  const message=document.getElementById('output-action-message');
  email?.addEventListener('click',()=>{const target=document.getElementById('output-email-form');target.hidden=!target.hidden;email.setAttribute('aria-expanded',String(!target.hidden));if(!target.hidden)target.querySelector('[name=to_email]').focus();});
  document.getElementById('output-email-cancel')?.addEventListener('click',()=>{document.getElementById('output-email-form').hidden=true;email.setAttribute('aria-expanded','false');email.focus();});
  const emailForm=document.getElementById('output-email-form');
  let sending=false;
  if(emailForm)emailForm.addEventListener('submit',event=>{if(sending){event.preventDefault();return;}sending=true;emailForm.querySelector('button[type=submit]').disabled=true;document.getElementById('output-email-cancel').disabled=true;});
  document.getElementById('output-print')?.addEventListener('click',()=>window.print());
  document.getElementById('output-shop')?.addEventListener('change',()=>{form.querySelector('[name=selection]')?.removeAttribute('required');window.location.href='/mechanic/output?shop_id='+encodeURIComponent(document.getElementById('output-shop').value);});
  for(const id of ['output-selection','output-recipient'])document.getElementById(id)?.addEventListener('change',()=>{
    email.disabled=true;document.getElementById('output-print').disabled=true;
    if(message)message.textContent='Selection changed. Click Preview before printing, downloading or emailing.';
    const target=document.getElementById('output-email-form');if(target)target.hidden=true;
    const download=document.getElementById('output-download');if(download)download.hidden=true;
  });
})();
