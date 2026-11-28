/* Shared AIT Output actions; programme adapters own selection/data and permissions. */
(() => {
  'use strict';
  const form=document.getElementById('output-email-form'),email=document.getElementById('output-email');
  const cancel=document.getElementById('output-email-cancel'),message=document.getElementById('output-action-message');
  email?.addEventListener('click',()=>{if(!form)return;form.hidden=false;email.setAttribute('aria-expanded','true');form.querySelector('[name=to_email]').focus();});
  cancel?.addEventListener('click',()=>{form.hidden=true;email.setAttribute('aria-expanded','false');email.focus();});
  let sending=false;
  form?.addEventListener('submit',event=>{if(sending){event.preventDefault();return;}sending=true;form.querySelector('[type=submit]').disabled=true;cancel.disabled=true;message.textContent='Submitting the displayed PDF for email. Please wait; do not resubmit.';});
  document.getElementById('output-print')?.addEventListener('click',()=>window.print());
})();
