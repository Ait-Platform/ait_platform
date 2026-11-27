/* ProTrade export controls; local saving uses the existing shared folder API. */
(() => {
  'use strict';
  const email=document.getElementById('output-email'),form=document.getElementById('protrade-output-select');
  const message=document.getElementById('output-action-message');
  email?.addEventListener('click',()=>{const target=document.getElementById('output-email-form');if(!target){message.textContent='The email interface is unavailable. Preview the document again.';return;}target.hidden=false;email.setAttribute('aria-expanded','true');target.scrollIntoView({block:'nearest'});target.querySelector('[name=to_email]').focus();});
  document.getElementById('output-email-cancel')?.addEventListener('click',()=>{document.getElementById('output-email-form').hidden=true;email.setAttribute('aria-expanded','false');email.focus();});
  const emailForm=document.getElementById('output-email-form');
  let sending=false;
  if(emailForm)emailForm.addEventListener('submit',event=>{if(sending){event.preventDefault();return;}sending=true;const submit=emailForm.querySelector('button[type=submit]');submit.disabled=true;submit.textContent='Sending...';document.getElementById('output-email-cancel').disabled=true;message.textContent='Submitting the selected PDF for email. Please wait; do not resubmit.';});
  document.getElementById('output-print')?.addEventListener('click',()=>window.print());
  document.getElementById('output-shop')?.addEventListener('change',()=>{form.querySelector('[name=selection]')?.removeAttribute('required');window.location.href='/mechanic/output?shop_id='+encodeURIComponent(document.getElementById('output-shop').value);});
  const category=document.getElementById('output-category'),selection=document.getElementById('output-selection');
  const month=document.getElementById('output-month'),from=document.getElementById('output-from'),to=document.getElementById('output-to');
  const choices=JSON.parse(document.getElementById('output-choices').textContent);
  function invalidate(){
    email.disabled=true;document.getElementById('output-print').disabled=true;
    if(message)message.textContent='Selection changed. Click Preview before printing, downloading or emailing.';
    const target=document.getElementById('output-email-form');if(target)target.hidden=true;
    const download=document.getElementById('output-download');if(download){download.removeAttribute('href');download.setAttribute('aria-disabled','true');download.setAttribute('tabindex','-1');}
  }
  function documents(){
    const previous=selection.value;
    selection.replaceChildren(new Option('Choose an available document',''));
    for(const item of choices){
      if(item.kind!==category.value)continue;
      if((from.value || to.value) && (!item.date || (from.value && item.date<from.value) || (to.value && item.date>to.value)))continue;
      selection.add(new Option(item.label,item.key));
    }
    if(Array.from(selection.options).some(option=>option.value===previous))selection.value=previous;
  }
  category?.addEventListener('change',()=>{
    const dated=category.value!=='price_list';
    for(const field of [month,from,to]){field.disabled=!dated;field.value='';}
    selection.value='';documents();invalidate();
    document.getElementById('output-period-help').textContent=dated?'Filter saved documents by issue date. Saved financial records and statement periods remain unchanged.':'Price Lists show current configured rates; dates do not apply.';
  });
  month?.addEventListener('change',()=>{
    if(month.value){const [year,part]=month.value.split('-').map(Number);from.value=month.value+'-01';to.value=month.value+'-'+String(new Date(year,part,0).getDate()).padStart(2,'0');}
    else{from.value='';to.value='';}
    documents();invalidate();
  });
  for(const field of [from,to])field?.addEventListener('change',()=>{month.value='';documents();invalidate();});
  selection?.addEventListener('change',invalidate);
  document.getElementById('email-debtor')?.addEventListener('change',event=>{
    emailForm.querySelector('[name=to_email]').value=event.target.value ? (event.target.selectedOptions[0]?.dataset.email || '') : emailForm.dataset.defaultRecipient;
  });
})();
