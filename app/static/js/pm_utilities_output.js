(() => {
  const form=document.getElementById('output-filters');if(!form || form.dataset.outputInitialized)return;
  form.dataset.outputInitialized='true';
  const source=document.getElementById('output-source'),report=document.getElementById('output-report'),item=document.getElementById('output-item');
  const print=document.getElementById('output-print'),email=document.getElementById('output-email'),emailForm=document.getElementById('output-email-form');
  const choices=JSON.parse(document.getElementById('output-choices').textContent),catalog=JSON.parse(document.getElementById('output-catalog').textContent);
  const month=document.getElementById('output-month'),recipient=document.getElementById('output-recipient'),first=document.getElementById('output-from'),last=document.getElementById('output-to');
  function dirty(){print.disabled=true;email.disabled=true;emailForm.hidden=true;email.setAttribute('aria-expanded','false');const download=document.getElementById('output-download');if(download){download.removeAttribute('href');download.setAttribute('aria-disabled','true');download.tabIndex=-1;}const area=document.getElementById('output-preview');if(area)area.hidden=true;document.getElementById('output-preview-help').hidden=false;}
  function context(){
    const period=['metsoa_statement','utilities_reconciliation'].includes(report.value);
    document.getElementById('output-period-controls').hidden=!period;month.required=period;recipient.required=period;
    first.readOnly=last.readOnly=period;first.disabled=last.disabled=month.disabled=report.value==='account_register';
    if(report.value==='account_register'){first.value='';last.value='';}
    if(period && !month.value)month.value=document.getElementById('output-billing-month').value;
    if(period && month.value)applyMonth();
  }
  function applyMonth(){
    if(!month.value)return;
    const [y,m]=month.value.split('-').map(Number);first.value=month.value+'-01';last.value=new Date(Date.UTC(y,m,0)).toISOString().slice(0,10);
    document.getElementById('output-billing-month').value=month.value;
  }
  [first,last].forEach(input=>input.addEventListener('change',()=>{if(!first.readOnly)month.value='';}));
  source.addEventListener('change',()=>{
    report.replaceChildren(...(catalog[source.value] || []).map(([key,label])=>new Option(label,key)));
    item.replaceChildren(new Option(source.value==='readings'?'All Meters':'All Accounts',''));
    (choices[source.value] || []).forEach(row=>item.add(new Option(source.value==='readings'?row.meter_number:row.account_number,String(row.id))));
    context();dirty();
  });
  report.addEventListener('change',()=>{context();dirty();});month.addEventListener('change',()=>{applyMonth();context();});
  form.querySelectorAll('input,select').forEach(control=>control.addEventListener('change',dirty));
  print.addEventListener('click',()=>{if(!print.disabled)window.print();});
  email.addEventListener('click',()=>{if(email.disabled)return;emailForm.hidden=!emailForm.hidden;email.setAttribute('aria-expanded',String(!emailForm.hidden));if(!emailForm.hidden)document.getElementById('email-to').focus();});
  context();
})();
