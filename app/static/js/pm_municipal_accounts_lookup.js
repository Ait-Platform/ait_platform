/* Read-only exact-number lookup. Property reassignment is never performed here. */
(() => {
  const form=document.getElementById('municipal-account-form');
  if (!form) return;
  const number=document.getElementById('account_number');
  const status=document.getElementById('account-lookup-status');
  const details=document.getElementById('account-lookup-details');
  const retry=document.getElementById('account-lookup-retry');
  const saveButtons=form.querySelectorAll('button[name="save_action"]');
  const accountId=form.querySelector('input[name="account_id"]');
  const version=form.querySelector('input[name="version"]');
  let timer,controller,generation=0,hadRecord=form.dataset.existingAccount==='yes';
  const groups=Array.from(form.querySelectorAll('[data-meter-baseline]'));
  function lock(locked) {
    for (const button of saveButtons) button.disabled=locked;
    for (const id of ['owner_name','water_number','electricity_number']) document.getElementById(id).readOnly=locked;
    for (const group of groups) {
      const kind=group.dataset.meterBaseline;
      for (const suffix of ['opening_reading','opening_date']) {
        const field=document.getElementById(kind+'_'+suffix);
        field.readOnly=locked || group.dataset.established==='yes';
        field.disabled=locked || !document.getElementById(kind+'_number').value.trim();
        field.required=!locked && group.dataset.established!=='yes' && Boolean(document.getElementById(kind+'_number').value.trim());
      }
    }
  }
  function clearRecord() {
    accountId.value='';version.value='';details.replaceChildren();
    for (const group of groups) {
      const kind=group.dataset.meterBaseline;group.dataset.established='no';
      document.getElementById(kind+'_latest_recorded').textContent='';
      if (hadRecord) for (const suffix of ['number','opening_reading','opening_date']) document.getElementById(kind+'_'+suffix).value='';
    }
    if (hadRecord) document.getElementById('owner_name').value='';
    hadRecord=false;
  }
  function line(parent,label,value) {
    if (value===null || value===undefined || value==='') return;
    const paragraph=document.createElement('p');paragraph.textContent=label+': '+value;parent.appendChild(paragraph);
  }
  function review(record) {
    const section=document.createElement('div');section.className='rounded-lg border border-slate-200 p-3';
    line(section,'Association',record.association==='selected_property'?'Selected property':record.association==='other_property'?'Another property':'Unassigned');
    if (record.details_available) {
      line(section,'Account owner',record.values.owner_name);
      line(section,'Owner email',record.owner_email);line(section,'Owner address',record.owner_address);
      for (const kind of ['water','electricity']) for (const meter of record.meters[kind] || []) {
        const label=kind==='water'?'Water':'Electricity';const unit=kind==='water'?'kL':'kWh';
        line(section,label+' meter',meter.number);
        if (meter.latest) line(section,'Latest recorded reading',meter.latest.value+' '+unit+' — '+meter.latest.date);
        else line(section,'Latest recorded reading','Not recorded');
      }
    }
    line(section,'Status',record.message);details.appendChild(section);
  }
  async function search() {
    clearTimeout(timer);if (controller) controller.abort();
    const attempt=++generation;const query=number.value.trim();retry.hidden=true;
    if (!query) { status.textContent='Enter a municipal account number.';lock(false);return; }
    lock(true);status.textContent='Searching existing municipal accounts…';controller=new AbortController();
    try {
      const url=new URL(form.dataset.lookupUrl,window.location.origin);url.searchParams.set('account_number',query);
      const response=await fetch(url,{headers:{Accept:'application/json'},credentials:'same-origin',cache:'no-store',signal:controller.signal});
      if (!response.ok) throw new Error('Lookup failed (HTTP '+response.status+'). Check the account number and your access, then retry.');
      if (!(response.headers.get('Content-Type') || '').includes('application/json')) throw new Error('Lookup returned a sign-in page or unexpected response. Sign in again and retry.');
      const result=await response.json();
      if (attempt!==generation || number.value.trim()!==query) return;
      if (!result || typeof result.found!=='boolean') throw new Error('Lookup returned an invalid response. Retry before saving.');
      details.replaceChildren();status.textContent=result.message;
      if (!result.found) { hadRecord=false;lock(false);return; }
      hadRecord=true;
      if (result.matches) { clearRecord();hadRecord=true;for (const match of result.matches) review(match);lock(true);return; }
      if (!result.details_available) { clearRecord();hadRecord=true; }
      review(result);
      if (result.details_available) {
        number.value=result.values.account_number;
        for (const [key,value] of Object.entries(result.values)) {
          const field=document.getElementById(key);if (field && key!=='account_number') field.value=value || '';
        }
        for (const group of groups) {
          const kind=group.dataset.meterBaseline;const meters=result.meters[kind] || [];
          const entry=meters.length===1?meters[0]:null;group.dataset.established=entry && entry.opening?'yes':'no';
          document.getElementById(kind+'_latest_recorded').textContent=entry && entry.latest?'Latest recorded reading: '+entry.latest.value+' '+(kind==='water'?'kL':'kWh')+' — '+entry.latest.date:'';
        }
      }
      if (result.editable) { accountId.value=result.account_id;version.value=result.version; }
      lock(!result.editable);
    } catch (error) {
      if (error.name==='AbortError' || attempt!==generation) return;
      status.textContent=error.message || 'Lookup failed. Retry before saving.';retry.hidden=false;lock(true);
    }
  }
  number.addEventListener('input',() => {
    clearTimeout(timer);if (controller) controller.abort();generation++;
    clearRecord();lock(true);status.textContent='Checking account number…';
    timer=setTimeout(search,350);
  });
  number.addEventListener('change',search);
  retry.addEventListener('click',search);
})();
