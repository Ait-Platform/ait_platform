/* Existing-record lookup; association is validated in the Save transaction. */
(() => {
  const form=document.getElementById('municipal-account-form');
  if (!form) return;
  const number=document.getElementById('account_number');
  const status=document.getElementById('account-lookup-status');
  const retry=document.getElementById('account-lookup-retry');
  const saveButtons=form.querySelectorAll('button[name="save_action"]');
  const accountId=form.querySelector('input[name="account_id"]');
  const version=form.querySelector('input[name="version"]');
  let timer,controller,generation=0,hadRecord=form.dataset.existingAccount==='yes';
  function lock(locked) {
    for (const button of saveButtons) button.disabled=locked;
    for (const id of ['owner_name','water_number','electricity_number']) { const field=document.getElementById(id);field.readOnly=locked;if(field.tagName==='SELECT') field.disabled=locked; }
  }
  function clearRecord() {
    accountId.value='';version.value='';
    if (hadRecord) for (const id of ['owner_name','water_number','electricity_number']) document.getElementById(id).value='';
    hadRecord=false;
  }
  async function search() {
    clearTimeout(timer);if (controller) controller.abort();
    const attempt=++generation;const query=number.value.trim();retry.hidden=true;
    if (!query) { status.hidden=true;lock(false);return; }
    lock(true);status.hidden=true;controller=new AbortController();
    try {
      const url=new URL(form.dataset.lookupUrl,window.location.origin);url.searchParams.set('account_number',query);
      const response=await fetch(url,{headers:{Accept:'application/json'},credentials:'same-origin',cache:'no-store',signal:controller.signal});
      if (!response.ok) throw new Error('Lookup failed (HTTP '+response.status+'). Check the account number and your access, then retry.');
      if (!(response.headers.get('Content-Type') || '').includes('application/json')) throw new Error('Lookup returned a sign-in page or unexpected response. Sign in again and retry.');
      const result=await response.json();
      if (attempt!==generation || number.value.trim()!==query) return;
      if (!result || typeof result.found!=='boolean') throw new Error('Lookup returned an invalid response. Retry before saving.');
      status.hidden=true;status.textContent='';
      if (!result.found) { hadRecord=false;lock(false);return; }
      hadRecord=true;
      if (result.matches) { clearRecord();hadRecord=true;status.hidden=false;status.textContent='Multiple accounts use this number. Resolve the duplicate records before saving.';lock(true);return; }
      if (!result.details_available) { clearRecord();hadRecord=true; }
      if (result.account_id) accountId.value=result.account_id;
      if (!result.editable) { status.hidden=false;status.textContent=(result.association_conflicts || []).join(' ') || result.message || 'This account cannot be edited from the selected property.'; }
      if (result.details_available) {
        number.value=result.values.account_number;
        for (const [key,value] of Object.entries(result.values)) {
          const field=document.getElementById(key);if (field && key!=='account_number') { if(key==='owner_name' && field.tagName==='SELECT' && value && !Array.from(field.options).some(option=>option.value===value)) { const option=document.createElement('option');option.value=value;option.textContent=value+' — existing account owner';field.appendChild(option); } field.value=value || ''; }
        }
      }
      if (result.editable) { accountId.value=result.account_id;version.value=result.version; }
      lock(!result.editable);
    } catch (error) {
      if (error.name==='AbortError' || attempt!==generation) return;
      status.hidden=false;status.textContent=error.message || 'Lookup failed. Retry before saving.';retry.hidden=false;lock(true);
    }
  }
  number.addEventListener('input',() => {
    clearTimeout(timer);if (controller) controller.abort();generation++;
    clearRecord();lock(true);status.hidden=true;
    timer=setTimeout(search,350);
  });
  number.addEventListener('change',search);
  retry.addEventListener('click',search);
})();
