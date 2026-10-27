(function(){
 'use strict';const root=document.getElementById('addendum-pdf-selection');if(!root)return;
 const get=name=>document.getElementById('lease-pdf-'+name),form=get('search'),status=get('search-status'),viewer=document.getElementById('lease-selection-document');
 let state=JSON.parse(get('state').textContent),results=[],sourceHash='',busy=false,loading=true,pageTimer;const pending=new Map();
 const node=(tag,text)=>{const item=document.createElement(tag);if(text!==undefined)item.textContent=text;return item;};
 function link(label){return node('span',label);}
 function key(entry){if(entry.pdf_sha256)return entry.id;const saved=state.selected.find(item=>item.source===entry.source&&item.reference===entry.reference);return saved?saved.id:sourceHash+':'+entry.id;}
 function selections(){const entries=new Map(state.selected.map(entry=>[key(entry),entry]));for(const [id,change] of pending){if(change.checked)entries.set(id,change.entry);else entries.delete(id);}return Array.from(entries.values());}
 async function jsonResponse(response){
  if(response.redirected||response.type==='opaqueredirect')throw Error('Your session or access changed. Sign in again, then retry the unsaved selections.');
  if(!(response.headers.get('Content-Type')||'').toLowerCase().includes('application/json')){
   const messages={400:'The security token or request is invalid or expired. Reopen Select Clause before retrying.',401:'Your session has expired. Sign in again before retrying.',403:'You are not authorised to save selections for this lease.',404:'This property, draft or clause is unavailable.',409:'The lease changed. Reopen Select Clause before retrying.',503:'The lease service is unavailable. Please retry.'};
   throw Error(messages[response.status]||'The lease service returned an unexpected response (HTTP '+response.status+'). Please retry.');
  }
  let value;try{value=await response.json();}catch(error){throw Error('The lease service returned an invalid response. Please retry.');}
  if(!response.ok)throw Error(value.error||'The selection could not be saved (HTTP '+response.status+').');return value;
 }
 function render(){const visible=selections(),chosen=new Set(visible.map(entry=>key(entry)));get('search-results').replaceChildren();
  const query=form.elements.q.value.trim().toLocaleLowerCase();let count=0;
  for(const entry of results){if(query&&!([entry.reference,entry.title].join(' ').toLocaleLowerCase().includes(query)))continue;count++;const row=node('tr'),cell=node('td'),box=node('input');box.type='checkbox';box.checked=chosen.has(key(entry));box.disabled=busy||loading;box.setAttribute('aria-label','Select '+entry.reference);box.onchange=()=>select(entry,box.checked);cell.append(box);const reference=node('td');reference.textContent=entry.source_label+' — '+entry.reference+' — '+entry.title;row.append(cell,reference);get('search-results').append(row);}
  get('selected-count').textContent=visible.length+' selected — '+count+' of '+results.length+' provisions shown';get('selected').replaceChildren();
  for(const entry of visible){const item=node('li'),box=node('input');box.type='checkbox';box.checked=true;box.disabled=busy||loading;box.setAttribute('aria-label','Remove '+(entry.reference||'PDF page '+entry.page));box.onchange=()=>select(entry,false);item.append(box,link(entry.source_label+' — '+(entry.reference||'PDF page '+entry.page+' (text excerpt)'),entry.location_page||entry.page));get('selected').append(item);}
  get('retry').hidden=pending.size===0;get('retry').disabled=busy||loading;get('finish').setAttribute('aria-disabled',String(busy||pending.size>0));
 }
 async function select(entry,checked){if(busy||loading)return;const identifier=key(entry);pending.set(identifier,{entry,checked});
  if(state.selected.some(saved=>saved.id===identifier)===checked){pending.delete(identifier);render();status.textContent=pending.size?'There are unsaved selections. Retry saving.':'Selection unchanged.';return;}
  await save(entry,checked,identifier);
 }
 async function save(entry,checked,identifier){busy=true;render();const data=new FormData();data.set('action','select_lease_pdf');data.set('csrf_token',get('csrf').value);data.set('register_version',String(state.version));data.set('source_sha256',sourceHash);data.set('provision_id',checked?entry.id:identifier);data.set('selected',checked?'yes':'no');status.textContent='Saving selection...';const url=new URL(root.dataset.url,window.location.href);url.searchParams.set('action','select_lease_pdf');
  try{const response=await fetch(url.toString(),{method:'POST',credentials:'same-origin',headers:{'Accept':'application/json','X-CSRFToken':get('csrf').value},body:data,redirect:'manual'});const value=await jsonResponse(response);if(!value.state||!Array.isArray(value.state.selected)||!Number.isInteger(value.state.version))throw Error('The save response was incomplete. Please retry.');state=value.state;pending.delete(identifier);status.textContent=pending.size?'Saved. Other selections still need saving.':'Saved';}catch(error){status.textContent=error.message+' Your selections are retained but not saved. Use Retry Save or undo the change.';}finally{busy=false;render();}
 }
 get('retry').onclick=async()=>{if(busy||loading)return;for(const [id,change] of Array.from(pending)){await save(change.entry,change.checked,id);if(pending.has(id))break;}};
 form.onsubmit=event=>{event.preventDefault();render();};form.elements.q.oninput=()=>render();
 function acceptIndex(value){if(!Array.isArray(value.results)||!value.state)throw Error('The Clause Heading Index response is incomplete.');results=value.results;state=value.state;sourceHash=value.source_sha256;get('search-notices').replaceChildren();for(const notice of value.notices||[])get('search-notices').append(node('p',notice));loading=false;status.textContent='Clause Heading Index loaded.';render();}
 async function loadIndex(){loading=true;get('retry-index').hidden=true;render();const url=new URL(root.dataset.url,window.location.href);url.searchParams.set('action','retry_clause_index');status.textContent='Loading saved clause headings...';
  try{const data=new FormData();data.set('action','retry_clause_index');data.set('csrf_token',get('csrf').value);const response=await fetch(url.toString(),{method:'POST',credentials:'same-origin',headers:{'Accept':'application/json','X-CSRFToken':get('csrf').value},body:data,redirect:'manual'});acceptIndex(await jsonResponse(response));}
  catch(error){status.textContent=error.message;get('retry-index').hidden=false;}finally{loading=false;render();}
 }
 get('finish').onclick=event=>{if(busy||pending.size){event.preventDefault();status.textContent='Save or undo pending selections before leaving.';}};
 get('retry-index').onclick=()=>loadIndex();
 const initial=JSON.parse(get('initial').textContent);
 if(initial.index)acceptIndex({results:initial.index.entries,source_sha256:initial.index.source_sha256,notices:initial.index.notices,state});
 else{loading=false;status.textContent=initial.error||'The saved Clause Heading Index is unavailable. Use Retry Index.';get('retry-index').hidden=false;render();}
}());
