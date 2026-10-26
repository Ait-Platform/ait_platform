(function(){
 'use strict';const root=document.getElementById('addendum-pdf-selection');if(!root)return;
 const get=name=>document.getElementById('lease-pdf-'+name),form=get('search'),status=get('search-status'),viewer=document.getElementById('lease-selection-document');
 let state=JSON.parse(get('state').textContent),results=[],sourceHash='',busy=false,loading=true,pageTimer;const pending=new Map();
 const node=(tag,text)=>{const item=document.createElement(tag);if(text!==undefined)item.textContent=text;return item;};
 function page(number,tries=0){clearTimeout(pageTimer);const input=document.getElementById('lease-selection-page');if(input&&!input.disabled&&input.onchange){input.value=number;input.onchange();return;}if(tries<150)pageTimer=setTimeout(()=>page(number,tries+1),100);else status.textContent='The PDF is not ready. Retry the page link after it has loaded.';}
 function link(label,number){const item=node('a',label);item.href='#lease-selection-document';item.style.textDecoration='underline';item.onclick=event=>{event.preventDefault();page(number);viewer.scrollIntoView({block:'start'});};return item;}
 function key(entry){return entry.pdf_sha256?entry.id:sourceHash+':'+entry.id;}
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
  for(const entry of results){if(query&&!([entry.reference,entry.title,entry.search_text||entry.excerpt].join(' ').toLocaleLowerCase().includes(query)))continue;count++;const row=node('tr'),cell=node('td'),box=node('input');box.type='checkbox';box.checked=chosen.has(sourceHash+':'+entry.id);box.disabled=busy||loading;box.setAttribute('aria-label','Select '+(entry.reference||'excerpt')+' on PDF page '+entry.page);box.onchange=()=>select(entry,box.checked);cell.append(box);const location=node('td');location.append(link(String(entry.page),entry.page));const reference=node('td');reference.append(link(entry.title||entry.reference||'PDF page '+entry.page+' (text excerpt)',entry.page));row.tabIndex=0;row.onclick=event=>{if(event.target&&['INPUT','A'].includes(event.target.tagName))return;page(entry.page);viewer.scrollIntoView({block:'start'});};row.onkeydown=event=>{if(event.key==='Enter'){event.preventDefault();page(entry.page);}};row.append(cell,location,reference);get('search-results').append(row);}
  get('selected-count').textContent=visible.length+' selected — '+count+' of '+results.length+' provisions shown';get('selected').replaceChildren();
  for(const entry of visible){const item=node('li'),box=node('input');box.type='checkbox';box.checked=true;box.disabled=busy||loading;box.setAttribute('aria-label','Remove '+(entry.reference||'PDF page '+entry.page));box.onchange=()=>select(entry,false);item.append(box,link(entry.source_label+' — '+(entry.reference||'PDF page '+entry.page+' (text excerpt)'),entry.location_page||entry.page));get('selected').append(item);}
  get('retry').hidden=pending.size===0;get('retry').disabled=busy||loading;get('finish').setAttribute('aria-disabled',String(busy||loading||pending.size>0));
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
 async function loadIndex(){loading=true;render();const url=new URL(root.dataset.url,window.location.href);url.searchParams.set('action','clause_index');status.textContent='Loading the Full Lease Clause Index...';
  try{const response=await fetch(url.toString(),{credentials:'same-origin',headers:{'Accept':'application/json'},cache:'no-store',redirect:'manual'});const value=await jsonResponse(response);if(!Array.isArray(value.results)||!value.state)throw Error('The Clause Index response is incomplete. No entries have been substituted.');results=value.results;state=value.state;sourceHash=value.source_sha256;get('search-notices').replaceChildren();for(const notice of value.notices)get('search-notices').append(node('p',notice));
   const pdf=new URL(root.dataset.url,window.location.href);pdf.searchParams.set('action','lease_pdf');pdf.searchParams.set('source_sha256',sourceHash);viewer.dispatchEvent(new CustomEvent('lease-pdf-load',{detail:{url:pdf.toString()}}));const initial=Number(root.dataset.page);if(Number.isInteger(initial)&&initial>1)page(initial);status.textContent='Clause Index loaded. Select provisions or filter the list.';
  }catch(error){results=[];status.textContent=error.message;}finally{loading=false;render();}
 }
 get('finish').onclick=event=>{if(busy||loading||pending.size){event.preventDefault();status.textContent='Wait for loading to finish and save or undo pending selections before leaving.';}};
 window.addEventListener('pagehide',()=>clearTimeout(pageTimer));loadIndex();
}());
