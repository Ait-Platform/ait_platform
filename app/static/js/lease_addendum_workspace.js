(function(){
 'use strict';
 const root=document.getElementById('addendum-workspace');if(!root)return;
 const byId=id=>document.getElementById('addendum-'+id),form=byId('create-form'),clauseRoot=byId('clauses'),viewer=byId('pdf-document'),status=byId('view-status');
 let state=JSON.parse(byId('clause-data').textContent),general=!!byId('wording').value.trim(),previewBusy=false,pendingPreview=false,active='select',blobUrl=null,sequence=0;
 const panels=Array.from(root.querySelectorAll('[data-addendum-panel]')),buttons=Array.from(document.querySelectorAll('[data-addendum-workspace]'));
 function heading(){const groups=[];for(const source of ['certificate','core','special_conditions','signature']){const entries=state.selected.filter(entry=>entry.source===source);if(entries.length)groups.push(entries[0].source_label+' — '+Array.from(new Set(entries.map(entry=>entry.reference))).join(', '));}
  byId('notice').textContent=groups.length?'NOTICE REGARDING CHANGES TO CLAUSES:':'GENERAL ADDENDUM TO AGREEMENT OF LEASE';byId('reference-list').textContent=groups.join('; ');
 }
 function show(name){active=name;panels.forEach(panel=>panel.hidden=panel.dataset.addendumPanel!==name);buttons.forEach(button=>button.setAttribute('aria-pressed',String(button.dataset.addendumWorkspace===name)));if(name==='create'){const prompt=!state.selected.length&&!general;byId('general-question').hidden=!prompt;form.hidden=prompt;}heading();}
 function load(url){viewer.dispatchEvent(new CustomEvent('lease-pdf-load',{detail:{url}}));}
 async function preview(){show('view');if(previewBusy){pendingPreview=true;return;}
  if(!byId('wording').value.trim()){const saved=Array.from(root.querySelectorAll('[data-addendum-file]')).filter(link=>link.dataset.addendumLease===state.lease_key).at(-1);if(saved){load(saved.href);status.textContent='Saved Addendum';}else status.textContent='Enter Amendment Content in Create Addendum to preview the complete document.';return;}
  const current=++sequence,data=new FormData(form);data.set('action','addendum_preview');previewBusy=true;status.textContent='Preparing Addendum preview...';
  try{const response=await fetch(root.dataset.url,{method:'POST',credentials:'same-origin',body:data,redirect:'error'});if(!response.ok){let message='Addendum preview could not be prepared ('+response.status+').';try{message=(await response.json()).error||message;}catch(error){}throw Error(message);}if(!(response.headers.get('Content-Type')||'').includes('application/pdf'))throw Error('The preview response was not a PDF.');
   const blob=await response.blob();if(current!==sequence)return;if(blobUrl)URL.revokeObjectURL(blobUrl);blobUrl=URL.createObjectURL(blob);load(blobUrl);status.textContent='Preview only. The original Lease Agreement and saved Addendums are unchanged.';
  }catch(error){if(current===sequence)status.textContent=error.message;}finally{previewBusy=false;if(pendingPreview){pendingPreview=false;if(active==='view')preview();}}
 }
 buttons.forEach(button=>button.onclick=()=>button.dataset.addendumWorkspace==='view'?preview():show(button.dataset.addendumWorkspace));
 byId('return-select').onclick=()=>show('select');byId('continue-general').onclick=()=>{general=true;show('create');};
 clauseRoot.addEventListener('addendum-selection-change',event=>{state=event.detail;sequence++;heading();});
 ['subject','wording'].forEach(name=>byId(name).addEventListener('input',()=>{sequence++;}));
 root.querySelectorAll('[data-addendum-file]').forEach(link=>link.onclick=event=>{event.preventDefault();sequence++;load(link.href);show('view');status.textContent='Saved Addendum';});
 window.addEventListener('pagehide',()=>{if(blobUrl)URL.revokeObjectURL(blobUrl);});
 heading();if(root.dataset.initial==='view')preview();else show(root.dataset.initial==='create'?'create':'select');
}());
