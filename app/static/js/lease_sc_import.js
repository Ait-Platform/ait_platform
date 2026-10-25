(function(){
 'use strict';
 const root=document.getElementById('sc-word-import');if(!root)return;
 const byId=id=>document.getElementById('sc-import-'+id),form=byId('form'),status=byId('status'),comparison=byId('comparison');
 let token=null,source=null,busy=false;
 function sync(){byId('confirm').disabled=busy||!token||!byId('approved').checked;}
 function reset(){token=null;source=null;comparison.replaceChildren();byId('approval').hidden=true;byId('approved').checked=false;sync();}
 function close(){if(busy)return;reset();form.reset();status.textContent='';root.close();}
 document.getElementById('sc-import-open').onclick=()=>{reset();status.textContent='';root.showModal();};
 byId('cancel').onclick=close;root.addEventListener('cancel',event=>{event.preventDefault();close();});
 form.elements.source.onchange=reset;byId('approved').onchange=sync;
 function content(parent,blocks,text){
  if(!blocks){const p=document.createElement('pre');p.textContent=text;p.style.whiteSpace='pre-wrap';parent.append(p);return;}
  blocks.forEach(block=>{if(block.kind==='table'){const table=document.createElement('table');block.rows.forEach((row,index)=>{const tr=document.createElement('tr');row.forEach(value=>{const cell=document.createElement(index===0?'th':'td');cell.textContent=value;cell.style.whiteSpace='pre-wrap';tr.append(cell);});table.append(tr);});parent.append(table);}else{const p=document.createElement('pre');p.textContent=block.text;p.style.whiteSpace='pre-wrap';parent.append(p);}});
 }
 async function result(response){let data;const fallback=response.clone?response.clone():null;try{data=await response.json();}catch(error){data={};if(fallback){const page=new DOMParser().parseFromString(await fallback.text(),'text/html');const detail=page.querySelector('p');if(detail)data.error=detail.textContent;}}if(!response.ok)throw new Error(data.error||('Request rejected ('+response.status+'). Reopen the selected saved draft and try again.'));return data;}
 async function submit(action){
  if(busy)return;if(action==='sc_import_confirm'&&(!token||!byId('approved').checked)){status.textContent='Approve the validated comparison before confirming.';return;}
  const submittedSource=action==='sc_import_preview'?form.elements.source.files[0]:source;
  const data=new FormData(form);data.set('action',action);if(action==='sc_import_confirm'){data.set('source',source);data.set('token',token);data.set('confirmed','yes');}
  busy=true;form.elements.source.disabled=true;sync();status.textContent='Processing...';
  try{
   const response=await fetch(root.dataset.url,{method:'POST',credentials:'same-origin',body:data,redirect:'error'});const value=await result(response);
   if(value.saved){window.location.assign(value.url);return;}
   reset();token=value.token;source=submittedSource;
   value.comparison.forEach(row=>{const section=document.createElement('section'),title=document.createElement('h3');title.textContent=row.number;section.append(title);[['Existing',row.existing_heading,row.existing_text,row.existing_blocks],['Proposed',row.proposed_heading,row.proposed_text,row.proposed_blocks]].forEach(([label,heading,text,blocks])=>{const h=document.createElement('h4');h.textContent=label+' — '+heading;section.append(h);content(section,blocks,text);});comparison.append(section);});
   byId('approval').hidden=false;status.textContent='Comparison validated. No saved data has changed.';
  }catch(error){reset();status.textContent=error.message||'Import was not confirmed. The original draft remains unchanged.';}
  finally{busy=false;form.elements.source.disabled=false;sync();}
 }
 form.onsubmit=event=>{event.preventDefault();return submit('sc_import_preview');};byId('confirm').onclick=()=>submit('sc_import_confirm');
 const mapForm=document.getElementById('lease-map-form');
 if(mapForm){mapForm.onsubmit=async event=>{event.preventDefault();const output=document.getElementById('lease-map-status'),button=mapForm.querySelector('button');if(button.disabled)return;button.disabled=true;output.textContent='Uploading private Map Annexure...';try{const response=await fetch(mapForm.dataset.url,{method:'POST',credentials:'same-origin',body:new FormData(mapForm),redirect:'error'});const value=await result(response);window.location.assign(value.url);}catch(error){output.textContent=error.message;}finally{button.disabled=false;}};}
}());
