(function(){
 'use strict';
 const root=document.getElementById('sc-word-import');if(!root)return;
 const byId=id=>document.getElementById('sc-import-'+id),form=byId('form'),status=byId('status'),comparison=byId('comparison');
 let token=null,source=null,busy=false;
 function reset(){token=null;source=null;comparison.replaceChildren();byId('approval').hidden=true;byId('confirm').hidden=true;byId('approved').checked=false;}
 document.getElementById('sc-import-open').onclick=()=>{root.hidden=false;};
 byId('cancel').onclick=()=>{if(!busy){reset();root.hidden=true;form.reset();}};
 form.elements.source.onchange=reset;
 async function submit(action){
  if(busy)return;if(action==='sc_import_confirm'&&(!token||!byId('approved').checked)){status.textContent='Confirm the proposed wording before importing.';return;}
  const submittedSource=action==='sc_import_preview'?form.elements.source.files[0]:source;
  const data=new FormData(form);data.set('action',action);if(action==='sc_import_confirm'){data.set('source',source);data.set('token',token);data.set('confirmed','yes');}
  busy=true;form.elements.source.disabled=true;byId('confirm').disabled=true;status.textContent='Processing...';
  try{
   const response=await fetch(root.dataset.url,{method:'POST',credentials:'same-origin',body:data,redirect:'error'});
   if(!response.ok){token=null;byId('confirm').hidden=true;status.textContent='Import rejected. The original draft is unchanged. Check clause numbering and document structure, or compare again if the draft changed.';return;}
   const result=await response.json();if(result.saved){window.location.assign(result.url);return;}
   reset();token=result.token;source=submittedSource;
   result.comparison.forEach(row=>{const section=document.createElement('section'),title=document.createElement('h3');title.textContent=row.number;section.append(title);[['Existing',row.existing_heading,row.existing_text],['Proposed',row.proposed_heading,row.proposed_text]].forEach(([label,heading,text])=>{const h=document.createElement('h4'),p=document.createElement('pre');h.textContent=label+' — '+heading;p.textContent=text;p.style.whiteSpace='pre-wrap';section.append(h,p);});comparison.append(section);});
   byId('approval').hidden=false;byId('confirm').hidden=false;status.textContent='Comparison only. No saved data has changed.';
  }catch(error){token=null;byId('confirm').hidden=true;status.textContent='Import was not confirmed. The original draft remains unchanged.';}
  finally{busy=false;form.elements.source.disabled=false;byId('confirm').disabled=false;}
 }
 form.onsubmit=event=>{event.preventDefault();return submit('sc_import_preview');};byId('confirm').onclick=()=>submit('sc_import_confirm');
}());
