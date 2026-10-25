(function(){
 'use strict';
 const root=document.getElementById('sc-pdf-dialog');if(!root)return;
 const byId=id=>document.getElementById('sc-pdf-'+id),form=byId('form'),status=byId('status'),frame=byId('preview');
 let token=null,source=null,busy=false,previewed=false,blobUrl=null;
 function sync(){byId('confirm').disabled=busy||!token||!previewed||!byId('approved').checked;}
 function reset(){token=null;source=null;previewed=false;byId('approved').checked=false;byId('approval').hidden=true;frame.hidden=true;frame.removeAttribute('src');if(blobUrl)URL.revokeObjectURL(blobUrl);blobUrl=null;sync();}
 function close(){if(busy)return;reset();form.reset();status.textContent='';root.close();}
 byId('open').onclick=()=>{reset();status.textContent='';root.showModal();};byId('cancel').onclick=close;root.addEventListener('cancel',event=>{event.preventDefault();close();});
 form.elements.source.onchange=reset;byId('approved').onchange=sync;
 frame.onload=()=>{if(!token||!blobUrl)return;previewed=true;byId('approval').hidden=false;sync();};
 async function result(response){let value;const fallback=response.clone?response.clone():null;try{value=await response.json();}catch(error){value={};if(fallback){const page=new DOMParser().parseFromString(await fallback.text(),'text/html'),detail=page.querySelector('p');if(detail)value.error=detail.textContent;}}if(!response.ok)throw new Error(value.error||'Request rejected ('+response.status+').');return value;}
 async function submit(action){
  if(busy)return;if(action==='sc_pdf_confirm'&&(!token||!previewed||!byId('approved').checked)){status.textContent='Preview and approve the complete PDF before confirming.';return;}
  const file=action==='sc_pdf_preview'?form.elements.source.files[0]:source,data=new FormData(form);data.set('action',action);data.set('source',file);
  if(action==='sc_pdf_confirm'){data.set('token',token);data.set('confirmed','yes');}
  busy=true;form.elements.source.disabled=true;sync();status.textContent='Processing...';
  try{const value=await result(await fetch(root.dataset.url,{method:'POST',credentials:'same-origin',body:data,redirect:'error'}));if(value.saved){window.location.assign(value.url);return;}reset();token=value.token;source=file;blobUrl=URL.createObjectURL(new Blob([file],{type:'application/pdf'}));frame.hidden=false;frame.src=blobUrl;status.textContent='Preview all '+value.pages+' page(s), then approve and confirm to file. The current document is unchanged.';}
  catch(error){reset();status.textContent=error.message;}finally{busy=false;form.elements.source.disabled=false;sync();}
 }
 form.onsubmit=event=>{event.preventDefault();return submit('sc_pdf_preview');};byId('confirm').onclick=()=>submit('sc_pdf_confirm');
 const review=document.getElementById('lease-final-review-form');if(review){review.onsubmit=async event=>{event.preventDefault();const output=document.getElementById('lease-final-review-status'),button=review.querySelector('button');if(button.disabled)return;button.disabled=true;try{const value=await result(await fetch(root.dataset.url,{method:'POST',credentials:'same-origin',body:new FormData(review),redirect:'error'}));window.location.assign(value.url);}catch(error){output.textContent=error.message;}finally{button.disabled=false;}};}
 window.addEventListener('pagehide',()=>{if(blobUrl)URL.revokeObjectURL(blobUrl);});
}());
