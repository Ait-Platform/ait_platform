(function(){
 'use strict';
 const root=document.getElementById('sc-pdf-dialog');if(!root)return;
 const byId=id=>document.getElementById('sc-pdf-'+id),form=byId('form'),status=byId('status');let busy=false;
 function close(){if(!busy){form.reset();status.textContent='';root.close();}}
 byId('open').onclick=()=>{status.textContent='';root.showModal();};byId('cancel').onclick=close;
 root.addEventListener('cancel',event=>{event.preventDefault();close();});
 async function result(response){let value;const fallback=response.clone?response.clone():null;try{value=await response.json();}catch(error){value={};if(fallback){const page=new DOMParser().parseFromString(await fallback.text(),'text/html'),detail=page.querySelector('p');if(detail)value.error=detail.textContent;}}if(!response.ok)throw new Error(value.error||'Request rejected ('+response.status+').');return value;}
 form.onsubmit=async event=>{event.preventDefault();if(busy)return;const data=new FormData(form);data.set('action','sc_pdf_upload');busy=true;byId('upload').disabled=true;byId('cancel').disabled=true;status.textContent='Uploading and saving...';
  try{const value=await result(await fetch(root.dataset.url,{method:'POST',credentials:'same-origin',body:data,redirect:'error'}));if(!value.saved)throw new Error('Upload was not saved.');
   const output=byId('success');output.textContent=value.message+' ';const link=document.createElement('a');link.href=value.pdf_url;link.target='_blank';link.rel='noopener';link.textContent='Open uploaded PDF';output.appendChild(link);
   byId('word').href=value.word_url;byId('word').hidden=false;busy=false;close();
  }catch(error){status.textContent=error.message;}finally{busy=false;byId('upload').disabled=false;byId('cancel').disabled=false;}
 };
}());
