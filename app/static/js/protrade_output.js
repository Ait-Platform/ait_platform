/* ProTrade uses the existing PM Folders API; folder handles stay in this browser. */
(() => {
  'use strict';
  const scope=JSON.parse(document.getElementById('output-directory-context').textContent);
  const folderName=document.getElementById('output-folder-name'),message=document.getElementById('output-save-message');
  const select=document.getElementById('output-folder-select'),allow=document.getElementById('output-folder-allow');
  const save=document.getElementById('output-save'),email=document.getElementById('output-email');
  const form=document.getElementById('protrade-output-select');
  function fail(error){message.textContent=error.name==='AbortError'?'Folder selection or saving cancelled.':error.message;}
  if(!window.AITDirectories.supported()){
    select.disabled=true;allow.disabled=true;
    message.textContent='Direct folder saving requires Chrome or Edge on a secure connection. Save Report will download a uniquely named PDF instead.';
  }else window.AITDirectories.get(scope).then(row=>{if(row)folderName.textContent=row.handle.name;}).catch(fail);
  select.addEventListener('click',async()=>{
    try{const handle=await window.showDirectoryPicker({mode:'readwrite'});await window.AITDirectories.set(scope,handle);folderName.textContent=handle.name;message.textContent='Base ProTrade folder selected. Reports will be organised by shop and document type.';}catch(error){fail(error);}
  });
  allow.addEventListener('click',async()=>{try{folderName.textContent=await window.AITDirectories.reauthorise(scope);message.textContent='Folder access granted.';}catch(error){fail(error);}});
  let saving=false;
  save.addEventListener('click',async()=>{
    if(saving || save.disabled)return;
    saving=true;save.disabled=true;
    try{
      if(window.AITDirectories.supported() && navigator.locks){
        const name=await window.AITDirectories.save(scope,scope.report,scope.pdf_url);
        message.textContent='Saved '+name+' in the selected ProTrade folder, under this shop and document type.';
      }else{
        const response=await fetch(scope.pdf_url,{credentials:'same-origin',cache:'no-store'});
        if(!response.ok)throw new Error('PDF generation failed (HTTP '+response.status+'). Preview again. No file was saved.');
        const blob=await response.blob();if(await blob.slice(0,5).text()!=='%PDF-')throw new Error('The server did not return a PDF. No file was saved.');
        const suffix=typeof crypto.randomUUID==='function'?crypto.randomUUID():Array.from(crypto.getRandomValues(new Uint8Array(16)),value=>value.toString(16).padStart(2,'0')).join('');
        const name=scope.filename.replace(/\.pdf$/i,'_'+suffix+'.pdf');
        const url=URL.createObjectURL(blob),link=document.createElement('a');link.href=url;link.download=name;document.body.appendChild(link);link.click();link.remove();setTimeout(()=>URL.revokeObjectURL(url),60000);
        message.textContent='PDF download started: '+name+'. Browser completion is not confirmed.';
      }
    }catch(error){fail(error);}finally{saving=false;save.disabled=false;}
  });
  email.addEventListener('click',()=>{const target=document.getElementById('output-email-form');target.hidden=!target.hidden;email.setAttribute('aria-expanded',String(!target.hidden));});
  const emailForm=document.getElementById('output-email-form');
  if(emailForm)emailForm.addEventListener('submit',()=>{emailForm.querySelector('button[type=submit]').disabled=true;});
  document.getElementById('output-print').addEventListener('click',()=>window.print());
  document.getElementById('output-shop').addEventListener('change',()=>{form.querySelector('[name=selection]')?.removeAttribute('required');window.location.href='/mechanic/output?shop_id='+encodeURIComponent(document.getElementById('output-shop').value);});
  for(const id of ['output-selection','output-recipient'])document.getElementById(id)?.addEventListener('change',()=>{
    save.disabled=true;email.disabled=true;document.getElementById('output-print').disabled=true;
    message.textContent='Selection changed. Click Preview before printing, saving or emailing.';
    const target=document.getElementById('output-email-form');if(target)target.hidden=true;
    const download=document.getElementById('output-download');if(download)download.hidden=true;
  });
})();
