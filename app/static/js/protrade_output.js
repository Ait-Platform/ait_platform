/* ProTrade uses the existing PM Folders API; folder handles stay in this browser. */
(() => {
  'use strict';
  const scope=JSON.parse(document.getElementById('output-directory-context').textContent);
  const folderName=document.getElementById('output-folder-name'),message=document.getElementById('output-save-message');
  const select=document.getElementById('output-folder-select'),allow=document.getElementById('output-folder-allow');
  const save=document.getElementById('output-save'),email=document.getElementById('output-email');
  const form=document.getElementById('protrade-output-select');
  allow.disabled=true;
  let selected=null,choosing=false;
  function fail(error){message.textContent='Could not save the folder or document: '+(error.message || error.name)+'. The previous saved folder is unchanged.';}
  if(!window.AITDirectories.supported()){
    select.disabled=true;allow.disabled=true;
    message.textContent='Direct folder saving requires Chrome or Edge on a secure connection. Save Report will download a uniquely named PDF instead.';
  }else window.AITDirectories.get(scope).then(row=>{if(row && !selected){selected=row.handle;folderName.textContent=row.handle.name;allow.disabled=choosing;}}).catch(error=>{message.textContent=window.AITDirectories.folderError(error,'restore');});
  select.addEventListener('click',async()=>{
    if(choosing)return;
    choosing=true;select.disabled=true;allow.disabled=true;
    try{
      // Selection needs read access only. Ask for write access on the separate
      // Save gesture so a refused write prompt cannot discard the chosen name.
      const handle=await window.showDirectoryPicker({mode:'read',id:'ait-protrade-base'});
      selected=handle;folderName.textContent=handle.name;allow.disabled=false;
      message.textContent='Folder selected. Click Save to remember this folder on this browser.';
    }catch(error){
      message.textContent=window.AITDirectories.folderError(error,'picker');
    }finally{choosing=false;select.disabled=false;allow.disabled=!selected;}
  });
  allow.addEventListener('click',async()=>{
    if(!selected){message.textContent='Choose a folder using Select Folder, then click Save.';return;}
    const handle=selected;allow.disabled=true;select.disabled=true;let stage='permission';
    try{
      // Invoke permission from this explicit user gesture, before storage awaits.
      if(await handle.requestPermission({mode:'readwrite'})!=='granted')throw new Error('Browser folder permission was not granted');
      stage='storage';await window.AITDirectories.set(scope,handle);
      folderName.textContent=handle.name;message.textContent='Folder saved. Documents will appear automatically when available.';
    }catch(error){message.textContent=window.AITDirectories.folderError(error,stage);}finally{allow.disabled=false;select.disabled=false;}
  });
  let saving=false;
  save?.addEventListener('click',async()=>{
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
  email?.addEventListener('click',()=>{const target=document.getElementById('output-email-form');target.hidden=!target.hidden;email.setAttribute('aria-expanded',String(!target.hidden));});
  const emailForm=document.getElementById('output-email-form');
  if(emailForm)emailForm.addEventListener('submit',()=>{emailForm.querySelector('button[type=submit]').disabled=true;});
  document.getElementById('output-print')?.addEventListener('click',()=>window.print());
  document.getElementById('output-shop')?.addEventListener('change',()=>{form.querySelector('[name=selection]')?.removeAttribute('required');window.location.href='/mechanic/output?shop_id='+encodeURIComponent(document.getElementById('output-shop').value);});
  for(const id of ['output-selection','output-recipient'])document.getElementById(id)?.addEventListener('change',()=>{
    save.disabled=true;email.disabled=true;document.getElementById('output-print').disabled=true;
    message.textContent='Selection changed. Click Preview before printing, saving or emailing.';
    const target=document.getElementById('output-email-form');if(target)target.hidden=true;
    const download=document.getElementById('output-download');if(download)download.hidden=true;
  });
})();
