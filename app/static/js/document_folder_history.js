/* Historical files remain local; opening never regenerates or charges for a document. */
(() => {
  const refresh=document.getElementById('document-history-refresh');if(!refresh)return;
  const api=window.AITDirectories;
  const scope=JSON.parse((document.getElementById('output-directory-context') || document.getElementById('directory-context')).textContent);
  const reports=JSON.parse(document.getElementById('directory-reports').textContent);
  const message=document.getElementById('document-history-message'),list=document.getElementById('document-history-files');
  if(!scope.property){refresh.disabled=true;message.textContent='Select an authorised property first.';return;}
  if(!api.supported()){refresh.disabled=true;message.textContent='Local file browsing requires Chrome or Edge over HTTPS. Existing PDFs remain in your chosen folder and can be opened and printed there.';return;}
  refresh.addEventListener('click',async()=>{
    refresh.disabled=true;list.replaceChildren();
    try{
      const seen=new Set();
      for(const report of reports)for(const file of await api.list(scope,report.code)){
        const key=file.folder+'/'+file.name;if(seen.has(key))continue;seen.add(key);
        const row=document.createElement('li');row.className='flex items-center justify-between gap-3 rounded border border-slate-300 p-3';
        const label=document.createElement('span');label.textContent=key;row.append(label);
        const view=document.createElement('button');view.type='button';view.className='rounded bg-slate-100 px-3 py-2 font-semibold';view.textContent='View / Print';
        view.addEventListener('click',async()=>{
          const popup=window.open('','_blank');
          if(!popup){message.textContent='Allow pop-ups to open the PDF viewer.';return;}
          popup.opener=null;
          try{
            const data=await file.handle.getFile();
            if(await data.slice(0,5).text()!=='%PDF-')throw new Error('This file is not a valid PDF. No file was changed.');
            const url=URL.createObjectURL(new Blob([data],{type:'application/pdf'}));popup.location.href=url;
            setTimeout(()=>URL.revokeObjectURL(url),60000);
            message.textContent='Opened '+file.name+'. Use the PDF viewer’s Print control to print it.';
          }catch(error){popup.close();message.textContent=error.message;}
        });row.append(view);list.append(row);
      }
      message.textContent=seen.size?seen.size+' saved PDF(s). Files and filenames are unchanged.':'No saved PDFs found in this account’s configured folders.';
    }catch(error){message.textContent=error.message;}finally{refresh.disabled=false;}
  });
})();
