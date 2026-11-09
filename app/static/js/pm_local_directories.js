/* Browser-only handles. Never send directories or local paths to AIT. */
(() => {
  'use strict';
  const key=scope=>JSON.stringify([String(scope.user),String(scope.property),'__main__']);
  function reports(){
    const configured=document.getElementById('directory-reports');
    if(configured)return JSON.parse(configured.textContent);
    const catalog=JSON.parse(document.getElementById('output-catalog').textContent),sources=JSON.parse(document.getElementById('output-sources').textContent);
    return Object.entries(catalog).flatMap(([source,items])=>items.map(([code,title])=>({code,title,source:sources[source]})));
  }
  async function destination(handle,report){
    const entry=reports().find(item=>item.code===report);
    if(!entry)throw new Error('Select a supported report.');
    const source=await handle.getDirectoryHandle(entry.source,{create:true});
    return source.getDirectoryHandle(entry.title,{create:true});
  }
  async function structure(handle){for(const entry of reports())await destination(handle,entry.code);}
  function database(){return new Promise((resolve,reject)=>{
    const request=indexedDB.open('ait-pm-directories',1);
    request.onupgradeneeded=()=>{const store=request.result.createObjectStore('assignments',{keyPath:'key'});store.createIndex('scope',['user','property']);};
    request.onsuccess=()=>resolve(request.result);request.onerror=()=>reject(request.error);
    request.onblocked=()=>reject(new Error('Close other AIT tabs and try again.'));
  });}
  async function operation(mode,work){
    const db=await database();
    try{return await new Promise((resolve,reject)=>{
      const tx=db.transaction('assignments',mode);let result;
      const req=work(tx.objectStore('assignments'));
      req.onsuccess=()=>{result=req.result;};
      tx.oncomplete=()=>resolve(result);tx.onerror=()=>reject(tx.error);tx.onabort=()=>reject(tx.error || new Error('Local storage operation cancelled.'));
    });}finally{db.close();}
  }
  function check(scope){if(!scope.user || !scope.property)throw new Error('Select an authorised property first.');}
  const api={
    supported:()=>window.isSecureContext && typeof window.showDirectoryPicker==='function' && !!window.indexedDB,
    async get(scope){check(scope);return operation('readonly',store=>store.get(key(scope)));},
    async set(scope,handle){
      check(scope);if(!handle || handle.kind!=='directory')throw new Error('Select a folder.');
      if(!await api.permission(handle))throw new Error('Folder permission was not granted. The previous main folder is unchanged.');
      await structure(handle);
      return operation('readwrite',store=>store.put({key:key(scope),user:String(scope.user),property:String(scope.property),handle}));
    },
    async reauthorise(scope){
      const row=await api.get(scope);if(!row)throw new Error('Select a main folder first.');
      if(!await api.permission(row.handle))throw new Error('Folder permission was not granted.');
      await structure(row.handle);return row.handle.name;
    },
    async permission(handle){
      const options={mode:'readwrite'};
      if(await handle.queryPermission(options)==='granted')return true;
      return await handle.requestPermission(options)==='granted';
    }
  };

  async function fetchPdf(url){
    let response;
    try{response=await fetch(url,{credentials:'same-origin',cache:'no-store',headers:{Accept:'application/json'}});}
    catch(error){throw new Error('The PDF request could not reach AIT. Check your connection and try again. No file was saved.');}
    if(!response.ok){
      let detail='';
      try{
        if((response.headers.get('content-type') || '').includes('application/json')){const failure=await response.json();if(typeof failure.error==='string')detail=failure.error;}
        else{const document=new DOMParser().parseFromString(await response.text(),'text/html');detail=document.querySelector('[role="alert"]')?.textContent.trim() || '';}
      }catch(_){}
      const defaults={400:'The PDF request could not be validated. Preview the report again.',401:'Your session has expired. Sign in again, then Preview the report.',403:'Access to this report was denied.',409:'Report records changed after Preview. Preview the current records again.',502:'The PDF server gateway could not complete the request.',503:'The server could not prepare the PDF. Please contact the administrator.',504:'The PDF request timed out at the server gateway.'};
      throw new Error((detail || defaults[response.status] || 'The PDF request failed.')+' (HTTP '+response.status+'). No file was saved.');
    }
    const blob=await response.blob();
    if(await blob.slice(0,5).text()!=='%PDF-')throw new Error(response.redirected?'AIT redirected the PDF request. Your session may have expired; sign in and Preview again. No file was saved.':'AIT returned a page instead of a PDF. Check your session and Preview again. No file was saved.');
    return blob;
  }
  function filenamePrefix(report,date){
    const entry=reports().find(item=>item.code===report);
    if(!entry)throw new Error('Select a supported report.');
    const pad=n=>String(n).padStart(2,'0');
    const iso=date instanceof Date?date.getFullYear()+'-'+pad(date.getMonth()+1)+'-'+pad(date.getDate()):date;
    if(typeof iso!=='string' || !/^\d{4}-\d{2}-\d{2}$/.test(iso) || !Number.isFinite(Date.parse(iso+'T00:00:00Z')) || new Date(iso+'T00:00:00Z').toISOString().slice(0,10)!==iso)throw new Error('Preview the report again to refresh its reporting-period ending date.');
    return iso.slice(8,10)+iso.slice(5,7)+iso.slice(2,4)+'_'+(report==='metsoa_statement'?'METSOA':entry.title.replaceAll(' ','_'));
  }
  api.download=async(scope,report,url)=>{
    const name=filenamePrefix(report,scope.period_end)+'.pdf',blob=await fetchPdf(url);
    const objectUrl=URL.createObjectURL(blob),link=document.createElement('a');
    link.href=objectUrl;link.download=name;link.hidden=true;document.body.appendChild(link);
    try{link.click();}finally{link.remove();setTimeout(()=>URL.revokeObjectURL(objectUrl),60000);}
    return name;
  };
  api.save=async(scope,report,url,date=scope.period_end)=>{
    if(!api.supported() || !navigator.locks)throw new Error('Direct folder saving is unavailable. Use Download PDF.');
    const entry=reports().find(item=>item.code===report);
    if(!entry)throw new Error('Select a supported report.');
    const prefix=filenamePrefix(report,date);
    const row=await api.get(scope);
    if(!row)throw new Error('No main folder is registered. Open Folders or use Download PDF.');
    if(!await api.permission(row.handle))throw new Error('Folder permission was not granted. Allow access and try again, or use Download PDF.');
    const blob=await fetchPdf(url);
    // One origin-wide lock also covers different properties assigned the same
    // folder. Check for existing files while holding it; never reuse a name.
    return navigator.locks.request('ait-pm-local-report-save',async()=>{
      const folder=await destination(row.handle,report);
      let number=0,name,file;
      while(true){
        name=prefix+(number===0?'':'('+number+')')+'.pdf';
        try{await folder.getFileHandle(name);number++;}
        catch(error){if(error.name!=='NotFoundError')throw error;break;}
      }
      file=await folder.getFileHandle(name,{create:true});
      if((await file.getFile()).size!==0)throw new Error('A file appeared with this name. Nothing was overwritten; try saving again.');
      const writer=await file.createWritable({mode:'exclusive'});
      try{await writer.write(blob);await writer.close();}
      catch(error){try{await writer.abort();}catch(_){}throw error;}
      return name;
    });
  };
  window.AITDirectories=api;
})();
