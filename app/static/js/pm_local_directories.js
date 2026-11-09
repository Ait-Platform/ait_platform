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

  api.save=async(scope,report,url,date=new Date())=>{
    if(!api.supported() || !navigator.locks)throw new Error('Direct folder saving is unavailable. Use Download PDF.');
    const entry=reports().find(item=>item.code===report);
    if(!entry)throw new Error('Select a supported report.');
    const row=await api.get(scope);
    if(!row)throw new Error('No main folder is registered. Open Folders or use Download PDF.');
    if(!await api.permission(row.handle))throw new Error('Folder permission was not granted. Allow access and try again, or use Download PDF.');
    const response=await fetch(url,{credentials:'same-origin',cache:'no-store'});
    if(!response.ok)throw new Error('The report could not be generated. Preview it again or use Download PDF.');
    const blob=await response.blob();
    if(await blob.slice(0,5).text()!=='%PDF-')throw new Error('The server did not return a PDF. No file was saved.');
    const pad=n=>String(n).padStart(2,'0');
    const prefix=pad(date.getDate())+pad(date.getMonth()+1)+pad(date.getFullYear()%100)+'_'+(report==='metsoa_statement'?'METSOA':entry.title.replaceAll(' ','_'));
    // One origin-wide lock also covers different properties assigned the same
    // folder. Check for existing files while holding it; never reuse a name.
    return navigator.locks.request('ait-pm-local-report-save',async()=>{
      const folder=await destination(row.handle,report);
      let number=1,name,file;
      while(true){
        name=prefix+(number===1?'':'('+number+')')+'.pdf';
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
