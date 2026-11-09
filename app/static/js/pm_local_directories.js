/* Browser-only handles. Never send directories or local paths to AIT. */
(() => {
  'use strict';
  const key=(scope,report)=>JSON.stringify([String(scope.user),String(scope.property),report]);
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
    async get(scope,report){check(scope);return operation('readonly',store=>store.get(key(scope,report)));},
    async set(scope,report,handle){check(scope);if(!handle || handle.kind!=='directory')throw new Error('Select a folder.');return operation('readwrite',store=>store.put({key:key(scope,report),user:String(scope.user),property:String(scope.property),report,handle}));},
    async remove(scope,report){check(scope);return operation('readwrite',store=>store.delete(key(scope,report)));},
    async list(scope){check(scope);return operation('readonly',store=>store.index('scope').getAll([String(scope.user),String(scope.property)]));},
    async permission(handle){
      const options={mode:'readwrite'};
      if(await handle.queryPermission(options)==='granted')return true;
      return await handle.requestPermission(options)==='granted';
    }
  };
  const titles={history:'Reading_History',municipal_submission:'Municipal_Reading_Submission',metro_reconciliation:'Metro_Reconciliation',utilities_reconciliation:'Utilities_Reconciliation',metsoa_statement:'METSOA',account_register:'Account_Register',arrangement_register:'Arrangement_Register'};
  api.save=async(scope,report,url,date=new Date())=>{
    if(!api.supported() || !navigator.locks)throw new Error('Direct folder saving is unavailable. Use Download PDF.');
    if(!titles[report])throw new Error('Select a supported report.');
    const row=await api.get(scope,report);
    if(!row)throw new Error('No directory is registered for this report. Open Directory Register or use Download PDF.');
    if(!await api.permission(row.handle))throw new Error('Folder permission was not granted. Allow access and try again, or use Download PDF.');
    const response=await fetch(url,{credentials:'same-origin',cache:'no-store'});
    if(!response.ok)throw new Error('The report could not be generated. Preview it again or use Download PDF.');
    const blob=await response.blob();
    if(await blob.slice(0,5).text()!=='%PDF-')throw new Error('The server did not return a PDF. No file was saved.');
    const pad=n=>String(n).padStart(2,'0');
    const prefix=pad(date.getDate())+pad(date.getMonth()+1)+pad(date.getFullYear()%100)+'_'+titles[report];
    // One origin-wide lock also covers different properties assigned the same
    // folder. Check for existing files while holding it; never reuse a name.
    return navigator.locks.request('ait-pm-local-report-save',async()=>{
      let number=1,name,file;
      while(true){
        name=prefix+(number===1?'':'('+number+')')+'.pdf';
        try{await row.handle.getFileHandle(name);number++;}
        catch(error){if(error.name!=='NotFoundError')throw error;break;}
      }
      file=await row.handle.getFileHandle(name,{create:true});
      if((await file.getFile()).size!==0)throw new Error('A file appeared with this name. Nothing was overwritten; try saving again.');
      const writer=await file.createWritable({mode:'exclusive'});
      try{await writer.write(blob);await writer.close();}
      catch(error){try{await writer.abort();}catch(_){}throw error;}
      return name;
    });
  };
  window.AITDirectories=api;
})();
