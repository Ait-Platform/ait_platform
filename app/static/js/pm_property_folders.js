/* New PM-only implementation. Not enabled on production pages until native
 * directory selection is verified. Never opens or migrates legacy databases. */
(() => {
  'use strict';
  const databaseName='ait-pm-property-folders-v1',storeName='propertyFolders',mounted=new WeakSet();
  function identity(scope){
    const userId=String(scope.userId),propertyId=String(scope.propertyId);
    if(!/^[1-9]\d*$/.test(userId) || !/^[1-9]\d*$/.test(propertyId))throw new Error('Select an authorised property.');
    return {userId,propertyId};
  }
  function available(){return window.isSecureContext && typeof window.showDirectoryPicker==='function' && !!window.indexedDB;}
  async function record(scope,handle){
    const owner=identity(scope);
    const database=await new Promise((resolve,reject)=>{
      let blocked=false;
      const request=indexedDB.open(databaseName,1);
      request.onupgradeneeded=()=>request.result.createObjectStore(storeName,{keyPath:['userId','propertyId']});
      request.onsuccess=()=>{if(blocked)request.result.close();else resolve(request.result);};
      request.onerror=()=>reject(request.error);
      request.onblocked=()=>{blocked=true;reject(new Error('Close other folder tabs and retry.'));};
    });
    try{return await new Promise((resolve,reject)=>{
      const transaction=database.transaction(storeName,handle?'readwrite':'readonly');
      const store=transaction.objectStore(storeName);
      const request=handle?store.put({...owner,directory:handle}):store.get([owner.userId,owner.propertyId]);
      let result;
      request.onsuccess=()=>{result=request.result;};
      transaction.oncomplete=()=>resolve(result);
      transaction.onerror=()=>reject(transaction.error);
      transaction.onabort=()=>reject(transaction.error || new Error('Folder storage was interrupted.'));
    });}finally{database.close();}
  }
  async function restore(scope){return (await record(scope))?.directory || null;}
  async function choose(scope){
    const owner=identity(scope);
    if(!available())throw new Error('Use a supported desktop browser over HTTPS.');
    // A genuine click must reach the browser before any database work.
    let directory;
    try{directory=await window.showDirectoryPicker({mode:'readwrite'});}
    catch(error){throw new Error('The browser rejected directory selection.',{cause:error});}
    if(directory?.kind!=='directory')throw new Error('The browser returned no folder.');
    const access={mode:'readwrite'};
    if(await directory.queryPermission(access)!=='granted' && await directory.requestPermission(access)!=='granted')throw new Error('Folder access was not allowed.');
    await record(owner,directory);
    const saved=await restore(owner);
    if(!saved || !await saved.isSameEntry(directory))throw new Error('The saved folder could not be verified.');
    return directory;
  }
  function segment(value){
    if(typeof value!=='string' || !value || value==='.' || value==='..' || /[<>:"/\\|?*\x00-\x1f]/.test(value))throw new Error('Invalid report folder.');
    return value;
  }
  async function saveReport(scope,report,url,periodEnd){
    const owner=identity(scope);
    if(!navigator.locks)throw new Error('Direct saving is unavailable. Use Download PDF.');
    const source=segment(report.source),title=segment(report.title);
    if(typeof periodEnd!=='string' || !/^\d{4}-\d{2}-\d{2}$/.test(periodEnd) || !Number.isFinite(Date.parse(periodEnd+'T00:00:00Z')) || new Date(periodEnd+'T00:00:00Z').toISOString().slice(0,10)!==periodEnd)throw new Error('Preview the reporting period again.');
    const destination=new URL(url,location.href);
    if(destination.origin!==location.origin)throw new Error('Use an AIT report URL.');
    const root=await restore(owner);
    if(!root)throw new Error('Select a folder for this property first.');
    const access={mode:'readwrite'};
    if(await root.queryPermission(access)!=='granted' && await root.requestPermission(access)!=='granted')throw new Error('Select the property folder again to allow access.');
    const response=await fetch(destination.href,{credentials:'same-origin',cache:'no-store',headers:{Accept:'application/json'}});
    if(!response.ok)throw new Error('The report could not be generated.');
    const pdf=await response.blob();
    if(await pdf.slice(0,5).text()!=='%PDF-')throw new Error('AIT did not return a PDF. No file was saved.');
    const stem=periodEnd.slice(8,10)+periodEnd.slice(5,7)+periodEnd.slice(2,4)+'_'+(report.code==='metsoa_statement'?'METSOA':title.replaceAll(' ','_'));
    // Preserve the origin-wide document lock during eventual rolling cutover;
    // no legacy assignment or picker logic is accessed by this implementation.
    return navigator.locks.request('ait-pm-local-report-save',async()=>{
      const category=await root.getDirectoryHandle(source,{create:true});
      const folder=await category.getDirectoryHandle(title,{create:true});
      let suffix=0,name;
      for(;;){
        name=stem+(suffix?'('+suffix+')':'')+'.pdf';
        try{await folder.getFileHandle(name);suffix++;}
        catch(error){if(error.name==='NotFoundError')break;throw error;}
      }
      const file=await folder.getFileHandle(name,{create:true});
      if((await file.getFile()).size)throw new Error('A file appeared with this name. Retry without overwriting it.');
      const writer=await file.createWritable({mode:'exclusive'});
      try{await writer.write(pdf);await writer.close();}
      catch(error){try{await writer.abort();}catch(_){}throw error;}
      return name;
    });
  }
  function mount(scope,elements){
    const owner=identity(scope);
    if(mounted.has(elements.select))return;
    mounted.add(elements.select);
    let changed=false,busy=false;
    elements.select.disabled=!available();
    if(!available())elements.message.textContent='Folder selection is unavailable in this browser.';
    elements.select.addEventListener('click',async()=>{
      if(busy)return;
      busy=true;elements.select.disabled=true;elements.message.textContent='Choose a folder.';
      try{
        const directory=await choose(owner);
        changed=true;elements.name.textContent=directory.name;elements.message.textContent='Folder saved.';
      }catch(error){elements.message.textContent=error.cause?.name==='AbortError'?'The browser did not provide a folder. Existing assignments are unchanged.':'Could not save this folder. Please try again.';}
      finally{busy=false;elements.select.disabled=!available();}
    });
    // Reads do not query/request permission, enumerate files or create folders.
    restore(owner).then(directory=>{if(directory && !changed)elements.name.textContent=directory.name;})
      .catch(()=>{elements.message.textContent='Could not restore this property folder.';});
  }
  window.PMPropertyFolders=Object.freeze({available,choose,restore,saveReport,mount});
})();
