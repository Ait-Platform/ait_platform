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
  const marker='.ait-folder-owner.json';
  function safeSegment(name){
    if(typeof name!=='string' || !name || name==='.' || name==='..' || /[<>:"/\\|?*\x00-\x1f]/.test(name))throw new Error('Invalid document directory.');
    return name;
  }
  async function ownership(handle){
    try{return JSON.parse(await(await(await handle.getFileHandle(marker)).getFile()).text());}
    catch(error){if(error.name==='NotFoundError')return null;throw error;}
  }
  async function ownedFolder(parent,name,owner,kind,id,create){
    const expected={program:owner.program,user:String(owner.user),kind,id:String(id)};
    const matches=value=>value && Object.entries(expected).every(([key,item])=>value[key]===item);
    // Find by stable identity, including after a display-name change. Never rename files.
    try{const candidate=await parent.getDirectoryHandle(safeSegment(name));if(matches(await ownership(candidate)))return candidate;}
    catch(error){if(error.name!=='NotFoundError' && error.name!=='TypeMismatchError')throw error;}
    for await(const [,candidate] of parent.entries())if(candidate.kind==='directory' && matches(await ownership(candidate)))return candidate;
    if(!create)return null;
    let index=0,folder;
    while(true){
      const candidate=index===0?name:name+' ('+owner.program+'-'+id+(index===1?'':'-'+index)+')';
      try{await parent.getDirectoryHandle(safeSegment(candidate));index++;}
      catch(error){if(error.name!=='NotFoundError' && error.name!=='TypeMismatchError')throw error;if(error.name==='TypeMismatchError'){index++;continue;}folder=await parent.getDirectoryHandle(candidate,{create:true});break;}
    }
    const writer=await(await folder.getFileHandle(marker,{create:true})).createWritable({mode:'exclusive'});
    try{await writer.write(JSON.stringify(expected));await writer.close();}catch(error){try{await writer.abort();}catch(_){}throw error;}
    return folder;
  }
  async function resolve(handle,entry,scope,create){
    if(entry.path){
      if(entry.path.length!==3 || !entry.owner || String(entry.owner.user)!==String(scope.user))throw new Error('Document folder is outside this user account.');
      entry.path.forEach(safeSegment);
      const user=await ownedFolder(handle,entry.path[0],entry.owner,'user',scope.user,create);if(!user)return null;
      const client=await ownedFolder(user,entry.path[1],entry.owner,'client',entry.owner.client,create);if(!client)return null;
      try{const folder=await client.getDirectoryHandle(entry.path[2],{create});return {handle:folder,path:[user.name,client.name,folder.name].join('/')};}
      catch(error){if(!create && error.name==='NotFoundError')return null;throw error;}
    }
    try{const source=await handle.getDirectoryHandle(safeSegment(entry.source),{create});const folder=await source.getDirectoryHandle(safeSegment(entry.title),{create});return {handle:folder,path:[source.name,folder.name].join('/')};}
    catch(error){if(!create && error.name==='NotFoundError')return null;throw error;}
  }
  async function destination(handle,report,scope){
    const entry=reports().find(item=>item.code===report);
    if(!entry)throw new Error('Select a supported report.');
    return (await resolve(handle,entry,scope,true)).handle;
  }
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
  function notify(scope,type){
    if(String(scope.property)!=='protrade')return;
    const detail={user:String(scope.user),property:String(scope.property),type};
    window.dispatchEvent(new CustomEvent('ait:local-documents',{detail}));
    // The folder/file is already committed. Optional cross-tab messaging must
    // never report a completed Save as a failed write. This tab's event remains.
    if(window.BroadcastChannel){
      let channel;
      try{channel=new BroadcastChannel('ait-protrade-documents');channel.postMessage(detail);}
      catch(_){}finally{try{channel?.close();}catch(_){}}
    }
  }
  const api={
    folderError(error,stage){
      const detail=(error.name || 'Error')+': '+(error.message || 'No browser detail available');
      if(stage==='picker'){
        const reasons={
          AbortError:'The browser ended folder selection without returning a folder. Save and browser storage have not run. Click Select Folder, choose the intended report folder, then confirm any browser access prompt. AbortError does not distinguish a dismissed dialog from a folder the browser refuses to expose.',
          NotAllowedError:'Browser folder access was denied. Select the folder again and confirm its access prompt; if this site is blocked, review its folder permissions in browser Site settings.',
          SecurityError:'The browser blocked the folder picker for security reasons. Open the HTTPS AIT page directly in its own tab and click Select Folder yourself.',
          NotFoundError:'The selected folder is no longer available. Check that its drive is connected and select the folder again.',
          TypeError:'The browser did not return a usable directory handle or rejected the picker request. Retry in current desktop Chrome or Edge.'
        };
        return (reasons[error.name] || 'The folder picker failed before a directory handle was available.')+' '+detail+' The saved folder is unchanged.';
      }
      if(stage==='permission')return 'Folder permission was not granted. The selected folder is retained; click Save to retry. The previous saved folder is unchanged. '+detail;
      if(stage==='storage')return 'Browser storage could not save the folder. The selected folder is retained; click Save to retry. The previous saved folder is unchanged. '+detail;
      return 'The saved folder could not be restored from browser storage. '+detail;
    },
    supported:()=>window.isSecureContext && typeof window.showDirectoryPicker==='function' && !!window.indexedDB,
    async get(scope){check(scope);return operation('readonly',store=>store.get(key(scope)));},
    async set(scope,handle){
      check(scope);if(!handle || handle.kind!=='directory')throw new Error('Select a folder.');
      if(!await api.permission(handle))throw new Error('Folder permission was not granted. The previous main folder is unchanged.');
      const result=await operation('readwrite',store=>store.put({key:key(scope),user:String(scope.user),property:String(scope.property),handle}));
      notify(scope,'folder');return result;
    },
    async reauthorise(scope){
      const row=await api.get(scope);if(!row)throw new Error('Select a main folder first.');
      if(!await api.permission(row.handle))throw new Error('Folder permission was not granted.');
      return row.handle.name;
    },
    async permission(handle){
      const options={mode:'readwrite'};
      if(await handle.queryPermission(options)==='granted')return true;
      return await handle.requestPermission(options)==='granted';
    }
  };

  api.list=async(scope,report,options={})=>{
    const row=await api.get(scope);if(!row)throw new Error('Select Folder first.');
    const permitted=options.prompt===false?await row.handle.queryPermission({mode:'readwrite'})==='granted':await api.permission(row.handle);
    if(!permitted)throw new Error(String(scope.property)==='protrade'?'Folder access needs confirmation. Click Save for this folder.':'Folder permission expired. Click Allow Access and try again.');
    const entry=reports().find(item=>item.code===report);if(!entry)throw new Error('Select a supported report.');
    const folders=[],current=await resolve(row.handle,entry,scope,false);
    if(current)folders.push(current);
    // Previously saved ProTrade PDFs retain their original layout and names.
    if(entry.path){const legacy=await resolve(row.handle,{source:entry.source,title:entry.title},scope,false);if(legacy)folders.push(legacy);}
    if(entry.owner?.program==='protrade')for await(const [name,source] of row.handle.entries()){
      if(source.kind!=='directory' || !(name==='Shop-'+entry.owner.client || name.startsWith('Shop-'+entry.owner.client+'-')))continue;
      try{const folder=await source.getDirectoryHandle(entry.title);folders.push({handle:folder,path:source.name+'/'+folder.name});}
      catch(error){if(error.name!=='NotFoundError')throw error;}
    }
    const files=[];
    for(const folder of folders)for await(const [name,file] of folder.handle.entries()){
      if(file.kind==='file' && /\.pdf$/i.test(name))files.push({name,folder:folder.path,handle:file});
    }
    return files;
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
      const folder=await destination(row.handle,report,scope);
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
      notify(scope,'document');
      return name;
    });
  };
  window.AITDirectories=api;
})();
