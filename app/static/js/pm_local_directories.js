/* Browser-only handles. Never send directories or local paths to AIT. */
(() => {
  'use strict';
  const templates=new Map();
  const diagnosticStep=(diagnostic,label,work)=>diagnostic?diagnostic.run(label,work):work();
  async function* diagnosticEntries(handle,diagnostic,label){
    const iterator=handle.entries()[Symbol.asyncIterator]();
    try{while(true){const result=await diagnosticStep(diagnostic,label,()=>iterator.next());if(result.done)return;yield result.value;}}
    finally{if(iterator.return)await diagnosticStep(diagnostic,label+' (close iterator)',()=>iterator.return());}
  }
  const key=scope=>JSON.stringify([String(scope.user),String(scope.property),'__main__']);
  function reports(){
    const configured=document.getElementById('directory-reports');
    let values;
    if(configured)values=JSON.parse(configured.textContent);
    else{
      const catalog=JSON.parse(document.getElementById('output-catalog').textContent),sources=JSON.parse(document.getElementById('output-sources').textContent);
      values=Object.entries(catalog).flatMap(([source,items])=>items.map(([code,title])=>({code,title,source:sources[source]})));
    }
    const programme=document.getElementById('directory-program');
    if(!programme)return values; // Existing legacy Output adapters remain compatible.
    const template=templates.get(JSON.parse(programme.textContent));
    if(!template)throw new Error('Unsupported folder programme.');
    return template.documents({reports:values});
  }
  const marker='.ait-folder-owner.json';
  function safeSegment(name){
    if(typeof name!=='string' || !name || name==='.' || name==='..' || /[<>:"/\\|?*\x00-\x1f]/.test(name))throw new Error('Invalid document directory.');
    return name;
  }
  async function ownership(handle,diagnostic){
    try{
      const file=await diagnosticStep(diagnostic,'Owner marker: getFileHandle',()=>handle.getFileHandle(marker));
      const data=await diagnosticStep(diagnostic,'Owner marker: getFile',()=>file.getFile());
      return JSON.parse(await diagnosticStep(diagnostic,'Owner marker: read text',()=>data.text()));
    }catch(error){if(error.name==='NotFoundError')return null;throw error;}
  }
  async function ownedFolder(parent,name,owner,kind,id,create,diagnostic){
    const run=(label,work)=>diagnosticStep(diagnostic,kind+' directory: '+label,work);
    const expected={program:owner.program,user:String(owner.user),kind,id:String(id)};
    const matches=value=>value && Object.entries(expected).every(([key,item])=>value[key]===item);
    // Find by stable identity, including after a display-name change. Never rename files.
    try{const candidate=await run('open named directory',()=>parent.getDirectoryHandle(safeSegment(name)));if(matches(await ownership(candidate,diagnostic)))return candidate;}
    catch(error){if(error.name!=='NotFoundError' && error.name!=='TypeMismatchError')throw error;}
    const entries=diagnostic?diagnosticEntries(parent,diagnostic,kind+' directory: enumerate next entry'):parent.entries();
    for await(const [,candidate] of entries)if(candidate.kind==='directory' && matches(await ownership(candidate,diagnostic)))return candidate;
    if(!create)return null;
    let index=0,folder;
    while(true){
      const candidate=index===0?name:name+' ('+owner.program+'-'+id+(index===1?'':'-'+index)+')';
      try{await run('check name collision',()=>parent.getDirectoryHandle(safeSegment(candidate)));index++;}
      catch(error){if(error.name!=='NotFoundError' && error.name!=='TypeMismatchError')throw error;if(error.name==='TypeMismatchError'){index++;continue;}folder=await run('create directory',()=>parent.getDirectoryHandle(candidate,{create:true}));break;}
    }
    const file=await run('open owner marker',()=>folder.getFileHandle(marker,{create:true}));
    const writer=await run('create owner-marker writer',()=>file.createWritable({mode:'exclusive'}));
    try{await run('write owner marker',()=>writer.write(JSON.stringify(expected)));await run('close owner-marker writer',()=>writer.close());}
    catch(error){try{await run('abort owner-marker writer',()=>writer.abort());}catch(_){}throw error;}
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
    if(entry.directories){
      if(!Array.isArray(entry.directories) || !entry.directories.length)throw new Error('Invalid report destination.');
      entry.directories.forEach(safeSegment);
      let folder=handle;
      try{for(const segment of entry.directories)folder=await folder.getDirectoryHandle(segment,{create});}
      catch(error){if(!create && error.name==='NotFoundError')return null;throw error;}
      return {handle:folder,path:entry.directories.join('/')};
    }
    try{const source=await handle.getDirectoryHandle(safeSegment(entry.source),{create});const folder=await source.getDirectoryHandle(safeSegment(entry.title),{create});return {handle:folder,path:[source.name,folder.name].join('/')};}
    catch(error){if(!create && error.name==='NotFoundError')return null;throw error;}
  }
  async function destination(handle,report,scope){
    const entry=reports().find(item=>item.code===report);
    if(!entry)throw new Error('Select a supported report.');
    return (await resolve(handle,entry,scope,true)).handle;
  }
  // Optional local diagnostics. Observer failures must never affect storage.
  function observe(observer,event,details={}){if(typeof observer==='function')try{observer(event,details);}catch(_){}}
  function observedError(error){return {name:error?.name || 'Error',message:error?.message || ''};}
  function database(observer){return new Promise((resolve,reject)=>{
    observe(observer,'indexeddb-open-start');
    const request=indexedDB.open('ait-pm-directories',1);
    request.onupgradeneeded=()=>{const store=request.result.createObjectStore('assignments',{keyPath:'key'});store.createIndex('scope',['user','property']);};
    request.onsuccess=()=>{observe(observer,'indexeddb-open-complete');resolve(request.result);};
    request.onerror=()=>{observe(observer,'indexeddb-open-failed',observedError(request.error));reject(request.error);};
    request.onblocked=()=>{observe(observer,'indexeddb-open-blocked');reject(new Error('Close other AIT tabs and try again.'));};
  });}
  async function operation(mode,work,observer){
    const db=await database(observer);
    try{return await new Promise((resolve,reject)=>{
      const tx=db.transaction('assignments',mode);let result;
      observe(observer,'indexeddb-transaction-start',{mode});
      const req=work(tx.objectStore('assignments'));
      req.onsuccess=()=>{result=req.result;observe(observer,'indexeddb-request-success',{mode});};
      tx.oncomplete=()=>{observe(observer,'indexeddb-transaction-complete',{mode});resolve(result);};
      tx.onerror=()=>{observe(observer,'indexeddb-transaction-error',{mode,...observedError(tx.error)});reject(tx.error);};
      tx.onabort=()=>{observe(observer,'indexeddb-transaction-abort',{mode,...observedError(tx.error)});reject(tx.error || new Error('Local storage operation cancelled.'));};
    });}finally{db.close();}
  }
  function check(scope){if(!scope.user || !scope.property)throw new Error('Select an authorised property first.');}
  function notify(scope,type){
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
    async get(scope,observer){check(scope);observe(observer,'indexeddb-read-key',{key:key(scope)});return operation('readonly',store=>store.get(key(scope)),observer);},
    async set(scope,handle,observer,diagnostic){
      check(scope);if(!handle || handle.kind!=='directory')throw new Error('Select a folder.');
      if(!await api.permission(handle,diagnostic))throw new Error('Folder permission was not granted. The previous main folder is unchanged.');
      observe(observer,'indexeddb-write-key',{key:key(scope)});
      const result=await operation('readwrite',store=>{
        observe(observer,'indexeddb-put-start');
        return store.put({key:key(scope),user:String(scope.user),property:String(scope.property),handle});
      },observer);
      notify(scope,'folder');return result;
    },
    async reauthorise(scope){
      const row=await api.get(scope);if(!row)throw new Error('Select a main folder first.');
      if(!await api.permission(row.handle))throw new Error('Folder permission was not granted.');
      return row.handle.name;
    },
    async permission(handle,diagnostic){
      const options={mode:'readwrite'};
      if(await diagnosticStep(diagnostic,'Permission: queryPermission(readwrite)',()=>handle.queryPermission(options))==='granted')return true;
      return await diagnosticStep(diagnostic,'Permission: requestPermission(readwrite)',()=>handle.requestPermission(options))==='granted';
    }
  };

  // Shared selection/registration core. Programme adapters retain their UI and
  // supply authorised scope, display metadata and programme-specific checks.
  api.droppedDirectory=async event=>{
    const items=Array.from(event.dataTransfer.items).filter(item=>item.kind==='file');
    if(items.length!==1 || typeof items[0].getAsFileSystemHandle!=='function')throw new Error('This browser cannot authorise a dropped folder. Use Download PDF.');
    // Obtain the promise inside the native drop event, before any await.
    const handle=await items[0].getAsFileSystemHandle();
    if(handle?.kind!=='directory')throw new Error('Drop one folder, not a file.');
    return handle;
  };
  api.registerTemplate=(program,template)=>{
    if(!/^[a-z][a-z0-9_-]*$/.test(program) || typeof template.initial!=='function')throw new Error('Invalid folder template.');
    if(templates.has(program))throw new Error('Folder template is already registered.');
    if(template.documents!==undefined && typeof template.documents!=='function')throw new Error('Invalid document template.');
    templates.set(program,{...template,documents:template.documents || (context=>context.reports)});
  };
  // Central programme templates. All names and ownership come from the page's
  // authenticated database context, never from a filesystem path or user input.
  api.registerTemplate('pm',{initial:context=>{
    if(!context.reports?.length)throw new Error('The PM report catalogue is unavailable.');
    return context.reports.filter(report=>!report.create_on_save);
  }});
  api.registerTemplate('protrade',{shopDirectories:()=>['Invoice','Statement'],initial:context=>[
    {owned:{name:context.consultant,kind:'user',id:String(context.scope.user)}}
  ]});
  api.initialise=async(scope,handle,options)=>{
    check(scope);
    const template=templates.get(options.program);
    if(!template)throw new Error('Unsupported folder programme.');
    if(await handle.queryPermission({mode:'readwrite'})!=='granted')throw new Error('Write access was not granted.');
    for(const entry of template.initial({...options,scope})){
      if(entry.owned){
        const item=entry.owned;
        if(item.kind!=='user' || String(item.id)!==String(scope.user))throw new Error('Invalid consultant ownership.');
        await ownedFolder(handle,safeSegment(item.name),{program:options.program,user:String(scope.user)},item.kind,item.id,true);
      }else{
        if(entry.owner && (entry.owner.program!==options.program || String(entry.owner.user)!==String(scope.user)))throw new Error('Invalid folder ownership.');
        if(!await resolve(handle,entry,scope,true))throw new Error('Report folder creation failed.');
      }
    }
  };
  api.register=async(scope,handle,options)=>{
    check(scope);
    if(handle?.kind!=='directory')throw new Error('Select a folder.');
    if(!navigator.locks)throw new Error('Direct folder saving is unavailable. Use Download PDF.');
    // Keep permission approval in the caller's click, before storage/network waits.
    if(!await api.permission(handle) || await handle.queryPermission({mode:'readwrite'})!=='granted')throw new Error('Write access was not granted. The existing assignment is unchanged.');
    return navigator.locks.request('ait-pm-local-report-save',async()=>{
      await options.authorise?.();
      const existing=await api.get(scope);
      if(existing){
        if(String(existing.user)!==String(scope.user) || String(existing.property)!==String(scope.property) || existing.handle?.kind!=='directory')throw new Error('Could not verify the existing assignment.');
        await options.existing?.(existing);
        throw new Error('An existing assignment was preserved.');
      }
      await options.validate?.(handle);
      let stage='creation';
      try{
        await api.initialise(scope,handle,options);
        stage='storage';await api.set(scope,handle);
        stage='verification';await options.authorise?.();const saved=await api.get(scope);
        if(!saved || String(saved.user)!==String(scope.user) || String(saved.property)!==String(scope.property) || !await saved.handle.isSameEntry(handle))throw new Error('Assignment verification failed.');
        return saved;
      }catch(error){error.registrationStage=stage;throw error;}
    });
  };

  // Opt-in main-directory registration. Existing PM registration is unchanged.
  async function verifyWritableDirectory(handle,diagnostic){
    const run=(label,work)=>diagnosticStep(diagnostic,'Accessibility: '+label,work);
    if(await run('query read/write permission',()=>handle.queryPermission({mode:'readwrite'}))!=='granted')throw new Error('Read/write access is not granted.');
    const name='.ait-access-'+crypto.randomUUID()+'.tmp',value=crypto.randomUUID();
    let created=false;
    try{
      const file=await run('getFileHandle(create probe)',()=>handle.getFileHandle(name,{create:true}));created=true;
      const writer=await run('createWritable(probe)',()=>file.createWritable());
      try{await run('write probe',()=>writer.write(value));await run('close probe writer',()=>writer.close());}
      catch(error){try{await run('abort probe writer',()=>writer.abort());}catch(_){}throw error;}
      const readback=await run('getFile(probe)',()=>file.getFile());
      if(await run('read probe text',()=>readback.text())!==value)throw new Error('Directory read/write verification failed.');
      const entries=diagnostic?diagnosticEntries(handle,diagnostic,'Accessibility: enumerate next entry'):handle.entries();
      for await(const entry of entries){break;}
    }finally{if(created)await run('remove temporary probe',()=>handle.removeEntry(name));}
  }
  api.registerMainDirectory=async(scope,handle,options)=>{
    const d=options.diagnostic,run=(label,work)=>diagnosticStep(d,label,work),observer=d?.event;
    check(scope);
    if(!navigator.locks)throw new Error('Directory registration requires browser Web Locks.');
    // Request any needed permission in the selection gesture, before network waits.
    if(!await api.permission(handle,d))throw new Error('Read/write access was denied. The previous registration is unchanged.');
    return run('Registration: origin-wide lock request (includes time held)',()=>navigator.locks.request('ait-pm-local-report-save',async()=>{
      d?.log('LOCK ACQUIRED: main-directory registration');
      await run('Server: validate consultant before registration (no handle upload)',()=>options.authorise());
      const previous=await run('IndexedDB: read previous registration',()=>api.get(scope,observer));
      const expected=options.previous;
      if(!!previous!==!!expected || (previous && !await run('Registration: compare previous handle',()=>previous.handle.isSameEntry(expected.handle))))throw new Error('The registration changed in another tab. Reload before replacing it.');
      const program=await run('Program directory: getDirectoryHandle(create)',()=>handle.getDirectoryHandle(safeSegment(options.programName),{create:true}));
      await run('Program directory: verify actual read/write access',()=>verifyWritableDirectory(program,d));
      await run('Server: revalidate consultant before IndexedDB storage',()=>options.authorise());
      await run('IndexedDB: store directory handle and commit transaction',()=>api.set(scope,handle,observer,d));
      const saved=await run('IndexedDB: read back stored directory handle',()=>api.get(scope,observer));
      if(!saved || !await run('IndexedDB: verify stored handle isSameEntry',()=>saved.handle.isSameEntry(handle)))throw new Error('Registration could not be verified. Reload before continuing.');
      await run('Program directory: verify physical existence',()=>saved.handle.getDirectoryHandle(options.programName,{create:false}));
      return saved;
    }));
  };
  api.createRegisteredShopDirectories=async(scope,options)=>{
    const d=options.diagnostic,run=(label,work)=>diagnosticStep(d,label,work);
    check(scope);
    if(!navigator.locks)throw new Error('Directory creation requires browser Web Locks.');
    return run('Shop creation: origin-wide lock request (includes time held)',()=>navigator.locks.request('ait-pm-local-report-save',async()=>{
      d?.log('LOCK ACQUIRED: new-shop directory creation');
      const context=await run('Server: validate signed new-shop event',()=>options.authorise());
      if(!context.shop)throw new Error('No newly registered shop is authorised for folder creation.');
      const row=await run('IndexedDB: read main directory for new shop',()=>api.get(scope,d?.event));
      if(!row)throw new Error('Shop registered. Select and authorise a main directory to create its folders.');
      if(await run('Shop creation: query main-directory permission',()=>row.handle.queryPermission({mode:'readwrite'}))!=='granted')throw new Error('Shop registered. Main directory access is unavailable. Authorise or replace it before creating folders.');
      const program=await run('Shop creation: open program directory',()=>row.handle.getDirectoryHandle(safeSegment(context.program),{create:true}));
      await verifyWritableDirectory(program,d);
      const owner={program:context.programKey,user:String(scope.user)};
      const consultant=await run('Consultant directory: resolve stable owner',()=>ownedFolder(program,safeSegment(context.consultant),owner,'user',scope.user,true,d));
      const shop=await run('Shop directory: resolve stable owner',()=>ownedFolder(consultant,safeSegment(context.shop.name),owner,'client',context.shop.id,true,d));
      const template=templates.get(context.programKey);
      if(!template?.shopDirectories)throw new Error('The registered program directory template is unavailable.');
      for(const name of template.shopDirectories(context)){
        const child=await run(name+': create or reuse directory',()=>shop.getDirectoryHandle(name,{create:true}));
        await run(name+': verify read/write access',()=>verifyWritableDirectory(child,d));
        await run(name+': verify physical existence',()=>shop.getDirectoryHandle(name,{create:false}));
      }
      await run('Server: revalidate new-shop ownership',()=>options.authorise());
      await run('Verify program directory exists',()=>row.handle.getDirectoryHandle(program.name,{create:false}));
      await run('Verify consultant directory exists',()=>program.getDirectoryHandle(consultant.name,{create:false}));
      await run('Verify shop directory exists',()=>consultant.getDirectoryHandle(shop.name,{create:false}));
      return program.name+'/'+consultant.name+'/'+shop.name;
    }));
  };

  api.bindSelection=options=>{
    const dialog=document.getElementById('directory-modal'),zone=document.getElementById('directory-drop-zone');
    const status=document.getElementById('directory-modal-message'),close=document.getElementById('directory-modal-close');
    const browse=document.getElementById('directory-browse');
    const d=options.diagnostic,run=(label,work)=>diagnosticStep(d,label,work);
    let pending=null,busy=false,version=0;
    const notice=text=>{status.textContent=text;options.message.textContent=text;};
    function dismiss(){if(busy)return;version++;pending=null;dialog.close();}
    async function connect(){
      if(!pending || busy || !options.ready())return;
      busy=true;zone.setAttribute('aria-disabled','true');close.disabled=true;if(browse)browse.disabled=true;
      try{
        const saved=await run('Registration pipeline',()=>(options.register || api.register)(options.scope,pending,options.registration));
        pending=null;
        const completion=run('Completion callback',()=>options.success(saved));
        if(d && completion?.catch)completion.catch(error=>d.log('Completion exception: '+error.name+': '+error.message));
        run('Modal closure',()=>dialog.close());
      }catch(error){
        d?.log('REGISTRATION EXCEPTION: '+error.name+': '+error.message);
        if(error.name==='SecurityError' && !error.registrationStage){
          notice('Folder received. Click here to allow saving, then approve the browser permission prompt.');zone.focus();
        }else{
          const detail=error.registrationStage==='verification'?'The assignment was stored but could not be verified. Reopen Folders before saving a report.':
            error.registrationStage==='storage'?'Report subfolders were created, but the assignment could not be saved. The previous saved folder is unchanged.':
            error.registrationStage==='creation'?'Folder creation could not finish; some subfolders may exist. The previous saved folder is unchanged.':error.message;
          notice(detail);if(error.registrationStage)pending=null;
          options.failure?.(error);
        }
      }finally{busy=false;zone.removeAttribute('aria-disabled');close.disabled=false;if(browse)browse.disabled=false;}
    }
    options.button.onclick=()=>{
      if(!options.ready())return;
      version++;pending=null;status.textContent=browse?'Click Browse for Folder or drop your main folder below. Empty folders are supported.':'Drag your main Windows folder into the area below. Empty folders are supported.';
      if(!window.isSecureContext || !window.indexedDB || !navigator.locks || typeof dialog.showModal!=='function'){notice('Folder setup is unavailable in this browser. Use Download PDF.');return;}
      dialog.showModal();zone.focus();
    };
    async function accept(handle,current){
      if(current!==version || !dialog.open)return;
      if(handle?.kind!=='directory')throw new Error('Select a directory, not a file.');
      d?.log('HANDLE RECEIVED: kind='+handle.kind+'; directory='+handle.name);
      await run('Selection: inspect directory handle',()=>options.inspect?.(handle));
      if(current!==version || !dialog.open)return;
      pending=handle;
      // Keep the registered label unchanged until verification succeeds in the
      // opt-in browse workflow. Legacy adapters retain their selection display.
      if(!browse)options.label.textContent=handle.name;
      const permission=await run('Selection: queryPermission(readwrite)',()=>handle.queryPermission({mode:'readwrite'}));
      d?.log('PERMISSION RESPONSE: '+permission+'; user activation='+!!navigator.userActivation?.isActive);
      if(current!==version || !dialog.open)return;
      if(permission==='granted' || navigator.userActivation?.isActive)await connect();
      else notice('Folder received. Click here to allow saving, then approve the browser permission prompt.');
    }
    if(browse)browse.onclick=async()=>{
      if(!dialog.open || busy || browse.disabled || !options.ready())return;
      const current=++version;pending=null;
      if(typeof window.showDirectoryPicker!=='function'){
        notice('Browse for Folder requires Chrome or Edge over HTTPS. You can also try dragging a folder into the area below.');return;
      }
      browse.disabled=true;
      try{
        // Invoke directly in the user click, before any asynchronous work.
        const handle=await run('Selection: showDirectoryPicker(readwrite)',()=>window.showDirectoryPicker({mode:'readwrite'}));
        await accept(handle,current);
      }catch(error){
        if(current===version && dialog.open){
          pending=null;
          d?.log('PICKER EXCEPTION: '+error.name+': '+error.message);
          notice(error.name==='AbortError'?'Folder selection cancelled. The registered main directory is unchanged.':api.folderError(error,'picker'));
        }
      }finally{browse.disabled=busy;}
    };
    zone.ondragover=event=>{if(Array.from(event.dataTransfer.types).includes('Files'))event.preventDefault();};
    zone.ondrop=async event=>{
      event.preventDefault();if(!dialog.open || busy || !options.ready())return;
      const current=++version;pending=null;
      try{await accept(await run('Selection: getAsFileSystemHandle from drop',()=>api.droppedDirectory(event)),current);}
      catch(error){d?.log('DROP EXCEPTION: '+error.name+': '+error.message);if(current===version)notice(error.message);}
    };
    zone.onclick=()=>connect();
    zone.onkeydown=event=>{if(event.key==='Enter' || event.key===' '){event.preventDefault();connect();}};
    close.onclick=dismiss;
    dialog.oncancel=event=>{event.preventDefault();dismiss();};
    dialog.addEventListener('close',()=>{version++;pending=null;});
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
