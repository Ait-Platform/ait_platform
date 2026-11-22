/* ProTrade adapter over the existing AIT directory store and owner markers. */
(() => {
  'use strict';
  const config=document.getElementById('directory-context');if(!config)return;
  const scope=JSON.parse(config.textContent),reports=JSON.parse(document.getElementById('directory-reports').textContent),api=window.AITDirectories;
  const button=document.getElementById('directory-select'),input=document.getElementById('directory-input'),label=document.getElementById('directory-folder'),message=document.getElementById('directory-message');
  let registered=null,pending=null,loaded=false,busy=false;
  async function authorise(){
    const endpoint=JSON.parse(document.getElementById('protrade-folder-auth').textContent).url;
    let value;
    try{const response=await fetch(endpoint,{credentials:'same-origin',cache:'no-store',redirect:'error'});
      if(!response.ok)throw Error('Access unavailable');value=await response.json();
    }catch(_){throw Error('Consultant access could not be verified. Sign in again before accessing local files.');}
    if(String(value.user)!==String(scope.user) || !Array.isArray(value.shops) || reports.some(report=>!value.shops.includes(String(report.shop))))throw Error('Consultant or business access changed. Reopen ProTrade Folders.');
  }
  function validate(value){if(String(value.user)!==String(scope.user) || String(value.property)!=='protrade')throw Error('Folder assignment is outside this consultant account.');}
  async function record(){await authorise();const value=await api.get(scope);if(value){validate(value);if(value.handle?.kind!=='directory')throw Error('Invalid saved directory.');}return value;}
  async function owned(parent,kind,id){
    for await(const [,handle] of parent.entries())if(handle.kind==='directory'){
      try{
        const value=JSON.parse(await(await(await handle.getFileHandle('.ait-folder-owner.json')).getFile()).text());
        if(value.program==='protrade' && value.user===String(scope.user) && value.kind===kind && value.id===String(id))return handle;
      }catch(error){if(error.name!=='NotFoundError' && !(error instanceof SyntaxError))throw error;}
    }
    throw new DOMException('No saved documents in this destination.','NotFoundError');
  }
  function readableRoot(root){
    const tree={children:new Map()};
    for(const report of reports){
      if(report.owner?.user!==String(scope.user) || report.owner?.program!=='protrade')throw Error('Invalid business ownership.');
      let node=tree;
      for(const segment of report.directories){if(!node.children.has(segment))node.children.set(segment,{children:new Map()});node=node.children.get(segment);}
      node.report=report;
    }
    function nodeHandle(node){return {
      name:root.name,kind:'directory',queryPermission:options=>root.queryPermission(options),requestPermission:options=>root.requestPermission(options),
      async getDirectoryHandle(name,options){
        if(options?.create)throw Error('Folder Register cannot create directories.');
        const next=node.children.get(name);if(!next)throw new DOMException('Unknown business destination.','NotFoundError');
        if(!next.report)return nodeHandle(next);
        const entry=next.report;
        const consultant=await owned(root,'user',scope.user),business=await owned(consultant,'client',entry.owner.client);
        return business.getDirectoryHandle(entry.path[2],{create:false});
      }
    };}
    return nodeHandle(tree);
  }
  // Only this ProTrade page receives a read-only tree facade. The original save
  // helper retains its original private API and stable-identity write resolver.
  window.AITDirectories=Object.assign({},api,{async get(value){validate(value);const row=await record();return row?{...row,handle:readableRoot(row.handle)}:null;}});
  function state(){button.hidden=!!registered;button.disabled=!loaded || busy;}
  button.disabled=true;
  record().then(row=>{registered=row?.handle || null;loaded=true;if(registered)label.textContent=registered.name;state();message.textContent=registered?'Assigned main folder restored. Existing assignments cannot be replaced here.':'Select a folder name, or drag your main ProTrade folder onto Select Folder and click it to approve write access.';}).catch(()=>{message.textContent='The saved assignment could not be read. It has not been changed.';});
  button.ondragover=event=>{if(event.dataTransfer.types.includes('Files'))event.preventDefault();};
  button.ondrop=async event=>{
    event.preventDefault();if(!loaded || registered || busy)return;
    try{
      const items=Array.from(event.dataTransfer.items).filter(item=>item.kind==='file');
      if(items.length!==1 || !items[0].getAsFileSystemHandle)throw Error('Drop one supported directory.');
      const handle=await items[0].getAsFileSystemHandle();if(handle?.kind!=='directory')throw Error('Drop a folder, not a file.');
      pending=handle;label.textContent=handle.name;message.textContent='Folder received. Click Select Folder to approve and assign it. No document directories have been created.';
    }catch(error){message.textContent=error.message;}
  };
  button.onclick=async()=>{
    if(!loaded || registered || busy)return;
    if(!pending){input.click();return;}
    busy=true;state();const handle=pending;
    try{
      if(!navigator.locks)throw Error('Direct saving is unavailable. Download PDF remains available.');
      if(!await api.permission(handle))throw Error('Write access was not granted.');
      await navigator.locks.request('ait-pm-local-report-save',async()=>{
        const previous=await record();
        if(previous){registered=previous.handle;throw Error('An existing assignment was preserved.');}
        await api.set(scope,handle);const saved=await record();
        if(!saved || !await saved.handle.isSameEntry(handle))throw Error('The saved assignment could not be verified. Reopen Folders.');
        registered=saved.handle;message.textContent='Main folder assigned. Only saving a document creates its consultant, business and document-type folders.';
      });
    }catch(error){message.textContent=error.message;}
    finally{pending=null;busy=false;if(registered)label.textContent=registered.name;state();}
  };
  input.onchange=()=>{
    if(registered || !loaded){input.value='';return;}
    const path=input.files[0]?.webkitRelativePath || '',name=path.includes('/')?path.split('/')[0]:'';
    label.textContent=name || 'No folder selected';message.textContent=name?'Folder name identified only. Drag the folder onto Select Folder, then click it to authorise saving.':'Choose a folder containing a file to identify its name.';input.value='';
  };
  window.addEventListener('DOMContentLoaded',()=>{
    function protect(element){
      const original=element.onclick || element.onsubmit,submit=element.tagName==='FORM';
      const guarded=async event=>{
        event?.preventDefault();
        try{await authorise();return original(event);}
        catch(error){document.getElementById('folder-register-close').onclick();label.textContent='Folder unavailable';message.textContent=error.message;}
      };
      if(submit)element.onsubmit=guarded;else element.onclick=guarded;
    }
    for(const id of ['folder-view-print','folder-view-email','folder-view-back','folder-register-back','folder-view-email-form'])protect(document.getElementById(id));
  });
  input.oncancel=()=>{message.textContent='Folder selection cancelled. The existing assignment is unchanged.';};
})();
