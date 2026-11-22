/* ProTrade adapter over the existing AIT directory store and owner markers. */
(() => {
  'use strict';
  const config=document.getElementById('directory-context');if(!config)return;
  const scope=JSON.parse(config.textContent),reports=JSON.parse(document.getElementById('directory-reports').textContent),api=window.AITDirectories;
  const button=document.getElementById('directory-select'),label=document.getElementById('directory-folder'),message=document.getElementById('directory-message');
  let registered=null,loaded=false,readable=false;
  const destinations=new Set(),rows=document.getElementById('folder-register-rows');
  const registerButton=document.getElementById('folder-register-open'),empty=document.getElementById('protrade-register-empty');
  // Server metadata is an ownership allowlist, not evidence of saved folders.
  // Project the unchanged PM register onto destinations actually found on disk.
  function actualRows(){
    let number=0;
    for(const row of Array.from(rows.children)){
      const code=row.dataset.destination || reports[Number(row.children[0]?.textContent)-1]?.code;
      if(!registered || !destinations.has(code)){row.remove();continue;}
      row.dataset.destination=code;row.children[0].textContent=String(++number);
    }
    empty.hidden=!registered || !readable || number>0;
  }
  new MutationObserver(actualRows).observe(rows,{childList:true});
  async function authorise(){
    const endpoint=JSON.parse(document.getElementById('protrade-folder-auth').textContent).url;
    let value;
    try{const response=await fetch(endpoint,{credentials:'same-origin',cache:'no-store',redirect:'error'});
      if(!response.ok)throw Error('Access unavailable');value=await response.json();
    }catch(_){destinations.clear();readable=false;actualRows();throw Error('Consultant access could not be verified. Sign in again before accessing local files.');}
    if(String(value.user)!==String(scope.user) || !Array.isArray(value.shops) || reports.some(report=>!value.shops.includes(String(report.shop)))){destinations.clear();readable=false;actualRows();throw Error('Consultant or business access changed. Reopen ProTrade Folders.');}
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
      name:root.name,kind:'directory',async queryPermission(options){const result=await root.queryPermission(options);readable=result==='granted';return result;},requestPermission:options=>root.requestPermission(options),
      async getDirectoryHandle(name,options){
        if(options?.create)throw Error('Folder Register cannot create directories.');
        const next=node.children.get(name);if(!next)throw new DOMException('Unknown business destination.','NotFoundError');
        if(!next.report)return nodeHandle(next);
        const entry=next.report;
        try{
          const consultant=await owned(root,'user',scope.user),business=await owned(consultant,'client',entry.owner.client);
          const folder=await business.getDirectoryHandle(entry.path[2],{create:false});
          destinations.add(entry.code);return folder;
        }catch(error){if(error.name==='NotFoundError')destinations.delete(entry.code);throw error;}
      }
    };}
    return nodeHandle(tree);
  }
  // Only this ProTrade page receives a read-only tree facade. The original save
  // helper retains its original private API and stable-identity write resolver.
  window.AITDirectories=Object.assign({},api,{async get(value){validate(value);const row=await record();return row?{...row,handle:readableRoot(row.handle)}:null;}});
  function state(){button.hidden=!!registered;button.disabled=!loaded;registerButton.disabled=!registered;}
  button.disabled=true;
  record().then(row=>{registered=row?.handle || null;loaded=true;if(registered)label.textContent=registered.name;state();message.textContent=registered?'Assigned main folder restored. Existing assignments cannot be replaced here.':'Select Folder to authorise your main ProTrade folder.';}).catch(()=>{message.textContent='The saved assignment could not be read. It has not been changed.';});
  api.bindSelection({scope,button,label,message,ready:()=>loaded && !registered,
    registration:{program:'protrade',consultant:scope.consultant,authorise,
      existing:row=>{registered=row.handle;label.textContent=registered.name;state();}
    },
    success:row=>{registered=row.handle;label.textContent=registered.name;state();
      message.textContent='Main folder assigned. Consultant folder ready. Business and document-type folders are created only on first save.';}
  });
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
})();
