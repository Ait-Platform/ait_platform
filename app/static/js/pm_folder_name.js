(() => {
  'use strict';
  const button=document.getElementById('directory-select');
  if(!button)return;
  const name=document.getElementById('directory-folder'),message=document.getElementById('directory-message');
  const scope=JSON.parse(document.getElementById('directory-context').textContent),api=window.AITDirectories;
  let registered=null,writable=false,notice='';
  let assignmentVersion=0,loaded=false;
  button.disabled=true;
  function selectionState(){button.hidden=!!registered;button.disabled=!loaded || !scope.property;}
  function status(){
    const destination=registered?'Existing report destination: '+registered.name+(writable?'.':' (no current write access). Use Download PDF.'):'No authorised report folder is assigned. Use Download PDF.';
    message.textContent=[notice,destination].filter(Boolean).join(' ');
  }
  // Only restore the current user's property assignment; never replace it from
  // a FileList or prompt for permission while loading the page.
  if(scope.property){const version=assignmentVersion;api.get(scope).then(async row=>{
    if(row && (String(row.user)!==String(scope.user) || String(row.property)!==String(scope.property) || row.handle?.kind!=='directory'))throw new Error('Invalid assignment');
    const handle=row?.handle || null;
    let allowed=false;
    if(handle)try{allowed=await handle.queryPermission({mode:'readwrite'})==='granted';}catch(_){}
    if(version!==assignmentVersion)return;
    registered=handle;writable=allowed;loaded=true;selectionState();
    if(registered)name.textContent=registered.name;
    status();
    if(registered && writable && navigator.locks){
      try{await navigator.locks.request('ait-pm-local-report-save',async()=>{
        const saved=await api.get(scope);
        if(!saved || !await saved.handle.isSameEntry(registered))return;
        await api.initialise(scope,registered,{program:'pm',reports:JSON.parse(document.getElementById('directory-reports').textContent)});
      });}catch(_){notice='Some report subfolders could not be checked or created. Existing files are unchanged.';status();}
    }
  }).catch(()=>{if(version===assignmentVersion){notice='Could not read the existing assignment.';status();}});}
  async function separateFromOtherProperties(handle){
    for(const option of document.getElementById('directory-property').options){
      if(!option.value || option.value===String(scope.property))continue;
      const other=await api.get({user:scope.user,property:option.value});
      if(!other?.handle)continue;
      if(await other.handle.isSameEntry(handle) || await other.handle.resolve(handle)!==null || await handle.resolve(other.handle)!==null){
        throw new Error('Choose a separate folder. This destination overlaps the assignment for '+option.textContent+'.');
      }
    }
  }
  api.bindSelection({scope,button,label:name,message,
    ready:()=>loaded && !registered && !!scope.property,
    inspect:separateFromOtherProperties,
    registration:{program:'pm',reports:JSON.parse(document.getElementById('directory-reports').textContent),
      validate:separateFromOtherProperties,
      existing:async row=>{
        registered=row.handle;name.textContent=registered.name;
        writable=await registered.queryPermission({mode:'readwrite'})==='granted';selectionState();
        throw new Error('This property already has a folder assignment. It has been preserved.');
      }
    },
    success:row=>{assignmentVersion++;registered=row.handle;writable=true;name.textContent=registered.name;
      notice='Writable folder assigned. PM report subfolders are ready. Save a report from Utilities Output to verify the complete connection.';selectionState();status();}
  });
})();
