(() => {
  'use strict';
  const button=document.getElementById('directory-select');
  if(!button)return;
  const input=document.getElementById('directory-input'),name=document.getElementById('directory-folder'),message=document.getElementById('directory-message');
  const scope=JSON.parse(document.getElementById('directory-context').textContent),api=window.AITDirectories;
  let attempted=false,selectedName='',registered=null,writable=false,notice='';
  let dropped=null,connecting=false,dropVersion=0,assignmentVersion=0,loaded=false;
  button.disabled=true;
  function selectionState(){button.hidden=!!registered;button.disabled=!loaded || connecting || !scope.property || !('webkitdirectory' in input);}
  function status(){
    const destination=registered?'Existing report destination: '+registered.name+(writable?'.':' (no current write access). Use Download PDF.'):'No authorised report folder is assigned. Use Download PDF.';
    message.textContent=[notice,selectedName?'This folder name does not grant write access or change the report destination.':'',destination,!registered && loaded?'To authorise a destination, drag its folder onto Select Folder, then click that button.':''].filter(Boolean).join(' ');
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
    if(registered && !attempted)name.textContent=registered.name;
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
  button.ondragover=event=>{if(Array.from(event.dataTransfer.types).includes('Files'))event.preventDefault();};
  button.ondrop=async event=>{
    event.preventDefault();if(connecting || !loaded || registered || !scope.property)return;
    const version=++dropVersion;attempted=true;dropped=null;
    try{
      const handle=await api.droppedDirectory(event);
      await separateFromOtherProperties(handle);
      if(version!==dropVersion)return;
      dropped=handle;selectedName='';name.textContent=handle.name;
      notice='Folder received, not yet assigned. Click Select Folder to approve write access and assign it to this property.';status();
    }catch(error){if(version===dropVersion){notice=error.message;status();}}
  };
  async function connect(){
    if(connecting || !loaded || registered || !dropped)return;
    const handle=dropped;connecting=true;button.disabled=true;assignmentVersion++;
    try{
      const saved=await api.register(scope,handle,{
        program:'pm',reports:JSON.parse(document.getElementById('directory-reports').textContent),
        validate:separateFromOtherProperties,
        existing:async row=>{
          registered=row.handle;name.textContent=registered.name;selectedName='';
          writable=await registered.queryPermission({mode:'readwrite'})==='granted';
          throw new Error('This property already has a folder assignment. It has been preserved.');
        }
      });
      registered=saved.handle;writable=true;selectedName='';name.textContent=handle.name;
      notice='Writable folder assigned. PM report subfolders are ready. Save a report from Utilities Output to verify the complete connection.';
    }catch(error){
      notice=error.registrationStage==='verification'?'The assignment was stored but could not be verified. Reopen Folders before saving a report.':
        error.registrationStage==='storage'?'Report subfolders were created, but the assignment could not be saved. The previous assignment is unchanged.':
        error.registrationStage==='creation'?'Report folder creation could not finish; some subfolders may exist. The previous assignment is unchanged.':error.message;
    }finally{dropped=null;connecting=false;selectionState();status();}
  }
  if(!('webkitdirectory' in input)){
    button.disabled=true;message.textContent='Folder selection is unavailable in this browser.';return;
  }
  button.onclick=()=>{if(!loaded || registered)return;if(dropped)return connect();dropVersion++;attempted=true;selectedName='';name.textContent='No folder selected';notice='Choose a folder containing a file.';status();api.selectFolderName(input);};
  input.onchange=()=>{
    if(!loaded || registered){input.value='';return;}
    const folderName=api.selectedFolderName(input);
    name.textContent=folderName || 'No folder selected';
    selectedName=folderName;notice=folderName?'':'Folder name unavailable. Choose a folder containing a file.';status();
    // Retain neither the selected files nor their contents after reading the name.
    input.value='';
  };
  input.oncancel=()=>{notice='Folder selection cancelled.';status();};
})();
