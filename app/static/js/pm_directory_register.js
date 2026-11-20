(() => {
  'use strict';
  if(!document.getElementById('directory-register'))return;
  const scope=JSON.parse(document.getElementById('directory-context').textContent),api=window.AITDirectories;
  const folder=document.getElementById('directory-folder'),message=document.getElementById('directory-message'),select=document.getElementById('directory-select');
  let selected=null,busy=false;
  const debug=new URLSearchParams(location.search).get('folder_debug')==='1',records=[];
  let status='',attempt=0;
  function render(){message.textContent=status+(debug?'\n'+records.map(row=>JSON.stringify(row)).join('\n'):'');}
  function trace(event,details={}){
    if(!debug)return;
    const row={sequence:records.length?records[records.length-1].sequence+1:1,attempt,user:String(scope.user),property:String(scope.property),key:JSON.stringify([String(scope.user),String(scope.property),'__main__']),event,...details};
    records.push(row);if(records.length>100)records.shift();
    try{console.info('[PM folder diagnostic]',JSON.stringify(row));}catch(_){}
    render();
  }
  function observer(phase){return debug?(event,details)=>trace(event,{phase,...details}):undefined;}
  function say(text){status=text;render();}
  if(debug){window.AITPMFolderDiagnostics=records;message.style.whiteSpace='pre-wrap';
    trace('page-context',{secure:window.isSecureContext,apiAvailable:!!api,scripts:Array.from(document.scripts).map(s=>s.src.split('/').pop().split('?')[0]).filter(s=>/pm_|protrade|folder|director/.test(s))});}
  function controls(){select.disabled=busy || !scope.property || !api.supported();}
  function failure(error,stage){
    if(stage==='confirm')return 'Could not confirm the saved folder. Reopen this page before selecting it again.';
    if(stage==='storage')return 'Could not remember this folder. Please select it again; the previous saved folder is unchanged.';
    if(error.name==='NotAllowedError' || stage==='permission')return 'Folder access was not allowed. Select the folder again and allow access.';
    if(error.name==='AbortError')return 'No folder was returned. Select a folder and confirm browser access.';
    if(error.name==='SecurityError')return 'Open the secure AIT page and click Select Folder directly.';
    return 'Could not select this folder. Please try again.';
  }
  select.onclick=async function chooseFolder(){
    if(!busy)attempt++;trace('selection-handler-entered',{busy,apiUnchanged:window.AITDirectories===api,handlerUnchanged:select.onclick===chooseFolder});
    if(busy || !scope.property || !api.supported()){trace('selection-not-started');return;}
    if(navigator.userActivation && !navigator.userActivation.isActive){trace('selection-no-user-activation');say('Click Select Folder directly in this tab.');return;}
    busy=true;controls();say('Select a folder in the browser.');let stage='picker';
    try{
      // Original PM picker options; no storage await before the genuine click.
      trace('picker-open-requested',{mode:'readwrite',pickerId:'ait-pm-reports',active:navigator.userActivation?.isActive});
      let handle;
      try{handle=await window.showDirectoryPicker({mode:'readwrite',id:'ait-pm-reports'});}
      catch(error){trace('picker-rejected',{name:error.name,message:error.message});throw error;}
      trace('picker-resolved',{folderName:handle?.name,kind:handle?.kind});
      if(!handle || handle.kind!=='directory')throw new TypeError('No directory handle');
      selected=handle;folder.textContent=handle.name;say('Saving folder...');
      // Use the existing permission check; do not request an already-granted
      // permission a second time after the native picker.
      stage='permission';
      trace('permission-check-start');
      if(!await api.permission(handle))throw new DOMException('Denied','NotAllowedError');
      trace('permission-check-granted');
      stage='storage';trace('assignment-write-start');await api.set(scope,handle,observer('write'));
      trace('assignment-write-complete');
      stage='confirm';
      trace('readback-start');
      const saved=await api.get(scope,observer('readback'));
      const matches=!!saved && saved.user===String(scope.user) && saved.property===String(scope.property) && await saved.handle.isSameEntry(handle);
      trace('readback-result',{exists:!!saved,storedKey:saved?.key,storedUser:saved?.user,storedProperty:saved?.property,folderName:saved?.handle?.name,matches});
      if(!matches)throw new Error('Folder assignment verification failed');
      trace('selection-saved',{folderName:handle.name});
      say('Folder saved.');
    }catch(error){trace('exception',{stage,name:error.name,message:error.message});say(failure(error,stage));}
    finally{trace('selection-finished',{apiUnchanged:window.AITDirectories===api});busy=false;controls();}
  };
  controls();
  if(!scope.property)say('Register a property first.');
  else if(!api.supported())say('Folder selection needs desktop Chrome or Edge over HTTPS with browser storage enabled.');
  else {trace('restore-start');api.get(scope,observer('restore')).then(row=>{
    // Restore only; never request permissions or create directories on loading.
    trace('restore-result',{exists:!!row,storedKey:row?.key,folderName:row?.handle?.name,origin:'previously-stored',applied:!!row && !selected});
    if(row && !selected){selected=row.handle;folder.textContent=selected.name;}
  }).catch(error=>{trace('restore-exception',{name:error.name,message:error.message});say('Could not restore the saved folder. Please select it again.');});}
})();
