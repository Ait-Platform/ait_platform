(() => {
  'use strict';
  if(!document.getElementById('directory-register'))return;
  const scope=JSON.parse(document.getElementById('directory-context').textContent),api=window.AITDirectories;
  const folder=document.getElementById('directory-folder'),message=document.getElementById('directory-message'),select=document.getElementById('directory-select');
  let selected=null,busy=false;
  function say(text){message.textContent=text;}
  function controls(){select.disabled=busy || !scope.property || !api.supported();}
  function failure(error,stage){
    if(stage==='storage')return 'Could not remember this folder. Please select it again; the previous saved folder is unchanged.';
    if(error.name==='AbortError')return 'No folder was returned. Select a folder and confirm browser access.';
    if(error.name==='NotAllowedError' || stage==='permission')return 'Folder access was not allowed. Select the folder again and allow access.';
    if(error.name==='SecurityError')return 'Open the secure AIT page and click Select Folder directly.';
    return 'Could not select this folder. Please try again.';
  }
  select.onclick=async()=>{
    if(busy || !scope.property || !api.supported())return;
    if(navigator.userActivation && !navigator.userActivation.isActive){say('Click Select Folder directly in this tab.');return;}
    busy=true;controls();say('Select a folder in the browser.');let stage='picker';
    try{
      // Original PM picker options; no storage await before the genuine click.
      const handle=await window.showDirectoryPicker({mode:'readwrite',id:'ait-pm-reports'});
      if(!handle || handle.kind!=='directory')throw new TypeError('No directory handle');
      selected=handle;folder.textContent=handle.name;say('Saving folder...');
      stage='permission';
      if(await handle.requestPermission({mode:'readwrite'})!=='granted')throw new DOMException('Denied','NotAllowedError');
      stage='storage';await api.set(scope,handle);
      say('Folder saved.');
    }catch(error){say(failure(error,stage));}
    finally{busy=false;controls();}
  };
  controls();
  if(!scope.property)say('Register a property first.');
  else if(!api.supported())say('Folder selection needs desktop Chrome or Edge over HTTPS with browser storage enabled.');
  else api.get(scope).then(row=>{
    // Restore only; never request permissions or create directories on loading.
    if(row && !selected){selected=row.handle;folder.textContent=selected.name;}
  }).catch(()=>say('Could not restore the saved folder. Please select it again.'));
})();
