(() => {
  'use strict';
  if(!document.getElementById('directory-register'))return;
  const scope=JSON.parse(document.getElementById('directory-context').textContent),api=window.AITDirectories;
  const workspace=document.getElementById('directory-workspace'),folder=document.getElementById('directory-folder'),message=document.getElementById('directory-message');
  const select=document.getElementById('directory-select'),change=document.getElementById('directory-change'),reauthorise=document.getElementById('directory-reauthorise'),save=document.getElementById('directory-save');
  let busy=false,selected=null;
  function say(text){message.textContent=text;}
  function unavailable(){
    if(!window.isSecureContext)return 'Folder selection requires HTTPS. Open Property Management using its secure AIT address. Use Download PDF until then.';
    if(typeof window.showDirectoryPicker!=='function')return 'This browser does not support local folder selection. Use current desktop Chrome or Edge, or Download PDF in Utilities Output.';
    if(!window.indexedDB)return 'Browser storage is unavailable, so this browser cannot remember a property folder. Allow site storage or use Download PDF.';
    return '';
  }
  function controls(){
    select.disabled=change.disabled=busy || !scope.property || !api.supported();
    save.disabled=reauthorise.disabled=busy || !selected || !api.supported();
  }
  function view(){workspace.hidden=false;folder.textContent=selected?selected.name:'No folder selected';controls();}
  async function choose(){
    if(busy)return;
    const reason=unavailable();if(reason){say(reason);return;}
    if(navigator.userActivation && !navigator.userActivation.isActive){say('Folder selection needs a direct click. Click Select Folder again in the active Property Management tab.');return;}
    busy=true;controls();
    try{
      // Retain PM's original picker identity and read/write permission request.
      // Invoke before any storage/permission await so the click stays active.
      const handle=await window.showDirectoryPicker({mode:'readwrite',id:'ait-pm-reports'});
      if(!handle || handle.kind!=='directory')throw new TypeError('The browser returned no directory handle');
      selected=handle;view();say('Folder selected. Click Save to remember this folder on this browser.');
    }catch(error){say(api.folderError(error,'picker'));}
    finally{busy=false;controls();}
  }
  async function persist(){
    if(busy || !selected)return;
    busy=true;controls();let stage='permission';
    try{
      // Request permission directly from Save, before any IndexedDB awaits.
      if(await selected.requestPermission({mode:'readwrite'})!=='granted')throw new DOMException('Browser folder permission was not granted','NotAllowedError');
      stage='storage';await api.set(scope,selected);
      view();say('Folder saved. Folder access authorised. Existing files are unchanged; subfolders are created only when saving.');
    }catch(error){say(api.folderError(error,stage));}
    finally{busy=false;controls();}
  }
  select.onclick=change.onclick=choose;
  save.onclick=reauthorise.onclick=persist;
  document.getElementById('directory-view').onclick=view;
  document.getElementById('directory-close').onclick=()=>{workspace.hidden=true;};
  controls();
  if(!scope.property || !api.supported()){
    document.getElementById('directory-view').disabled=!scope.property || !window.indexedDB;
    say(!scope.property?'Register a property first.':unavailable());
  }else api.get(scope).then(row=>{if(row && !selected){selected=row.handle;view();}}).catch(error=>say(api.folderError(error,'restore')));
})();
