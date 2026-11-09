(() => {
  'use strict';
  if(!document.getElementById('directory-register'))return;
  const scope=JSON.parse(document.getElementById('directory-context').textContent),api=window.AITDirectories;
  const workspace=document.getElementById('directory-workspace'),folder=document.getElementById('directory-folder'),message=document.getElementById('directory-message');
  const select=document.getElementById('directory-select'),change=document.getElementById('directory-change'),reauthorise=document.getElementById('directory-reauthorise');
  let busy=false;
  function say(text){message.textContent=text;}
  async function view(){
    const row=await api.get(scope);workspace.hidden=false;folder.textContent=row?row.handle.name:'No folder selected';
    change.disabled=!api.supported();reauthorise.disabled=!row || !api.supported();
  }
  async function choose(){
    if(busy)return;busy=true;select.disabled=change.disabled=true;
    try{
      const handle=await window.showDirectoryPicker({mode:'readwrite',id:'ait-pm-reports'});
      await api.set(scope,handle);await view();say('Main folder registered. Source and report subfolders are ready.');
    }catch(error){if(error.name!=='AbortError')say(error.message);}
    finally{busy=false;select.disabled=change.disabled=!api.supported();}
  }
  select.onclick=change.onclick=choose;
  document.getElementById('directory-view').onclick=()=>view().catch(error=>say(error.message));
  document.getElementById('directory-close').onclick=()=>{workspace.hidden=true;};
  reauthorise.onclick=async()=>{if(busy)return;busy=true;reauthorise.disabled=true;try{await api.reauthorise(scope);say('Folder access authorised. Source and report subfolders are ready.');}catch(error){say(error.message);}finally{busy=false;reauthorise.disabled=false;}};
  if(!scope.property || !api.supported()){
    select.disabled=change.disabled=reauthorise.disabled=true;document.getElementById('directory-view').disabled=!scope.property || !window.indexedDB;
    say(!scope.property?'Register a property first.':'Direct folder saving requires Chrome or Edge over HTTPS. Use Download PDF in Utilities Output in this browser.');
  }
})();
