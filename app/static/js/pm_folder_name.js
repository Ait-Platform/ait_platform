(() => {
  'use strict';
  const button=document.getElementById('directory-select');
  if(!button)return;
  const name=document.getElementById('directory-folder'),message=document.getElementById('directory-message');
  if(!window.isSecureContext || typeof window.showDirectoryPicker!=='function'){
    button.disabled=true;message.textContent='Folder selection is unavailable in this browser.';return;
  }
  button.onclick=async()=>{
    button.disabled=true;message.textContent='';
    try{
      const folder=await window.showDirectoryPicker();
      name.textContent=folder.name;
    }catch(_){message.textContent='The browser did not return a folder.';}
    finally{button.disabled=false;}
  };
})();
