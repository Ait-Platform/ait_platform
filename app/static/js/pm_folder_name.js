(() => {
  'use strict';
  const button=document.getElementById('directory-select');
  if(!button)return;
  const input=document.getElementById('directory-input'),name=document.getElementById('directory-folder'),message=document.getElementById('directory-message');
  const scope=JSON.parse(document.getElementById('directory-context').textContent),api=window.AITDirectories;
  let attempted=false,selectedName='',registered=null,writable=false,notice='';
  function status(){
    const destination=registered?'Existing report destination: '+registered.name+(writable?'.':' (no current write access). Use Download PDF.'):'No authorised report folder is assigned. Use Download PDF.';
    message.textContent=[notice,selectedName?'This folder name does not grant write access or change the report destination.':'',destination].filter(Boolean).join(' ');
  }
  // Only restore the current user's property assignment; never replace it from
  // a FileList or prompt for permission while loading the page.
  if(scope.property)api.get(scope).then(async row=>{
    registered=row?.handle || null;
    if(registered && !attempted)name.textContent=registered.name;
    if(registered)try{writable=await registered.queryPermission({mode:'readwrite'})==='granted';}catch(_){writable=false;}
    status();
  }).catch(()=>{notice='Could not read the existing assignment.';status();});
  if(!('webkitdirectory' in input)){
    button.disabled=true;message.textContent='Folder selection is unavailable in this browser.';return;
  }
  button.onclick=()=>{attempted=true;selectedName='';name.textContent='No folder selected';notice='Choose a folder containing a file.';status();input.click();};
  input.onchange=()=>{
    const relativePath=input.files[0]?.webkitRelativePath || '';
    const folderName=relativePath.includes('/')?relativePath.split('/')[0]:'';
    name.textContent=folderName || 'No folder selected';
    selectedName=folderName;notice=folderName?'':'Folder name unavailable. Choose a folder containing a file.';status();
    // Retain neither the selected files nor their contents after reading the name.
    input.value='';
  };
  input.oncancel=()=>{notice='Folder selection cancelled.';status();};
})();
