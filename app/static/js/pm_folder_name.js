(() => {
  'use strict';
  const button=document.getElementById('directory-select');
  if(!button)return;
  const input=document.getElementById('directory-input'),name=document.getElementById('directory-folder'),message=document.getElementById('directory-message');
  if(!('webkitdirectory' in input)){
    button.disabled=true;message.textContent='Folder selection is unavailable in this browser.';return;
  }
  button.onclick=()=>{name.textContent='No folder selected';message.textContent='Choose a folder containing a file.';input.click();};
  input.onchange=()=>{
    const relativePath=input.files[0]?.webkitRelativePath || '';
    const folderName=relativePath.includes('/')?relativePath.split('/')[0]:'';
    name.textContent=folderName || 'No folder selected';
    message.textContent=folderName?'':'Folder name unavailable. Choose a folder containing a file.';
    // Retain neither the selected files nor their contents after reading the name.
    input.value='';
  };
  input.oncancel=()=>{message.textContent='Folder selection cancelled.';};
})();
