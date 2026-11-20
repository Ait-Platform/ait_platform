(() => {
  'use strict';
  const open=document.getElementById('folder-register-open');if(!open)return;
  const area=document.getElementById('folder-register-area'),close=document.getElementById('folder-register-close');
  const rows=document.getElementById('folder-register-rows'),message=document.getElementById('folder-register-message');
  const scope=JSON.parse(document.getElementById('directory-context').textContent);
  const reports=JSON.parse(document.getElementById('directory-reports').textContent);
  let revision=0;
  function render(states){
    rows.replaceChildren();
    reports.forEach((report,index)=>{
      const row=document.createElement('tr');row.className='border-t border-slate-200';
      for(const value of [String(index+1),report.source+' / '+report.title,states[index]]){
        const cell=document.createElement('td');cell.className='p-2';cell.textContent=value;row.appendChild(cell);
      }
      rows.appendChild(row);
    });
  }
  close.onclick=()=>{revision++;area.hidden=true;open.setAttribute('aria-expanded','false');open.focus();};
  open.onclick=async()=>{
    const current=++revision;
    area.hidden=false;open.setAttribute('aria-expanded','true');message.textContent='Checking existing report folders…';
    const states=reports.map(()=> 'Checking');render(states);
    try{
      const assignment=await window.AITDirectories.get(scope);
      if(current!==revision)return;
      let unavailable='';
      if(!assignment)unavailable='No folder assigned';
      else if(String(assignment.user)!==String(scope.user) || String(assignment.property)!==String(scope.property))throw new Error('Assignment scope mismatch');
      else if(await assignment.handle.queryPermission({mode:'read'})!=='granted')unavailable='Access not authorised';
      if(current!==revision)return;
      if(unavailable){render(states.map(()=>unavailable));message.textContent=unavailable==='No folder assigned'?'No report destination is assigned to this property.':'Folder access is not currently authorised. Confirm access through Utilities Output.';return;}
      for(let index=0;index<reports.length;index++){
        try{
          const category=await assignment.handle.getDirectoryHandle(reports[index].source,{create:false});
          await category.getDirectoryHandle(reports[index].title,{create:false});
          states[index]='Available';
        }catch(error){states[index]=error.name==='NotFoundError'?'Not found':error.name==='NotAllowedError'?'Access not authorised':error.name==='TypeMismatchError'?'Not a folder':'Unable to check';}
        if(current!==revision)return;
        render(states);
      }
      message.textContent='Existing report folders in '+assignment.handle.name+'.';
    }catch(_){if(current===revision){render(states.map(()=> 'Unable to check'));message.textContent='Could not read this property’s folder register. Existing folders are unchanged.';}}
  };
})();
