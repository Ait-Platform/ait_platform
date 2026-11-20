(() => {
  'use strict';
  const open=document.getElementById('folder-register-open');if(!open)return;
  const area=document.getElementById('folder-register-area'),close=document.getElementById('folder-register-close');
  const rows=document.getElementById('folder-register-rows'),message=document.getElementById('folder-register-message');
  const table=document.getElementById('folder-register-table'),contents=document.getElementById('folder-register-contents');
  const path=document.getElementById('folder-register-path'),files=document.getElementById('folder-register-files');
  const scope=JSON.parse(document.getElementById('directory-context').textContent);
  const reports=JSON.parse(document.getElementById('directory-reports').textContent);
  let revision=0,summary='';
  const states=reports.map(()=>null);
  function register(){table.hidden=false;contents.hidden=true;message.textContent=summary;}
  function render(){
    rows.replaceChildren();
    reports.forEach((report,index)=>{
      const row=document.createElement('tr');row.className='border-t border-slate-200';
      for(const value of [String(index+1),report.source+' / '+report.title,states[index]===null?'\u2014':String(states[index])]){
        const cell=document.createElement('td');cell.className='p-2';cell.textContent=value;row.appendChild(cell);
      }
      const action=document.createElement('td'),button=document.createElement('button');
      action.className='p-2';button.type='button';button.textContent='Open';
      button.className='rounded-lg border border-blue-200 bg-blue-100 px-3 py-1 disabled:opacity-50';
      button.disabled=states[index]===null;button.setAttribute('aria-label','Open '+report.source+' / '+report.title);
      button.onclick=()=>showFiles(index);action.appendChild(button);row.appendChild(action);rows.appendChild(row);
    });
  }
  async function assignedRoot(){
    const assignment=await window.AITDirectories.get(scope);
    if(!assignment)throw new Error('No report destination is assigned to this property.');
    if(String(assignment.user)!==String(scope.user) || String(assignment.property)!==String(scope.property))throw new Error('Could not verify this property\u2019s assignment.');
    if(await assignment.handle.queryPermission({mode:'read'})!=='granted')throw new Error('Folder access is not currently authorised. Confirm access through Utilities Output.');
    return assignment.handle;
  }
  async function names(root,report){
    const source=await root.getDirectoryHandle(report.source,{create:false});
    const folder=await source.getDirectoryHandle(report.title,{create:false}),result=[];
    for await(const [name,entry] of folder.entries())if(entry.kind==='file')result.push(name);
    return result.sort((a,b)=>a.localeCompare(b));
  }
  async function showFiles(index){
    const current=++revision;
    table.hidden=true;contents.hidden=false;files.replaceChildren();
    path.textContent=reports[index].source+' / '+reports[index].title;message.textContent='Reading existing file names...';
    try{
      const root=await assignedRoot(),listed=await names(root,reports[index]);
      if(current!==revision)return;
      states[index]=listed.length;render();
      for(const name of listed){const item=document.createElement('li');item.textContent=name;files.appendChild(item);}
      message.textContent=listed.length?'Existing files only. View or print documents through Utilities Output.':'No files in this folder.';
    }catch(error){if(current===revision){states[index]=null;render();message.textContent='Could not list this folder. Check read access through Utilities Output.';}}
  }
  close.onclick=()=>{
    revision++;
    if(!contents.hidden){register();return;}
    area.hidden=true;open.setAttribute('aria-expanded','false');open.focus();
  };
  open.onclick=async()=>{
    const current=++revision;
    area.hidden=false;open.setAttribute('aria-expanded','true');summary='Reading existing report folders...';register();
    states.fill(null);render();
    try{
      const root=await assignedRoot();
      if(current!==revision)return;
      let unavailable=false;
      for(let index=0;index<reports.length;index++){
        let count=null;
        try{count=(await names(root,reports[index])).length;}catch(_){unavailable=true;}
        if(current!==revision)return;
        states[index]=count;render();
      }
      summary='Existing report folders in '+root.name+'.'+(unavailable?' A dash means the folder could not be read.':'');
      message.textContent=summary;
    }catch(error){if(current===revision){states.fill(null);render();summary=error.message || 'Could not read this property\u2019s folder register.';message.textContent=summary;}}
  };
})();
