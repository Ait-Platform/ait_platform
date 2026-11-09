(() => {
  'use strict';
  const root=document.getElementById('directory-register');if(!root)return;
  const scope=JSON.parse(document.getElementById('directory-context').textContent);
  const reports=JSON.parse(document.getElementById('directory-reports').textContent),api=window.AITDirectories;
  const workspace=document.getElementById('directory-workspace'),form=document.getElementById('directory-form'),table=document.getElementById('directory-table');
  const report=document.getElementById('directory-report'),folder=document.getElementById('directory-folder'),message=document.getElementById('directory-message');
  let selected=null;
  function say(text){message.textContent=text;}
  function close(){workspace.hidden=true;form.hidden=table.hidden=true;selected=null;}
  function edit(code,handle){workspace.hidden=form.hidden=false;table.hidden=true;document.getElementById('directory-workspace-title').textContent=handle?'Edit Directory':'Add Directory';report.disabled=!!code;report.value=code || reports[0].code;selected=handle || null;folder.textContent=selected?selected.name:'No folder selected';}
  async function register(){
    const rows=await api.list(scope),body=document.getElementById('directory-rows');body.replaceChildren();
    workspace.hidden=table.hidden=false;form.hidden=true;document.getElementById('directory-workspace-title').textContent='Directory Register';
    for(const row of rows){
      const tr=document.createElement('tr');
      for(const value of [reports.find(r=>r.code===row.report)?.title || row.report,row.handle.name]){const td=document.createElement('td');td.textContent=value;tr.appendChild(td);}
      const actions=document.createElement('td');
      for(const [label,action] of [['Edit',()=>edit(row.report,row.handle)],['Remove',async()=>{await api.remove(scope,row.report);await register();say('Directory assignment removed. Files on your PC were not changed.');}]]){
        const button=document.createElement('button');button.type='button';button.textContent=label;button.className='rounded-lg bg-slate-100 px-4 py-2 mr-2';button.addEventListener('click',()=>Promise.resolve(action()).catch(error=>say(error.message)));actions.appendChild(button);
      }
      tr.appendChild(actions);body.appendChild(tr);
    }
    if(!rows.length){const row=body.insertRow();const cell=row.insertCell();cell.colSpan=3;cell.textContent='No directories registered for this property.';}
  }
  document.getElementById('directory-close').onclick=close;document.getElementById('directory-cancel').onclick=close;
  document.getElementById('directory-add').onclick=()=>{say('');edit();};
  document.getElementById('directory-view').onclick=()=>register().catch(error=>say(error.message));
  document.getElementById('directory-choose').onclick=async()=>{try{selected=await window.showDirectoryPicker({mode:'readwrite',id:'ait-pm-reports'});folder.textContent=selected.name;}catch(error){if(error.name!=='AbortError')say(error.message);}};
  form.addEventListener('submit',async event=>{event.preventDefault();try{await api.set(scope,report.value,selected);await register();say('Directory registered on this PC.');}catch(error){say(error.message);}});
  if(!scope.property || !api.supported()){
    document.getElementById('directory-add').disabled=true;document.getElementById('directory-view').disabled=!scope.property || !window.indexedDB;
    say(!scope.property?'Register a property first.':'Direct folder saving requires Chrome or Edge over HTTPS. Use Download PDF in Utilities Output in this browser.');
  }
})();
