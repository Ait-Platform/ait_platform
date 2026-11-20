(() => {
  'use strict';
  const open=document.getElementById('folder-register-open');if(!open)return;
  const byId=id=>document.getElementById(id),area=byId('folder-register-area'),close=byId('folder-register-close');
  const rows=byId('folder-register-rows'),message=byId('folder-register-message'),access=byId('folder-register-access');
  const table=byId('folder-register-table'),contents=byId('folder-register-contents'),viewer=byId('folder-register-viewer');
  const path=byId('folder-register-path'),files=byId('folder-register-files'),frame=byId('folder-view-pdf');
  const pages=byId('folder-view-pages');
  let pdfTask=null,renderTask=null;
  const emailForm=byId('folder-view-email-form'),emailConfig=JSON.parse(byId('directory-email').textContent);
  const scope=JSON.parse(byId('directory-context').textContent),reports=JSON.parse(byId('directory-reports').textContent);
  let revision=0,summary='',readHandle=null,url=null,selected=null;
  const states=reports.map(()=>null);
  function release(){
    if(renderTask){renderTask.cancel();renderTask=null;}
    if(pdfTask){pdfTask.destroy().catch(()=>{});pdfTask=null;}
    for(const canvas of pages.querySelectorAll('canvas')){canvas.width=canvas.height=0;}
    pages.replaceChildren();delete pages.dataset.objectUrl;frame.onload=null;frame.removeAttribute('src');if(url)URL.revokeObjectURL(url);url=null;selected=null;emailForm.hidden=true;}
  function register(){release();table.hidden=false;contents.hidden=true;viewer.hidden=true;message.textContent=summary;}
  function cell(row,text){const td=document.createElement('td');td.className='p-2';td.textContent=text;row.appendChild(td);return td;}
  function link(parent,label,callback,disabled=false){const button=document.createElement('button');button.type='button';button.textContent=label;button.className='text-left text-blue-800 underline disabled:text-slate-600 disabled:no-underline';button.disabled=disabled;button.onclick=callback;parent.appendChild(button);}
  function render(){
    rows.replaceChildren();
    reports.forEach((report,index)=>{
      const row=document.createElement('tr');row.className='border-t border-slate-200';cell(row,String(index+1));
      link(cell(row,''),report.source+' / '+report.title,()=>showFiles(index),states[index]===null);
      cell(row,states[index]===null?'\u2014':String(states[index]));rows.appendChild(row);
    });
  }
  async function assignedRoot(){
    const assignment=await window.AITDirectories.get(scope);
    if(!assignment)throw new Error('No report destination is assigned to this property.');
    if(String(assignment.user)!==String(scope.user) || String(assignment.property)!==String(scope.property))throw new Error('Could not verify this property\u2019s assignment.');
    const handle=assignment.handle;
    if(await handle.queryPermission({mode:'read'})!=='granted'){
      readHandle=handle;access.hidden=false;
      throw new Error('Read access is required. Click Allow Access for this assigned folder.');
    }
    readHandle=null;access.hidden=true;return handle;
  }
  async function entries(root,report){
    const source=await root.getDirectoryHandle(report.source,{create:false});
    const folder=await source.getDirectoryHandle(report.title,{create:false}),result=[];
    for await(const [name,handle] of folder.entries())if(handle.kind==='file')result.push({name,handle});
    return result.sort((a,b)=>a.name.localeCompare(b.name));
  }
  async function showFiles(index){
    const current=++revision;release();table.hidden=true;contents.hidden=false;viewer.hidden=true;files.replaceChildren();
    path.textContent=reports[index].source+' / '+reports[index].title;message.textContent='Reading existing files...';
    try{
      const listed=await entries(await assignedRoot(),reports[index]);
      if(current!==revision)return;states[index]=listed.length;render();
      for(const [number,entry] of listed.entries()){
        let date='\u2014';try{const file=await entry.handle.getFile();if(file.lastModified)date=new Intl.DateTimeFormat('en-GB').format(file.lastModified);}catch(_){}
        if(current!==revision)return;
        const row=document.createElement('tr');row.className='border-t border-slate-200';cell(row,String(number+1));
        link(cell(row,''),entry.name,()=>showPDF(index,entry));cell(row,date);files.appendChild(row);
      }
      message.textContent=listed.length?'Select a filename to view the original saved PDF.':'No files in this folder.';
    }catch(_){if(current===revision){states[index]=null;render();message.textContent='Could not list this folder. Check its read access.';}}
  }
  async function showPDF(index,entry){
    const current=++revision;release();message.textContent='Opening the original saved PDF...';
    try{
      // Resolve the entry again through the currently authorised assignment.
      const listed=await entries(await assignedRoot(),reports[index]),match=listed.find(item=>item.name===entry.name);
      if(!match || !await match.handle.isSameEntry(entry.handle))throw new Error('The saved file is no longer available.');
      const file=await match.handle.getFile();
      if(await file.slice(0,5).text()!=='%PDF-')throw new Error('This file is not a PDF. No file was changed.');
      const bytes=await file.arrayBuffer();if(current!==revision)return;
      const blob=new Blob([bytes],{type:'application/pdf'});url=URL.createObjectURL(blob);
      selected={blob,name:entry.name,report:reports[index].code,request:crypto.randomUUID()};
      byId('folder-view-name').textContent=entry.name;pages.dataset.objectUrl=url;
      table.hidden=true;contents.hidden=true;viewer.hidden=false;
      message.textContent='Rendering the original saved PDF...';
      // Render pages ourselves rather than delegating display to the browser's
      // PDF plug-in, which may substitute a grey click-to-open placeholder.
      const library=await import(pages.dataset.runtime);if(current!==revision)return;
      library.GlobalWorkerOptions.workerSrc=pages.dataset.worker;
      const task=library.getDocument({url,isEvalSupported:false,
        cMapUrl:pages.dataset.assets+'cmaps/',cMapPacked:true,
        standardFontDataUrl:pages.dataset.assets+'standard_fonts/',wasmUrl:pages.dataset.assets+'wasm/'});
      pdfTask=task;const pdf=await task.promise;if(current!==revision)return;
      for(let number=1;number<=pdf.numPages;number++){
        const page=await pdf.getPage(number);if(current!==revision)return;
        const natural=page.getViewport({scale:1});
        const width=Math.max(240,pages.clientWidth-20),scale=Math.min(2,(window.devicePixelRatio || 1)*width/natural.width);
        const viewport=page.getViewport({scale}),canvas=document.createElement('canvas');
        canvas.width=Math.ceil(viewport.width);canvas.height=Math.ceil(viewport.height);
        canvas.style.width='100%';canvas.style.height='auto';canvas.style.display='block';
        canvas.setAttribute('aria-label','PDF page '+number+' of '+pdf.numPages);pages.appendChild(canvas);
        renderTask=page.render({canvasContext:canvas.getContext('2d'),viewport});await renderTask.promise;
        if(current!==revision)return;renderTask=null;page.cleanup();
      }
      message.textContent='Original saved PDF: '+pdf.numPages+' page(s). No report has been regenerated.';
    }catch(error){if(current===revision)message.textContent=error.message;}
  }
  byId('folder-register-back').onclick=()=>{revision++;register();};
  byId('folder-view-back').onclick=()=>{revision++;release();viewer.hidden=true;contents.hidden=false;message.textContent='Select a filename to view the original saved PDF.';};
  byId('folder-view-print').onclick=()=>{
    if(!selected || !url)return;
    const original=url,current=revision;
    message.textContent='Preparing the original saved PDF for browser printing...';
    function print(){
      if(current!==revision || url!==original)return;
      try{frame.contentWindow.focus();frame.contentWindow.print();message.textContent='Print requested for the original saved PDF. Confirm printing in the browser.';}
      catch(_){message.textContent='The browser could not print this saved PDF. Printing has not been confirmed.';}
    }
    if(frame.getAttribute('src')===original)print();
    else{frame.onload=print;frame.src=original;}

  };
  byId('folder-view-email').onclick=()=>{if(selected){emailForm.hidden=false;byId('folder-email-to').focus();}};
  emailForm.onsubmit=async event=>{
    event.preventDefault();if(!selected || !emailForm.reportValidity())return;
    const document=selected,submit=emailForm.querySelector('button[type="submit"]'),current=revision;
    if(document.blob.size>10*1024*1024){message.textContent='Email attachments must be 10 MB or smaller.';return;}
    submit.disabled=true;
    try{
      const data=new FormData();data.set('property_id',scope.property);data.set('report',document.report);
      data.set('email_to',byId('folder-email-to').value);data.set('request_id',document.request);data.set('csrf_token',emailConfig.csrf);
      data.set('document',document.blob,document.name);
      const response=await fetch(emailConfig.url,{method:'POST',body:data,credentials:'same-origin'});
      let result;try{result=await response.json();}catch(_){throw new Error('Email was not confirmed. Check the previous outcome before retrying.');}
      if(!response.ok)throw new Error(result.message || 'Email was not confirmed.');
      if(current===revision)message.textContent=result.message;
    }catch(error){if(current===revision)message.textContent=error.message;}
    finally{submit.disabled=false;}
  };
  access.onclick=async()=>{
    const handle=readHandle;if(!handle)return;access.disabled=true;
    try{
      // Request immediately from the click, without an IndexedDB await.
      if(await handle.requestPermission({mode:'read'})!=='granted'){message.textContent='Read access was not granted. Existing assignments are unchanged.';return;}
      await load();
    }catch(_){message.textContent='Read access could not be confirmed. Existing assignments are unchanged.';}
    finally{access.disabled=false;}
  };
  close.onclick=()=>{revision++;release();area.hidden=true;access.hidden=true;open.setAttribute('aria-expanded','false');open.focus();};
  async function load(){
    const current=++revision;area.hidden=false;open.setAttribute('aria-expanded','true');summary='Reading existing report folders...';register();
    states.fill(null);render();access.hidden=true;
    try{
      const root=await assignedRoot();if(current!==revision)return;let unavailable=false;
      for(let index=0;index<reports.length;index++){
        let count=null;try{count=(await entries(root,reports[index])).length;}catch(_){unavailable=true;}
        if(current!==revision)return;states[index]=count;render();
      }
      summary='Existing report folders in '+root.name+'.'+(unavailable?' A dash means the folder could not be read.':'');message.textContent=summary;
    }catch(error){if(current===revision){states.fill(null);render();summary=error.message;message.textContent=summary;}}
  }
  open.onclick=load;window.addEventListener('pagehide',release);
})();
