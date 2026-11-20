/* Print only pages decoded from the selected saved PDF; never the PM webpage. */
(() => {
  'use strict';
  const frame=document.getElementById('folder-view-pdf'),button=document.getElementById('folder-view-print');
  if(!frame || !button)return;
  let active=null;
  function clear(){
    const job=active;if(!job)return;active=null;
    clearTimeout(job.timer);frame.onload=null;
    if(job.rejectLoad)job.rejectLoad(new Error('Printing cancelled'));
    if(job.render)job.render.cancel();
    if(job.task)job.task.destroy().catch(()=>{});
    if(job.done)frame.contentWindow.removeEventListener('afterprint',job.done);
    for(const canvas of frame.contentDocument?.querySelectorAll('canvas') || [])canvas.width=canvas.height=0;
    frame.removeAttribute('srcdoc');button.disabled=false;
  }
  async function print(blob,name,config,status){
    if(active)return;
    const job={};active=job;button.disabled=true;
    status('Preparing the original saved PDF for printing...');
    try{
      const library=await import(config.runtime);if(active!==job)return;
      library.GlobalWorkerOptions.workerSrc=config.worker;
      const bytes=new Uint8Array(await blob.arrayBuffer());if(active!==job)return;
      job.task=library.getDocument({data:bytes,isEvalSupported:false,
        cMapUrl:config.assets+'cmaps/',cMapPacked:true,
        standardFontDataUrl:config.assets+'standard_fonts/',wasmUrl:config.assets+'wasm/'});
      const pdf=await job.task.promise;if(active!==job)return;
      // HTML in a same-origin print frame is controllable, unlike Chrome's
      // native PDF plug-in. No application UI or generated report is copied.
      await new Promise((resolve,reject)=>{
        job.rejectLoad=reject;
        job.timer=setTimeout(()=>reject(new Error('Print frame did not load')),10000);
        frame.onload=()=>{clearTimeout(job.timer);frame.onload=null;job.rejectLoad=null;resolve();};
        frame.srcdoc='<!doctype html><html><head><title></title><style>html,body{margin:0;padding:0;background:white}canvas{display:block;break-after:page;page-break-after:always}canvas:last-child{break-after:auto;page-break-after:auto}</style></head><body></body></html>';
      });
      if(active!==job)return;
      const document=frame.contentDocument,style=document.querySelector('style');document.title=name;
      for(let number=1;number<=pdf.numPages;number++){
        const page=await pdf.getPage(number);if(active!==job)return;
        const size=page.getViewport({scale:1}),viewport=page.getViewport({scale:150/72});
        const canvas=document.createElement('canvas');canvas.width=Math.ceil(viewport.width);canvas.height=Math.ceil(viewport.height);
        canvas.style.width=size.width+'pt';canvas.style.height=size.height+'pt';canvas.style.page='pm_pdf_'+number;
        style.textContent+='@page pm_pdf_'+number+'{size:'+size.width+'pt '+size.height+'pt;margin:0}';
        canvas.setAttribute('aria-label','PDF page '+number+' of '+pdf.numPages);document.body.appendChild(canvas);
        job.render=page.render({canvasContext:canvas.getContext('2d'),viewport,intent:'print'});
        await job.render.promise;if(active!==job)return;job.render=null;page.cleanup();
      }
      job.done=()=>{if(active===job)clear();};
      frame.contentWindow.addEventListener('afterprint',job.done,{once:true});
      frame.contentWindow.focus();
      status('Opening the browser print dialog for the original saved PDF. Printing is not yet confirmed.');
      frame.contentWindow.print();
      // Keep the print surfaces alive until afterprint, Back, Close or pagehide.
    }catch(_){if(active===job){clear();status('The original PDF could not be prepared for printing. No saved document was changed.');}}
  }
  window.AITPMPdfPrint={print,clear};window.addEventListener('pagehide',clear);
})();
