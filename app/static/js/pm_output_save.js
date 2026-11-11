(() => {
  'use strict';
  const button=document.getElementById('output-save');if(!button)return;
  const scope=JSON.parse(document.getElementById('output-directory-context').textContent);
  const message=document.getElementById('output-save-message'),download=document.getElementById('output-download');
  let saving=false,cooldownUntil=0,timer=null,savedMessage='';
  function refresh(){
    const seconds=Math.max(0,Math.ceil((cooldownUntil-Date.now())/1000));
    button.disabled=saving || seconds>0 || download.getAttribute('aria-disabled')==='true';
    button.textContent=saving?'Saving...':seconds?'Save Report ('+seconds+'s)':'Save Report';
    if(!saving && savedMessage)message.textContent=savedMessage+(seconds?' Next METSOA save in '+seconds+'s.':'');
    if(!seconds && timer){clearInterval(timer);timer=null;}
  }
  button.addEventListener('click',async()=>{
    if(saving || Date.now()<cooldownUntil || button.disabled)return;
    if(scope.report==='metsoa_statement'){
      const end=scope.period_end;
      if(typeof end!=='string' || !/^\d{4}-\d{2}-\d{2}$/.test(end) || !Number.isFinite(Date.parse(end+'T00:00:00Z')) || new Date(end+'T00:00:00Z').toISOString().slice(0,10)!==end){message.textContent='Preview METSOA again to refresh its reporting-period ending date.';return;}
      const title=document.title,name=end.slice(8,10)+end.slice(5,7)+end.slice(2,4)+'_METSOA';
      const restore=()=>{document.title=title;delete document.documentElement.dataset.metsoaNativeSave;};
      const finish=()=>{
        restore();saving=false;cooldownUntil=Date.now()+10000;refresh();
        if(!timer)timer=setInterval(refresh,1000);
      };
      savedMessage='Choose Save as PDF in the browser print dialog. Suggested filename: '+name+'.pdf. Saving or cancellation is controlled by your browser.';
      saving=true;refresh();message.textContent=savedMessage;
      window.addEventListener('afterprint',finish,{once:true});
      try{document.title=name;document.documentElement.dataset.metsoaNativeSave='';window.print();}
      catch(error){window.removeEventListener('afterprint',finish);restore();saving=false;savedMessage='';refresh();message.textContent='The browser print dialog could not be opened. No PDF saving was confirmed.';}
      return;
    }
    const url=download.getAttribute('href');if(!url)return;
    download.hidden=true;saving=true;savedMessage='';refresh();
    try{
      if(!window.AITDirectories.supported() || !navigator.locks){
        const name=await window.AITDirectories.download(scope,scope.report,url);
        savedMessage='PDF download started: '+name+'. Browser completion is not confirmed.';
      }else{
        const name=await window.AITDirectories.save(scope,scope.report,url);
        savedMessage='Saved '+name+' to your registered folder.';
      }
      if(scope.report==='metsoa_statement')cooldownUntil=Date.now()+10000;
    }catch(error){message.textContent=error.name==='AbortError'?'Saving cancelled. No report was saved.':error.message;if(error.name!=='AbortError')download.hidden=false;}
    finally{
      saving=false;refresh();
      if(cooldownUntil>Date.now() && !timer)timer=setInterval(refresh,1000);
    }
  });
})();
