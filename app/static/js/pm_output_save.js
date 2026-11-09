(() => {
  'use strict';
  const button=document.getElementById('output-save');if(!button)return;
  const scope=JSON.parse(document.getElementById('output-directory-context').textContent);
  const message=document.getElementById('output-save-message'),download=document.getElementById('output-download');
  let saving=false;
  button.addEventListener('click',async()=>{
    if(saving || button.disabled)return;
    const url=download.getAttribute('href');if(!url)return;
    if(!window.AITDirectories.supported() || !navigator.locks){
      message.textContent='Direct folder saving is unavailable in this browser. Downloading the PDF normally.';download.click();return;
    }
    download.hidden=true;saving=true;button.disabled=true;button.textContent='Saving...';
    try{const name=await window.AITDirectories.save(scope,scope.report,url);message.textContent='Saved '+name+' to your registered folder.';}
    catch(error){message.textContent=error.name==='AbortError'?'Saving cancelled. No report was saved.':error.message;if(error.name!=='AbortError')download.hidden=false;}
    finally{saving=false;button.textContent='Save Report';button.disabled=download.getAttribute('aria-disabled')==='true';}
  });
})();
