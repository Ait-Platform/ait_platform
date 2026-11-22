/* Save new exports through the established owned-directory/collision-safe API. */
(() => {
  const button=document.getElementById('output-save');if(!button)return;
  const scope=JSON.parse(document.getElementById('output-directory-context').textContent),message=document.getElementById('output-save-message');
  button.onclick=async()=>{
    if(button.disabled)return;button.disabled=true;
    try{
      const name=await window.AITDirectories.save(scope,scope.report,document.getElementById('output-download').href);
      message.textContent='Saved '+name+' in this consultant and business destination.';
    }catch(error){message.textContent=error.message+' Download PDF remains available.';}
    finally{button.disabled=!!document.getElementById('output-download')?.hidden || !scope.report;}
  };
})();
