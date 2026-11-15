(() => {
  const node=document.getElementById('boq-folder-context');if(!node)return;
  const scope=JSON.parse(node.textContent),message=document.getElementById('boq-folder-message');
  document.querySelectorAll('[data-section-save]').forEach(button=>button.addEventListener('click',async()=>{
    button.disabled=true;
    try{
      const name=await window.AITDirectories.save(scope,'boq_section',button.dataset.sectionSave);
      message.textContent='Saved '+name+' in the selected BOQ folder.';
    }catch(error){message.textContent=error.message;}finally{button.disabled=false;}
  }));
})();
