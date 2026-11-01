(() => {
  const rows=Array.from(document.querySelectorAll('[data-reading-row]'));
  const button=document.getElementById('reading-toggle');
  const filter=document.getElementById('reading-meter-filter');
  const empty=document.getElementById('reading-filter-empty');
  let expanded=false;
  function render(){
    const selected=filter?.value || '';
    const output=document.getElementById('reading-output');
    if(output)output.href=output.dataset.outputBase+'&item_id='+encodeURIComponent(selected);
    const savedFilter=document.getElementById('capture-view-item');if(savedFilter)savedFilter.value=selected;
    const matching=rows.filter(row=>!selected || row.dataset.meterId===selected);
    rows.forEach(row=>{row.hidden=true;});
    matching.forEach((row,index)=>{row.hidden=!expanded && index>=5;});
    if(empty)empty.hidden=matching.length!==0;
    if(button){
      button.hidden=matching.length<=5;
      button.textContent=(expanded?'Show Less':'Show More')+' ('+(expanded?matching.length:Math.min(5,matching.length))+' of '+matching.length+')';
      button.setAttribute('aria-expanded',String(expanded));
      button.classList.toggle('bg-blue-100',!expanded);button.classList.toggle('bg-purple-100',expanded);
    }
  }
  button?.addEventListener('click',()=>{expanded=!expanded;render();});
  filter?.addEventListener('change',()=>{expanded=false;render();});
  render();
})();
