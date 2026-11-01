(() => {
  const filter=document.getElementById('statement-account-filter');if(!filter)return;
  const rows=Array.from(document.querySelectorAll('[data-statement-row]'));
  function render(){
    const selected=filter.value;
    rows.forEach(row=>{row.hidden=!!selected && row.dataset.accountId!==selected;});
    const empty=document.getElementById('statement-filter-empty');if(empty)empty.hidden=rows.some(row=>!row.hidden);
    document.getElementById('capture-view-item').value=selected;
    const output=document.getElementById('statement-output');output.href=output.dataset.outputBase+'&item_id='+encodeURIComponent(selected);
  }
  filter.addEventListener('change',render);render();
})();
