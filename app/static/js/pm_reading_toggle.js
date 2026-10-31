(() => {
  const button=document.getElementById('reading-toggle');if(!button)return;
  const rows=Array.from(document.querySelectorAll('[data-reading-row]'));
  let expanded=false;
  button.addEventListener('click',()=>{
    expanded=!expanded;
    rows.forEach((row,index)=>{row.hidden=!expanded && index>=5;});
    button.textContent=(expanded?'Show Less':'Show More')+' ('+(expanded?rows.length:5)+' of '+rows.length+')';
    button.setAttribute('aria-expanded',String(expanded));
    button.classList.toggle('bg-blue-100',!expanded);button.classList.toggle('bg-purple-100',expanded);
  });
})();