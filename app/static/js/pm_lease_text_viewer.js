(()=>{
 const pages=Array.from(document.querySelectorAll('[data-lease-page]'));if(!pages.length)return;
 const byId=id=>document.getElementById('lease-'+id);
 const previous=byId('previous'),next=byId('next'),input=byId('page-number'),status=byId('page-status');
 let current=1,zoom=1;
 function show(value){
  const parsed=Number(value);current=Math.max(1,Math.min(pages.length,Number.isInteger(parsed)?parsed:current));
  pages.forEach((page,index)=>{page.hidden=index!==current-1;page.style.fontSize=zoom+'em';});
  input.value=current;previous.hidden=current===1;next.hidden=current===pages.length;
  status.textContent='Logical page '+current+' of '+pages.length;
 }
 previous.addEventListener('click',()=>show(current-1));next.addEventListener('click',()=>show(current+1));
 input.addEventListener('change',()=>show(input.value));input.addEventListener('keydown',event=>{if(event.key==='Enter'){event.preventDefault();show(input.value);}});
 byId('zoom-out').addEventListener('click',()=>{zoom=Math.max(0.5,Math.round((zoom-0.1)*10)/10);show(current);});
 byId('zoom-in').addEventListener('click',()=>{zoom=Math.min(2.5,Math.round((zoom+0.1)*10)/10);show(current);});
 show(1);
})();
