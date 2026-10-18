(()=>{
 const pages=Array.from(document.querySelectorAll('[data-lease-page]'));if(!pages.length)return;
 const byId=id=>document.getElementById('lease-'+id);
 const previous=byId('previous'),next=byId('next'),input=byId('page-number'),fit=byId('zoom-fit'),documentArea=byId('merged-text');
 const baseWidth=Number(documentArea.dataset.pageWidth)||768;
 let current=1,scale=1,fitMode=true;
 function fitScale(){
  const css=getComputedStyle(documentArea);
  const available=documentArea.clientWidth-(parseFloat(css.paddingLeft)||0)-(parseFloat(css.paddingRight)||0);
  return Math.max(0.5,Math.min(2.5,available/baseWidth));
 }
 function show(value){
  const parsed=Number(value);current=Math.max(1,Math.min(pages.length,Number.isInteger(parsed)?parsed:current));
  if(fitMode)scale=fitScale();
  pages.forEach((page,index)=>{page.hidden=index!==current-1;page.style.zoom=String(scale);});
  input.value=current;previous.hidden=current===1;next.hidden=current===pages.length;
  fit.textContent=fitMode?'Fit':Math.round(scale*100)+'%';
  fit.setAttribute('aria-label',fitMode?'Fit to width':'Zoom '+Math.round(scale*100)+'%; restore fit to width');
 }
 function zoom(direction){
  scale=Math.max(0.5,Math.min(2.5,(direction>0?Math.floor(scale*4+1e-6)+1:Math.ceil(scale*4-1e-6)-1)/4));
  fitMode=false;show(current);
 }
 previous.addEventListener('click',()=>show(current-1));next.addEventListener('click',()=>show(current+1));
 input.addEventListener('change',()=>show(input.value));input.addEventListener('keydown',event=>{if(event.key==='Enter'){event.preventDefault();show(input.value);}});
 byId('zoom-out').addEventListener('click',()=>zoom(-1));byId('zoom-in').addEventListener('click',()=>zoom(1));
 fit.addEventListener('click',()=>{fitMode=true;show(current);});
 window.addEventListener('resize',()=>{if(fitMode)show(current);});
 show(1);
})();
