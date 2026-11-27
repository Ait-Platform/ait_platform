/* Visible, local-only diagnostics. Observe pending operations; never abort or retry them. */
(() => {
  'use strict';
  const lines=[];
  let sequence=0;
  const started=performance.now();
  function errorText(error){return String(error?.name || 'Error')+': '+String(error?.message || error || 'Unknown exception');}
  function log(message){
    const elapsed=((performance.now()-started)/1000).toFixed(1);
    lines.push('['+elapsed+'s] '+String(message).slice(0,1800));
    if(lines.length>250)lines.shift();
    for(const id of ['directory-diagnostic-log','directory-modal-diagnostics']){
      const target=document.getElementById(id);
      if(target){target.textContent=lines.join('\n');target.scrollTop=target.scrollHeight;}
    }
  }
  function run(label,work){
    const id=++sequence,time=performance.now();
    log('START #'+id+' '+label);
    let result;
    try{result=work();}
    catch(error){log('EXCEPTION #'+id+' '+label+' - '+errorText(error));throw error;}
    if(!result || typeof result.then!=='function'){log('DONE #'+id+' '+label);return result;}
    const timer=setInterval(()=>log('WAIT #'+id+' '+label+' - unresolved for '+Math.floor((performance.now()-time)/1000)+'s; stage-specific permission deadlines remain active; no automatic retry.'),5000);
    return Promise.resolve(result).then(value=>{
      log('DONE #'+id+' '+label+(typeof value==='string'?' -> '+value.slice(0,100):''));return value;
    },error=>{log('EXCEPTION #'+id+' '+label+' - '+errorText(error));throw error;}).finally(()=>clearInterval(timer));
  }
  const event=(name,details={})=>log('INDEXEDDB '+name+(details.mode?' mode='+details.mode:'')+(details.name?' '+details.name+': '+(details.message || ''):''));
  window.ProTradeDirectoryDiagnostics=Object.freeze({log,run,event});
  window.addEventListener('error',event=>log('JAVASCRIPT EXCEPTION: '+(event.error?errorText(event.error):event.message || 'Resource load failed')+(event.lineno?' at line '+event.lineno+':'+event.colno:'')),true);
  window.addEventListener('unhandledrejection',event=>log('UNHANDLED PROMISE REJECTION: '+errorText(event.reason)));
  log('Directory diagnostics loaded (20261127-stage-trace). Handles stay in browser IndexedDB; server checks identity only.');
})();
