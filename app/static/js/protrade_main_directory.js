/* Main-directory setup only. Output and historical documents remain independent. */
(() => {
  'use strict';
  const config=JSON.parse(document.getElementById('main-directory-setup').textContent);
  const api=window.AITDirectories,scope=config.scope,d=window.ProTradeDirectoryDiagnostics;
  const run=(label,work)=>d?d.run(label,work):work();
  const button=document.getElementById('directory-select'),label=document.getElementById('directory-folder');
  const message=document.getElementById('directory-message');
  let loaded=false,previous=null,metadata=null;
  async function authorise(){
    const response=await run('Server context: GET consultant/new-shop authorisation (no directory registration endpoint)',()=>fetch(config.url,{credentials:'same-origin',cache:'no-store',redirect:'error'}));
    d?.log('SERVER RESPONSE: HTTP '+response.status);
    if(!response.ok)throw new Error('Consultant or new-shop access could not be verified. Reopen Main Directory.');
    const value=await run('Server context: parse JSON response',()=>response.json());
    if(value.user!==scope.user || value.programKey!=='protrade')throw new Error('The signed-in consultant changed. Reopen Main Directory.');
    return value;
  }
  async function createShop(){
    if(!metadata.shop){d?.log('SHOP CREATION SKIPPED: no signed new-shop event; existing shops are not processed.');return;}
    const path=await api.createRegisteredShopDirectories(scope,{authorise,diagnostic:d});
    message.textContent='Verified directories: '+path+'/Invoice and Statement.';
  }
  const registration={authorise,previous:null,programName:'',diagnostic:d};
  api.bindSelection({scope,button,label,message,registration,register:api.registerMainDirectory,diagnostic:d,
    ready:()=>loaded,
    async success(row){
      previous=row;registration.previous=row;label.textContent=row.handle.name;button.textContent='Replace Main Directory';
      message.textContent='Main directory authorised. Program directory read/write access verified.';
      try{await createShop();}catch(error){d?.log('SHOP CREATION EXCEPTION: '+error.name+': '+error.message);message.textContent='Main directory authorised. '+error.message;}
    }
  });
  (async()=>{
    try{
      metadata=await authorise();registration.programName=metadata.program;
      previous=await run('Initialisation: restore registration from IndexedDB',()=>api.get(scope,d?.event));registration.previous=previous;
      label.textContent=previous?previous.handle.name:'None';
      button.textContent=previous?'Replace Main Directory':'Select Main Directory';
      loaded=true;button.disabled=false;
      if(metadata.shop){await createShop();}
      else if(previous){
        const permission=await run('Restoration: query read/write permission',()=>previous.handle.queryPermission({mode:'readwrite'}));
        if(permission!=='granted')throw new Error('The registered directory is inaccessible or needs authorisation. Replace it to verify access.');
        // Read existing entries without creating directories or requesting permission.
        await run('Restoration: enumerate registered main directory',async()=>{for await(const entry of previous.handle.entries()){break;}});
        message.textContent='Registration restored. Select Replace Main Directory to authorise a new destination.';
      }else message.textContent='Select and authorise a main directory. No existing shop directories will be created.';
    }catch(error){d?.log('INITIALISATION EXCEPTION: '+error.name+': '+error.message);message.textContent=(metadata?.shop?'Shop registered; directory creation failed: ':'')+error.message;if(!loaded)button.disabled=true;}
  })();
})();
