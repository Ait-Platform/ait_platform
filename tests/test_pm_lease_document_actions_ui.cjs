const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
module.exports = async function run(rootPath) {
  const components=['cover','certificate','core','special_conditions','signature','lease'];
  const requests=[],frames=[],blobs=[],elements=new Map();let printed=0,denied=false,revoked=0;
  const buttons=[];
  for(const component of components) {
    const row={dataset:{label:component,pdfUrl:'/kit.pdf?component='+component,emailUrl:'/kit/email?component='+component}};
    for(const action of ['email','print'])buttons.push({dataset:{action},disabled:false,closest(){return row;}});
  }
  const root={querySelectorAll(){return buttons;}};
  for(const name of ['status','email-form','email-status','email-send','email-cancel','email-title'])elements.set('lease-actions-'+name,{hidden:true,textContent:'',disabled:false});
  const form=elements.get('lease-actions-email-form');form.elements={recipient:{focus(){}}};form.reportValidity=()=>true;
  const events={};const sandbox={console,Blob,FormData:class {set(key,value){this[key]=value;}},Error,Array,
    document:{getElementById(id){return id==='lease-document-actions'?root:elements.get(id);},
      createElement(){return {style:{},remove(){},contentWindow:{focus(){},print(){printed++;}}};},
      body:{appendChild(frame){frames.push(frame);frame.onload();}}},
    URL:{createObjectURL(blob){blobs.push(blob);return 'blob:private-'+blobs.length;},revokeObjectURL(){revoked++;}},
    fetch:async(url,options)=>{requests.push({url,options});const component=new URL(url,'https://ait.test').searchParams.get('component');
      return {ok:!denied,status:denied?403:200,headers:{get(name){return name==='Content-Type'?'application/pdf':'token-'+component;}},
        arrayBuffer:async()=>new TextEncoder().encode('%PDF-'+component+'-all-pages-in-order').buffer,json:async()=>({message:'Submitted'})};},
    window:{addEventListener(name,fn){events[name]=fn;}}
  };
  vm.runInNewContext(fs.readFileSync(rootPath+'/app/static/js/lease_document_actions.js','utf8'),sandbox);
  for(let index=0;index<components.length;index++) {
    const component=components[index];
    await buttons[index*2+1].onclick();assert.equal(requests.at(-1).url,'/kit.pdf?component='+component);
    assert.equal(await blobs.at(-1).text(),'%PDF-'+component+'-all-pages-in-order');
    assert.equal(frames.at(-1).src,'blob:private-'+blobs.length);
    await buttons[index*2].onclick();assert.equal(form.hidden,false);
    assert.equal(elements.get('lease-actions-email-title').textContent,'Email '+component);
    await form.onsubmit({preventDefault(){}});
    assert.equal(requests.at(-1).url,'/kit/email?component='+component);
    assert.equal(requests.at(-1).options.body.document_token,'token-'+component);
    assert.equal(requests.at(-1).options.credentials,'same-origin');
  }
  assert.equal(printed,6);denied=true;
  await buttons[1].onclick();assert.equal(printed,6);assert.equal(frames.length,6);
  await buttons[0].onclick();assert.equal(form.hidden,true);
  assert.match(elements.get('lease-actions-status').textContent,/unavailable/);
  events.pagehide();assert.ok(revoked>=6);
  console.log('All six rows passed: independent PDF print bytes/order, email selection/token, protected fetch and denied access.');
};
