const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const {Element,script}=require('./test_owner_preview_ui.cjs');
const rendered=JSON.parse(fs.readFileSync(path.join(__dirname,'../tmp/owner_modal_rendered.json'),'utf8'));
const missing=JSON.parse(JSON.stringify(rendered.own));
const missingOwner=JSON.parse(missing.buttons[0]['data-owner-edit']);delete missingOwner.assignment_id;
missing.buttons[0]['data-owner-edit']=JSON.stringify(missingOwner);
rendered.missing=missing;
module.exports=(async()=>{
 for(const [scenario,fixture] of Object.entries(rendered)){
  const ids={};const keys=['type','name','identity_number','email','phone','percentage','residential_address','postal_address'];
  const fields=keys.map(key=>new Element({ownerField:key}));
  keys.forEach((key,i)=>ids['owner-'+(key==='type'?'kind':key)]=fields[i]);
  for(const key of ['owner-assignment-preview','owner-active','owner-postal-same','owner-property-name','owner-preview-message','owner-preview-close','owner-add-preview','owner-save-disabled','owner-existing-selection','owner-existing-percentage','owner-search'])ids[key]=new Element();
  const dialog=ids['owner-assignment-preview'];
  for(const [key,value] of Object.entries(fixture.dialog)){
   if(key.startsWith('data-'))dialog.dataset[key.slice(5).replace(/-([a-z])/g,(_,letter)=>letter.toUpperCase())]=value;
  }
  ids['owner-save-disabled'].disabled=Object.hasOwn(fixture.save,'disabled');
  dialog.querySelectorAll=selector=>selector==='[data-owner-field]'?fields:selector==='[data-owner-panel]'?[new Element({ownerPanel:'new'}),new Element({ownerPanel:'existing'})]:[];
  const record=fixture.buttons.find(button=>JSON.parse(button['data-owner-edit']).created_by===(scenario==='foreign'?8:7));
  const data=record||rendered.own.buttons[0];
  const button=new Element({ownerEdit:data['data-owner-edit'],ownerEditUrl:data['data-owner-edit-url'],ownerPropertyId:data['data-owner-property-id'],ownerPropertyName:data['data-owner-property-name']});
  const document={getElementById:id=>ids[id],querySelectorAll:selector=>selector==='[data-owner-edit]'?[button]:[],querySelector:()=>null,createElement:()=>new Element()};
  let calls=[],reloads=0;
  vm.runInNewContext(script,{document,Map,Array,Object,String,Number,Option:Element,fetch:async(url,options)=>{calls.push({url,options});return{ok:true,json:async()=>({message:'Saved'})};},window:{location:{reload:()=>reloads++}}});
  await button.fire('click');
  assert.equal(ids['owner-save-disabled'].hidden,false);
  assert.equal(ids['owner-add-preview'].hidden,true);
  if(scenario==='blocked' || scenario==='missing'){
   assert.equal(ids['owner-save-disabled'].disabled,true);
   assert.match(ids['owner-preview-message'].textContent,scenario==='blocked'?/PM_OWNER_WRITES_APPROVED/:/ID is missing/);
   await ids['owner-save-disabled'].fire('click');assert.equal(calls.length,0);
  }else{
   assert.equal(ids['owner-save-disabled'].disabled,false);
   assert.equal(fields[1].disabled,scenario==='foreign');
   assert.equal(ids['owner-percentage'].disabled,false);
   await ids['owner-save-disabled'].fire('click');assert.equal(calls.length,1);assert.equal(reloads,1);
   assert.equal(calls[0].options.method,'PATCH');
   assert.equal(calls[0].url.includes('/assignments/'),scenario==='foreign');
   if(scenario==='foreign')assert.deepEqual(Object.keys(JSON.parse(calls[0].options.body)).sort(),['active','percentage']);
  }
 }
 console.log('Actual rendered modal: identity edit enabled, foreign identity protected with assignment save enabled, approval block explained.');
})().catch(error=>{throw error;});
