const assert=require('node:assert/strict');
const vm=require('node:vm');
const {Element,script}=require('./test_owner_preview_ui.cjs');
module.exports = (async () => {
 const keys=['type','name','identity_number','email','phone','percentage','residential_address','postal_address'];
 const fields=keys.map(key=>new Element({ownerField:key}));const ids={};
 for(const [i,key] of keys.entries()) ids['owner-'+(key==='type'?'kind':key)]=fields[i];
 for(const id of ['owner-assignment-preview','owner-active','owner-postal-same','owner-property-name','owner-preview-list','owner-preview-message','owner-preview-close','owner-add-preview','owner-save-disabled','owner-existing-selection','owner-existing-percentage','owner-search'])ids[id]=new Element();
 const dialog=ids['owner-assignment-preview'];dialog.dataset={ownersEnabled:'true',csrf:'csrf-token',ownerSearchUrl:'/search',managerId:'7'};
 const opener=new Element({propertyId:'26',propertyName:'Almond'});const tabs=[new Element({ownerTab:'new'}),new Element({ownerTab:'existing'})];const panels=[new Element({ownerPanel:'new'}),new Element({ownerPanel:'existing'})];
 dialog.querySelectorAll=selector=>selector==='[data-owner-field]'?fields:selector==='[data-owner-panel]'?panels:tabs;
 const names=new Element();const document={getElementById:id=>ids[id],querySelectorAll:selector=>selector==='[data-open-owner]'?[opener]:[],querySelector:()=>names,createElement:()=>new Element()};
 let calls=[];let fail=false;let reloads=0;let owners=[];let edits=[];
 const fetch=async(url,options)=>{
  calls.push({url,options});
  if(fail)return{ok:false,json:async()=>({error:'No changes saved.'})};
  if(options.method==='PATCH'){edits.push({url,payload:JSON.parse(options.body)});return{ok:true,json:async()=>({owner_id:edits.length})};}
  if(options.method==='POST'){
   const payload=JSON.parse(options.body);
   if(!payload.owner_id)owners.push({...payload,owner_id:owners.length+1,created_by:7,ownership_percentage:'50',assignment_id:owners.length+1,active:true});
  }
  return{ok:true,json:async()=>({property_id:26,owners})};
 };
 const Option=function(name,value){const e=new Element();e.textContent=name;e.value=String(value);return e;};
 vm.runInNewContext(script,{document,Map,Array,Object,String,Number,fetch,Option,window:{location:{reload:()=>reloads++}},confirm:()=>true,prompt:()=>''});
 await opener.fire('click');fields[0].value='individual';fields[1].value='Owner One';fields[2].value='ID1';ids['owner-residential_address'].value='Home';ids['owner-postal-same'].checked=true;await ids['owner-postal-same'].fire('change');
 await ids['owner-add-preview'].fire('click');assert.equal(calls[0].url,'/billing/property-owner/26/owners');assert.equal(calls[0].options.method,'POST');assert.equal(calls[0].options.headers['X-CSRFToken'],'csrf-token');
 const payload=JSON.parse(calls[0].options.body);assert.equal(payload.postal_same,true);assert.equal(payload.postal_address,'Home');assert.equal(dialog.open,true);assert.equal(names.children[0].children[0].textContent,'Owner One');assert.equal(reloads,0);
 await opener.fire('click');fields[0].value='individual';fields[1].value='Owner One';fields[2].value='ID1';fail=true;
 await ids['owner-add-preview'].fire('click');assert.equal(dialog.open,true);assert.equal(ids['owner-preview-message'].textContent,'No changes saved.');assert.equal(ids['owner-save-disabled'].disabled,false);
 fail=false;await tabs[1].fire('click');ids['owner-existing-selection'].value='1';await ids['owner-add-preview'].fire('click');assert.equal(JSON.parse(calls.at(-1).options.body).owner_id,1);

 owners=[];await opener.fire('click');
 for(let i=1;i<=3;i++){
  fields[0].value='individual';fields[1].value='Individual '+i;fields[2].value='ID'+i;
  await ids['owner-add-preview'].fire('click');
  assert.equal(owners.length,i);
  assert.equal(names.children.length,i);
  assert.equal(dialog.open,true);
  if(i<3)assert.equal(fields[1].value,'');
 }
 assert.equal(calls.filter(call=>call.options.method==='POST').length>=3,true);
 for(let i=0;i<3;i++){
  const button=names.children[i].children[0];
  assert.equal(button.textContent,'Individual '+(i+1));
  assert.equal(button.textContent.includes('%'),false);
  await button.fire('click');
  assert.equal(fields[1].value,'Individual '+(i+1));
  assert.equal(ids['owner-percentage'].disabled,true);
  fields[1].value='Edited '+(i+1);
  await ids['owner-save-disabled'].fire('click');
  assert.equal(edits[i].url,'/billing/property-owner/owners/'+(i+1));
 }
 assert.equal(reloads,3);
 await opener.fire('click');fields[0].value='individual';fields[1].value='Keep on failure';fields[2].value='FAIL';
 fail=true;await ids['owner-add-preview'].fire('click');
 assert.equal(fields[1].value,'Keep on failure');assert.equal(dialog.open,true);
 console.log('Enabled owner UI passed: three persisted saves, per-owner clickable edits, names without percentages, refresh and failed-save retention.');
})().catch(error=>{throw error;});
