const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
module.exports=async function(rootPath){
 class Element{
  constructor(tag){this.tagName=tag;this.children=[];this.dataset={};this.style={};this.textContent='';this.attributes={};}
  append(...values){this.children.push(...values);}replaceChildren(...values){this.children=values;}setAttribute(key,value){this.attributes[key]=value;}get lastChild(){return this.children.at(-1);}
 }
 const ids=new Map();for(const name of ['data','status','catalog','selected','count','relationships','csrf'])ids.set('addendum-clause-'+name,new Element('div'));
 const rows=['certificate','core','special_conditions','signature'].map((source,index)=>({id:source+':id',source,source_label:source,reference:String(index+1),description:'Description '+source,original:'Exact '+source+' original wording',source_hash:source,provenance:'Actual source'}));
 const state={lease_key:'draft',version:4,catalog:rows,selected:[],relationships:[{left:rows[1].id,right:rows[2].id,evidence:'Explicit reference: Core Lease clause 2'}]};ids.get('addendum-clause-data').textContent=JSON.stringify(state);ids.get('addendum-clause-csrf').value='csrf';
 const buttons=rows.map(row=>Object.assign(new Element('button'),{dataset:{clauseSource:row.source}})),root=new Element('section');root.dataset.url='/billing/leases/property/26/addendum?draft_id=selected';root.querySelectorAll=()=>buttons;ids.set('addendum-clauses',root);
 const requests=[];let fail=false;const text=element=>element.textContent+element.children.map(text).join(' ');
 const input=element=>element.children.flatMap(child=>child.tagName==='input'?[child]:input(child));
 const sandbox={document:{getElementById:id=>ids.get(id)||null,createElement:tag=>new Element(tag)},FormData:class{constructor(){this.values=new Map();}set(key,value){this.values.set(key,value);}},fetch:async(url,options)=>{
  requests.push({url,options});const data=options.body.values;if(fail)return {ok:false,status:409,json:async()=>({error:'The register changed. Saved selections were preserved.'})};
  const entry=rows.find(row=>row.id===data.get('provision_id'));if(data.get('selected')==='yes'&&!state.selected.some(row=>row.id===entry.id))state.selected.push(entry);if(data.get('selected')==='no')state.selected=state.selected.filter(row=>row.id!==entry.id);state.version++;return {ok:true,json:async()=>JSON.parse(JSON.stringify(state))};
 }};
 vm.runInNewContext(fs.readFileSync(rootPath+'/app/static/js/lease_addendum_clauses.js','utf8'),sandbox);
 const catalog=ids.get('addendum-clause-catalog'),selected=ids.get('addendum-clause-selected');
 assert.match(text(catalog),/certificate/);let checkbox=input(catalog)[0];checkbox.checked=true;await checkbox.onchange();assert.equal(requests.length,1);assert.equal(requests[0].options.body.values.get('register_version'),'4');assert.equal(requests[0].options.credentials,'same-origin');assert.match(text(selected),/Exact certificate original wording/);assert.equal(selected.children[0].lastChild.children[0].open,true,'Selected original wording must be visible immediately');
 buttons[1].onclick();checkbox=input(catalog)[0];checkbox.checked=true;await checkbox.onchange();assert.equal(state.selected.length,2);assert.equal(input(ids.get('addendum-clause-relationships'))[0].checked,false,'Suggestions must not be selected automatically');assert.match(text(ids.get('addendum-clause-relationships')),/Suggested only/);
 checkbox=input(ids.get('addendum-clause-relationships'))[0];checkbox.checked=true;await checkbox.onchange();assert.equal(state.selected.length,3,'Suggested checkbox must allow explicit selection');buttons[2].onclick();assert.equal(input(catalog)[0].checked,true);assert.equal(input(selected).length,3);assert.equal(ids.get('addendum-clause-count').textContent,'3 selected');
 buttons[3].onclick();checkbox=input(catalog)[0];checkbox.checked=true;await checkbox.onchange();assert.equal(state.selected.length,4);
 checkbox=input(selected)[0];checkbox.checked=false;await checkbox.onchange();assert.equal(state.selected.length,3);assert.equal(ids.get('addendum-clause-count').textContent,'3 selected');
 fail=true;buttons[0].onclick();checkbox=input(catalog)[0];checkbox.checked=true;await checkbox.onchange();assert.equal(input(catalog)[0].checked,false);assert.match(ids.get('addendum-clause-status').textContent,/Saved selections were preserved/);
 return 'PASS: all four sections, persistent register, exact wording, suggestions remain unselected, checkbox removal and failed-save recovery';
};
