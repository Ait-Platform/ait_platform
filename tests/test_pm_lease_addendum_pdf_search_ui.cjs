const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
module.exports=async function(rootPath,indexFailure=false,oldSelection=false){
 class Element{constructor(tag){this.tagName=tag;this.children=[];this.dataset={};this.style={};this.textContent='';this.disabled=false;}append(...values){this.children.push(...values);}replaceChildren(...values){this.children=values;}setAttribute(k,v){this[k]=v;}}
 const ids=new Map();for(const name of ['search-status','search-notices','search-results','selected-count','selected','finish','retry','retry-index','csrf','state','initial'])ids.set('lease-pdf-'+name,new Element('div'));
 const state={version:1,selected:[]},core={id:'core:1',source:'core',source_label:'Core Lease',reference:'5.4',title:'Rental payment tax and interest'},sc={id:'sc:1',source:'special_conditions',source_label:'Special Conditions',reference:'SC-01',title:'PERMITTED USE'};
 const hash='heading-input-version';const index={entries:[core,sc],source_sha256:hash,notices:['Data Capture headings; not PDF verified.']};
 if(oldSelection)state.selected.push({...core,id:'old-pdf:legacy-clause',pdf_sha256:'old-pdf'});
 ids.get('lease-pdf-initial').textContent=JSON.stringify(indexFailure?{error:'Index unavailable'}:{index});ids.get('lease-pdf-state').textContent=JSON.stringify(state);ids.get('lease-pdf-csrf').value='csrf';
 const root=new Element('section');root.dataset={url:'/property/26/addendum?draft_id=current'};ids.set('addendum-pdf-selection',root);
 const requests=[];let htmlFailure=false,redirectFailure=false,reject=false;
 const response=value=>({ok:true,status:200,headers:{get:()=> 'application/json'},json:async()=>value});
 const sandbox={document:{getElementById:id=>ids.get(id),createElement:tag=>new Element(tag)},window:{location:{href:'https://ait.test/current'}},URL,FormData:class{constructor(){this.values=new Map();}set(k,v){this.values.set(k,v);}},fetch:async(url,options)=>{
  requests.push({url:String(url),options});const data=options.body.values;
  if(data.get('action')==='retry_clause_index')return response({results:index.entries,notices:index.notices,source_sha256:hash,state:JSON.parse(JSON.stringify(state))});
  if(htmlFailure)return {ok:false,status:400,headers:{get:()=> 'text/html'},json:async()=>{throw Error("Unexpected token '<'");}};
  if(redirectFailure)return {ok:false,status:0,type:'opaqueredirect'};
  if(reject)return {ok:false,status:409,headers:{get:()=> 'application/json'},json:async()=>({error:'The lease changed.'})};
  const id=data.get('provision_id');if(data.get('selected')==='yes'){const entry=id===core.id?core:sc;if(!state.selected.some(e=>e.id===hash+':'+id))state.selected.push({...entry,id:hash+':'+id,pdf_sha256:hash});}else state.selected=state.selected.filter(e=>e.id!==id);
  state.version++;return response({state:JSON.parse(JSON.stringify(state))});
 }};
 vm.runInNewContext(fs.readFileSync(rootPath+'/app/static/js/lease_addendum_pdf_search.js','utf8'),sandbox);
 assert.equal(requests.length,0,'Page load must not fetch or render PDFs or poll an index');
 if(indexFailure){assert.equal(ids.get('lease-pdf-retry-index').hidden,false);await ids.get('lease-pdf-retry-index').onclick();assert.equal(requests.length,1);assert.equal(requests[0].options.body.values.get('action'),'retry_clause_index');}
 const rows=()=>ids.get('lease-pdf-search-results').children;
 assert.equal(rows().length,2);assert.equal(rows()[0].children.length,2);
 assert.equal(ids.has('lease-pdf-search'),false);
 if(oldSelection)assert.equal(rows()[0].children[0].children[0].checked,true);
 await rows()[0].children[0].children[0].onchange.call(null); // event uses checkbox state
 // Explicitly check each box as a real change.
 let box=rows()[0].children[0].children[0];box.checked=true;await box.onchange();
 box=rows()[1].children[0].children[0];box.checked=true;await box.onchange();assert.equal(state.selected.length,2);
 assert.equal(rows().filter(row=>row.children[0].children[0].checked).length,2);
 box=rows()[0].children[0].children[0];box.checked=false;htmlFailure=true;await box.onchange();assert.match(ids.get('lease-pdf-search-status').textContent,/security token/);assert.doesNotMatch(ids.get('lease-pdf-search-status').textContent,/Unexpected token/);assert.equal(box.checked,false);
 let prevented=false;ids.get('lease-pdf-finish').onclick({preventDefault(){prevented=true;}});assert.equal(prevented,true);assert.equal(state.selected.length,2);
 htmlFailure=false;await ids.get('lease-pdf-retry').onclick();assert.equal(state.selected.length,1);
 box=rows()[1].children[0].children[0];box.checked=false;redirectFailure=true;await box.onchange();assert.match(ids.get('lease-pdf-search-status').textContent,/session or access/);redirectFailure=false;await ids.get('lease-pdf-retry').onclick();assert.equal(state.selected.length,0);
 prevented=false;ids.get('lease-pdf-finish').onclick({preventDefault(){prevented=true;}});assert.equal(prevented,false);
 for(const request of requests){assert.equal(request.options.method,'POST');assert.equal(request.options.credentials,'same-origin');assert.equal(request.options.headers['X-CSRFToken'],'csrf');}
 return 'PASS: immediate complete headings without search, multi-selection, manual retry, safe non-JSON failures and pending-selection retention';
};
