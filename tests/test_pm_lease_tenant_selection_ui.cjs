const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
module.exports=async function run(rootPath){
 const ids=new Map(),listeners={},groups={children:[],hidden:true,addEventListener(){},appendChild(group){this.children.push(group);}};
 const generic=()=>({value:'',hidden:false,dataset:{},textContent:'',children:[],addEventListener(name,fn){this[name]=fn;},appendChild(child){this.children.push(child);},append(...children){this.children.push(...children);},replaceChildren(){this.children=[];},focus(){},querySelectorAll(){return[];}});
 const form=generic();form.dataset={};form.reportValidity=()=>true;form.addEventListener=(name,fn)=>{(listeners[name]??=[]).push(fn);};
 form.querySelectorAll=()=>groups.children.flatMap(group=>[group.register,group.type,...group.inputs]);
 function makeGroup(index){
  const group=generic();group.dataset={lesseeIndex:String(index)};
  group.inputs=['LEGAL_NAME','IDENTITY_NUMBER','SIGNATORY_NAME','SIGNATORY_IDENTITY_NUMBER','ADDRESS','EMAIL','PHONE'].map(key=>{const input=generic();input.name='LESSEE_'+index+'_'+key;ids.set('field_'+input.name,input);return input;});
  group.type=generic();group.type.name='LESSEE_'+index+'_PARTY_TYPE';
  group.register=generic();group.register.name='tenant_register_id_'+index;group.register.dataset.field='LESSEE_'+index+'_LEGAL_NAME';
  group.register.closest=selector=>selector==='.register-choice'?group.register:group;
  group.register.matches=()=>false;
  group.register.dispatchEvent=()=>{for(const fn of listeners.change||[])fn({target:group.register});};
  group.rep=generic();group.rep.querySelectorAll=()=>group.inputs.filter(input=>input.name.includes('_SIGNATORY_'));
  const labels={LEGAL_NAME:generic(),IDENTITY_NUMBER:generic()},legend=generic();
  group.querySelector=selector=>selector==='.register-choice'?group.register:selector==='[data-lessee-type]'?group.type:selector==='.lessee-representative'?group.rep:selector==='legend'?legend:selector==='textarea'?group.inputs[0]:labels[/data-lessee-label="([^"]+)"/.exec(selector)?.[1]];
  group.querySelectorAll=selector=>selector==='textarea'?group.inputs:[group.register,group.type,...group.inputs];
  group.remove=()=>{groups.children.splice(groups.children.indexOf(group),1);};
  for(const input of [group.register,group.type,...group.inputs]){
   input.getAttribute=name=>input[name]||null;
   input.setAttribute=(name,value)=>{if(name==='id')ids.delete(input.id);input[name]=value;if(name==='id')ids.set(value,input);};
  }
  for(const input of group.inputs)input.id='field_'+input.name;
  return group;
 }
 const natural={id:11,party_type:'natural_person',legal_name:'Natural Lessee',identity_number:'NAT-11'};
 const entity={id:22,party_type:'juristic_person',legal_name:'Entity Lessee',identity_number:'REG-22',signatory_name:'Entity Representative',signatory_identity_number:'REP-33'};
 groups.children.push(makeGroup(1));ids.set('certificate-form',form);ids.set('lessee-count',{value:'1'});ids.set('capture-lessees',groups);
 ids.set('capture-lessee-template',{innerHTML:'<fieldset>LESSEE_1_LEGAL_NAME tenant_register_id_1</fieldset>'});ids.set('capture-tenants',{textContent:JSON.stringify([natural,entity])});
 for(const id of ['capture-save-back','capture-discard-back','party-tenant-choice','show-lessors','select-tenant','add-lessee','party-initial-message','party-lessors','party-tenant-selection','party-selected-details','party-tenant-status','party-current-lessees'])ids.set(id,generic());
 ids.set('tenant-selection-changed',generic());
 const document={getElementById:id=>ids.get(id)||null,querySelector:()=>null,querySelectorAll:()=>[],createElement(tag){if(tag!=='template')return generic();const holder={content:{}};Object.defineProperty(holder,'innerHTML',{set(value){holder.content.firstElementChild=makeGroup(Number(/LESSEE_(\d+)_/.exec(value)[1]));}});return holder;}};
 const sandbox={document,window:{addEventListener(){}},console,Event:class {},JSON,Array,Set,String,Object};
 vm.runInNewContext(fs.readFileSync(rootPath+'/app/static/js/lease_capture.js','utf8'),sandbox);
 const choice=ids.get('party-tenant-choice');
 const choose=value=>{choice.value=value;choice.change();};
 choose('11');
 assert.equal(groups.children.length,1);assert.equal(groups.children[0].register.value,'11');
 assert.equal(groups.children[0].inputs[0].value,'Natural Lessee');assert.equal(groups.children[0].rep.hidden,true);
 assert.equal(ids.get('tenant-selection-changed').value,'1');
 choose('22');
 assert.equal(groups.children.length,1,'Choosing another tenant replaces the prior selection');
 assert.equal(groups.children[0].register.value,'22');assert.equal(groups.children[0].type.value,'juristic_person');
 assert.equal(groups.children[0].inputs[0].value,'Entity Lessee');assert.equal(groups.children[0].rep.hidden,false);
 choose('11');choose('11');
 assert.equal(groups.children.length,1);assert.equal(ids.get('lessee-count').value,'1');
 const posted=Object.fromEntries(form.querySelectorAll().map(input=>[input.name,input.value]));
 assert.equal(posted.tenant_register_id_1,'11');assert.equal(posted.LESSEE_1_LEGAL_NAME,'Natural Lessee');
 assert.equal(posted.tenant_register_id_2,undefined);assert.equal(groups.children[0].rep.hidden,true);
 ids.get('party-current-lessees').children[0].children[2].click();
 assert.equal(choice.value,'');assert.equal(groups.children[0].register.value,'');
 assert.equal(JSON.parse(ids.get('capture-tenants').textContent).length,2,'Master tenant choices are preserved');
 console.log('Restored dropdown replaces the lease selection; natural and juristic representative rules pass.');
};
