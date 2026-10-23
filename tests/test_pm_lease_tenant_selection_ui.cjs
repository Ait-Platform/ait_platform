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
  group.querySelectorAll=()=>group.inputs;
  return group;
 }
 const natural={id:11,party_type:'natural_person',legal_name:'Natural Lessee',identity_number:'NAT-11'};
 const entity={id:22,party_type:'juristic_person',legal_name:'Entity Lessee',identity_number:'REG-22',signatory_name:'Entity Representative',signatory_identity_number:'REP-33'};
 groups.children.push(makeGroup(1));ids.set('certificate-form',form);ids.set('lessee-count',{value:'1'});ids.set('capture-lessees',groups);
 ids.set('capture-lessee-template',{innerHTML:'<fieldset>LESSEE_1_LEGAL_NAME tenant_register_id_1</fieldset>'});ids.set('capture-tenants',{textContent:JSON.stringify([natural,entity])});
 for(const id of ['capture-save-back','capture-discard-back','party-tenant-choice','show-lessors','select-tenant','add-lessee','party-initial-message','party-lessors','party-tenant-selection','party-selected-details','party-tenant-status'])ids.set(id,generic());
 const document={getElementById:id=>ids.get(id)||null,querySelector:()=>null,querySelectorAll:()=>[],createElement(tag){if(tag!=='template')return generic();const holder={content:{}};Object.defineProperty(holder,'innerHTML',{set(value){holder.content.firstElementChild=makeGroup(Number(/LESSEE_(\d+)_/.exec(value)[1]));}});return holder;}};
 const sandbox={document,window:{addEventListener(){}},console,Event:class {},JSON,Array,Set,String,Object};
 vm.runInNewContext(fs.readFileSync(rootPath+'/app/static/js/lease_capture.js','utf8'),sandbox);
 const choice=ids.get('party-tenant-choice');choice.value='11';choice.change();
 choice.value='22';choice.change();
 assert.equal(groups.children.length,2);assert.equal(ids.get('lessee-count').value,'2');
 assert.equal(ids.get('field_LESSEE_1_LEGAL_NAME').value,'Natural Lessee');assert.equal(ids.get('field_LESSEE_2_LEGAL_NAME').value,'Entity Lessee');
 assert.equal(groups.children[0].type.value,'natural_person');assert.equal(groups.children[1].type.value,'juristic_person');
 assert.equal(groups.children[0].rep.hidden,true);assert.equal(groups.children[1].rep.hidden,false);
 choice.value='11';choice.change();choice.value='22';choice.change();assert.equal(groups.children.length,2,'Repeated selections must reuse their own existing slot');
 const posted=Object.fromEntries(form.querySelectorAll().map(input=>[input.name,input.value]));
 assert.equal(posted.tenant_register_id_1,'11');assert.equal(posted.tenant_register_id_2,'22');
 console.log('Tenant selection regression passed: separate natural/juristic slots, correct form payload, no duplicate re-selection, conditional representatives.');
};
