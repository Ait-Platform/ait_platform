const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
module.exports=function(root){
 const controls=new Map(),links=[],windowHandlers={},locations=[];
 function element(id){const e={id,value:'',dataset:{},textContent:'',listeners:{},addEventListener(name,handler){this.listeners[name]=handler;},showModal(){this.open=true;},close(){this.open=false;}};controls.set(id,e);return e;}
 const field={name:'PROPERTY_ADDRESS',value:'Original address',type:'textarea'},form=element('certificate-form');form.querySelectorAll=()=>[field];form.reportValidity=()=>true;let submissions=0;form.requestSubmit=()=>submissions++;
 for(const id of ['capture-unsaved-dialog','capture-save-back','capture-discard-back','capture-return','capture-return-section','capture-save'])element(id);
 const target={href:'/capture/26/signing?draft_id=selected',dataset:{captureSection:'signing'},listeners:{},addEventListener(name,handler){this.listeners[name]=handler;}};links.push(target);
 const document={getElementById:id=>controls.get(id)||null,querySelector:()=>target,querySelectorAll:selector=>selector.includes('data-capture-back')?links:[]};
 const source=fs.readFileSync(root+'/app/static/js/lease_capture.js','utf8');vm.runInNewContext(source.slice(0,source.indexOf('  const commencement ='))+'}());',{document,window:{location:{assign:url=>locations.push(url)},addEventListener(name,handler){windowHandlers[name]=handler;}}});
 field.value='Entered address';let prevented=false;target.listeners.click({preventDefault(){prevented=true;}});assert.equal(prevented,true);assert.equal(controls.get('capture-unsaved-dialog').open,true);assert.equal(field.value,'Entered address');assert.equal(locations.length,0);
 controls.get('capture-save-back').listeners.click();assert.equal(submissions,1);assert.equal(controls.get('capture-return').value,'section');assert.equal(controls.get('capture-return-section').value,'signing');assert.equal(field.value,'Entered address');
 form.listeners.submit();let unload=false;windowHandlers.beforeunload({preventDefault(){unload=true;}});assert.equal(unload,false,'Successful form navigation must not be blocked');
 target.listeners.click({preventDefault(){}});controls.get('capture-discard-back').listeners.click();assert.deepEqual(locations,[target.href]);
 return 'PASS: unsaved navigation retains fields, uses the existing Save, and only discards explicitly';
};
