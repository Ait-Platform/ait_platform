const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
module.exports=async function(root){
 const ids=new Map();function node(){return {hidden:true,disabled:false,textContent:'',appendChild(child){this.child=child;},showModal(){this.open=true;},close(){this.open=false;},addEventListener(){}};}
 for(const id of ['sc-pdf-dialog','sc-pdf-form','sc-pdf-status','sc-pdf-upload','sc-pdf-success','sc-pdf-word','sc-pdf-open','sc-pdf-cancel'])ids.set(id,node());
 const form=ids.get('sc-pdf-form');let resets=0;form.reset=()=>resets++;ids.get('sc-pdf-dialog').dataset={url:'/billing/leases/property/26/history?draft_id=selected'};
 const requests=[];let deny=false;
 const sandbox={document:{getElementById:id=>ids.get(id)||null,createElement:()=>node()},FormData:class{constructor(form){this.form=form;this.values=new Map();}set(k,v){this.values.set(k,v);}},fetch:async(url,options)=>{requests.push({url,...options});return deny?{ok:false,status:503,json:async()=>({error:'Private PDF storage failed: private storage access denied (AccessDenied). The current PDF is unchanged.'})}:{ok:true,json:async()=>({saved:true,message:'Special Conditions PDF uploaded and saved.',pdf_url:'/private/current.pdf',word_url:'/private/current.docx'})};}};
 vm.runInNewContext(fs.readFileSync(root+'/app/static/js/lease_sc_pdf.js','utf8'),sandbox);
 ids.get('sc-pdf-open').onclick();assert.equal(ids.get('sc-pdf-dialog').open,true);
 await form.onsubmit({preventDefault(){}});assert.equal(requests.length,1);assert.equal(requests[0].body.values.get('action'),'sc_pdf_upload');assert.equal(requests[0].credentials,'same-origin');assert.equal(requests[0].body.form,form);
 assert.equal(ids.get('sc-pdf-dialog').open,false);assert.equal(ids.get('sc-pdf-success').child.href,'/private/current.pdf');assert.equal(ids.get('sc-pdf-success').child.textContent,'Open uploaded PDF');assert.equal(ids.get('sc-pdf-word').href,'/private/current.docx');assert.equal(ids.get('sc-pdf-word').hidden,false);
 ids.get('sc-pdf-open').onclick();deny=true;await form.onsubmit({preventDefault(){}});assert.equal(ids.get('sc-pdf-dialog').open,true);assert.match(ids.get('sc-pdf-status').textContent,/AccessDenied/);assert.equal(ids.get('sc-pdf-upload').disabled,false);assert.equal(ids.get('sc-pdf-word').href,'/private/current.docx');assert.equal(resets,1,'Failure retains the selected file');
 ids.get('sc-pdf-cancel').onclick();assert.equal(ids.get('sc-pdf-dialog').open,false);assert.equal(requests.length,2);
 console.log('PDF UI passed: one upload request, immediate saved link, current Word link, detailed errors and cancellation.');
};
