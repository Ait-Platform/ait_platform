const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
module.exports=async function(root){
 const ids=new Map();function node(){return {hidden:true,disabled:false,checked:false,textContent:'',removeAttribute(){this.src='';},showModal(){this.open=true;},close(){this.open=false;},addEventListener(){}};}
 for(const id of ['sc-pdf-dialog','sc-pdf-form','sc-pdf-status','sc-pdf-preview','sc-pdf-confirm','sc-pdf-approved','sc-pdf-approval','sc-pdf-open','sc-pdf-cancel'])ids.set(id,node());
 const file=new Blob(['%PDF-synthetic'],{type:'application/pdf'}),form=ids.get('sc-pdf-form');form.elements={source:{files:[file]}};form.reset=()=>{};ids.get('sc-pdf-dialog').dataset={url:'/billing/leases/property/26/history?draft_id=selected'};
 const requests=[],locations=[],revoked=[];let deny=false;
 const sandbox={Blob,document:{getElementById:id=>ids.get(id)||null},URL:{createObjectURL:()=> 'blob:private-preview',revokeObjectURL:url=>revoked.push(url)},window:{location:{assign:url=>locations.push(url)},addEventListener(){}},FormData:class{constructor(){this.values=new Map();}set(k,v){this.values.set(k,v);}},fetch:async(url,options)=>{requests.push({url,...options});return deny?{ok:false,status:400,json:async()=>({error:'Upload a valid unencrypted PDF.'})}:{ok:true,json:async()=>options.body.values.get('action')==='sc_pdf_preview'?{token:'bound-token',pages:2}:{saved:true,url:'/history?draft_id=selected'}};}};
 vm.runInNewContext(fs.readFileSync(root+'/app/static/js/lease_sc_pdf.js','utf8'),sandbox);
 ids.get('sc-pdf-open').onclick();assert.equal(ids.get('sc-pdf-dialog').open,true);assert.equal(ids.get('sc-pdf-confirm').disabled,true);
 await form.onsubmit({preventDefault(){}});assert.equal(requests.length,1);assert.equal(locations.length,0);assert.equal(ids.get('sc-pdf-preview').src,'blob:private-preview');assert.equal(ids.get('sc-pdf-confirm').disabled,true);
 ids.get('sc-pdf-approved').checked=true;ids.get('sc-pdf-approved').onchange();assert.equal(ids.get('sc-pdf-confirm').disabled,true,'Preview must load before confirmation');
 ids.get('sc-pdf-preview').onload();assert.equal(ids.get('sc-pdf-confirm').disabled,false);
 await ids.get('sc-pdf-confirm').onclick();assert.equal(requests[1].body.values.get('source'),file);assert.equal(requests[1].body.values.get('confirmed'),'yes');assert.equal(requests[1].body.values.get('token'),'bound-token');assert.equal(locations.length,1);
 ids.get('sc-pdf-cancel').onclick();assert.equal(ids.get('sc-pdf-dialog').open,false);assert.ok(revoked.length);
 ids.get('sc-pdf-open').onclick();deny=true;await form.onsubmit({preventDefault(){}});assert.equal(ids.get('sc-pdf-status').textContent,'Upload a valid unencrypted PDF.');assert.equal(ids.get('sc-pdf-confirm').disabled,true);
 console.log('PDF modal passed: preview, explicit approval, confirmation, cancellation and specific errors.');
};
