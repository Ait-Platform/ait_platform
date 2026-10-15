const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const group=fs.readFileSync(path.join(__dirname,'../templates/program_billing/lease_particular_group.html'),'utf8');
const workspace=fs.readFileSync(path.join(__dirname,'../templates/program_billing/lease_management.html'),'utf8');
let opened=0,closed=0,submitted=0;
const context={document:{getElementById:id=>{assert.equal(id,'lessee-dialog');return {showModal:()=>opened++,close:()=>closed++};}}};
for(const action of ['showModal','close']) {
 const handler=[...group.matchAll(/onclick="([^"]+)"/g)].map(match=>match[1]).find(code=>code.includes('.'+action+'('));
 assert.ok(handler);vm.runInNewContext(handler,context);
}
assert.equal(opened,1);assert.equal(closed,1);
const select=workspace.match(/id="lease-property" onchange="([^"]+)"/)[1];
vm.runInNewContext('(function(){'+select+'}).call(control)',{control:{form:{requestSubmit:()=>submitted++}}});
assert.equal(submitted,1);
assert.equal((group.match(/<dialog /g)||[]).length,1);
assert.ok(group.includes('name="draft_id"'));assert.ok(group.includes('name="version"'));
console.log('Lease property-selection and single lessee-modal event handlers passed.');
