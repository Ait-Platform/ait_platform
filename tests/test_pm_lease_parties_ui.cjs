const fs=require('node:fs'),vm=require('node:vm'),path=require('node:path'),assert=require('node:assert/strict');
function checkbox(value,checked=false,disabled=false){return {value,checked,disabled,listeners:{},addEventListener(event,fn){this.listeners[event]=fn;}};}
const owners=[checkbox('owner-1'),checkbox('owner-2'),checkbox('owner-disabled',false,true)];
const tenants=[checkbox('1',true),checkbox('2'),checkbox('3')];
const templates=tenants.map(box=>({dataset:{tenantRow:box.value},content:{cloneNode(){return {tenantId:box.value};}}}));
const empty={hidden:false};
const body={children:[],listeners:{},replaceChildren(...rows){this.children=rows;},addEventListener(event,fn){this.listeners[event]=fn;},contains(button){return this.children.some(row=>row.tenantId===button.dataset.removeTenant);}};
const document={querySelectorAll(selector){if(selector.includes('representative_owner_id'))return owners;if(selector.includes('tenant_ids'))return tenants;return templates;},getElementById(id){return id==='selected-tenants-body'?body:empty;}};
vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../app/static/js/pm_lease_parties.js'),'utf8'),{document});
assert.deepEqual(body.children.map(row=>row.tenantId),['1']);assert.equal(empty.hidden,true);
owners[0].checked=true;owners[0].listeners.change();owners[1].checked=true;owners[1].listeners.change();
assert.deepEqual(owners.map(box=>box.checked),[false,true,false]);
owners[1].checked=false;owners[1].listeners.change();assert.equal(owners[1].checked,false);
tenants[2].checked=true;tenants[2].listeners.change();assert.deepEqual(body.children.map(row=>row.tenantId),['1','3']);
function remove(id){const button={dataset:{removeTenant:id}};body.listeners.click({target:{closest:()=>button}});}
remove('1');assert.equal(tenants[0].checked,false);assert.deepEqual(body.children.map(row=>row.tenantId),['3']);
remove('3');assert.deepEqual(body.children,[]);assert.equal(empty.hidden,false);
const template=fs.readFileSync(path.join(__dirname,'../templates/program_billing/lease_parties_structured.html'),'utf8');
assert.ok(template.indexOf('Add Tenant / View Tenant List')<template.indexOf('id="available-tenants"'));
assert.ok(template.includes('id="selected-tenants-body"'));assert.ok(template.includes('tenant.representative_phone'));assert.ok(template.includes('tenant.representative_email'));
for(const removed of ['owner-capacity-confirmation','lessee-dialog','name="lessor_signatory_name"'])assert.ok(!template.includes(removed));
console.log('Part A checkbox toggling, selected-only rows, add/remove and representative display: PASS');
