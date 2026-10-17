const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const elements = {};
function element(id, values={}) {
  return elements[id] = Object.assign({value:'',hidden:false,disabled:false,textContent:'',events:{},
    addEventListener(type,callback){this.events[type]=callback;}},values);
}
const rows = ['1','2','3'].map(id => {
  const input={disabled:true};
  return {dataset:{lessorRow:id},hidden:true,querySelector:()=>input,input};
});
const remove = rows.map(row => element('remove-'+row.dataset.lessorRow,{dataset:{removeLessor:row.dataset.lessorRow}}));
const choice=element('lease-owner-choice',{options:[{value:''},{value:'1'},{value:'2'},{value:'3'}]});
element('lease-add-owner');element('lease-lessor-empty');element('lease-owner-message');
const rep=element('lessor-representative-owner',{selectedIndex:0,options:[{value:''},{value:'1',dataset:{name:'Owner One',identity:'SYNTHETIC-OWNER-ID'}}]});
element('lessor-signatory_name');element('lessor-signatory_identity_number');element('lessor-signatory_capacity',{value:'Explicit entered capacity'});
const type=element('lessee-party_type',{value:'natural_person'});const repFields=element('lessee-representative-fields');
const address=element('lessor-service_address');const warning=element('lessor-notice-warning');
const nameLabel={textContent:''},idLabel={textContent:''};
const document={getElementById:id=>{assert.ok(elements[id],id);return elements[id];},
  querySelectorAll:selector=>selector==='[data-lessor-row]'?rows:remove,
  querySelector:selector=>selector.includes('legal_name')?nameLabel:idLabel};
vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../app/static/js/pm_lease_parties.js'),'utf8'),{document});
assert.equal(repFields.hidden,true);assert.equal(repFields.disabled,true);assert.equal(nameLabel.textContent,'Full name');
elements['lease-add-owner'].events.click();assert.ok(elements['lease-owner-message'].textContent.includes('Choose'));
for(const id of ['1','2','3']){choice.value=id;elements['lease-add-owner'].events.click();}
assert.equal(rows.filter(row=>!row.hidden).length,3);assert.ok(rows.every(row=>!row.input.disabled));assert.equal(elements['lease-lessor-empty'].hidden,true);
assert.ok(choice.options.slice(1).every(option=>option.disabled));
rep.value='1';rep.selectedIndex=1;rep.events.change();
assert.equal(elements['lessor-signatory_name'].value,'Owner One');assert.equal(elements['lessor-signatory_identity_number'].value,'SYNTHETIC-OWNER-ID');
assert.equal(elements['lessor-signatory_capacity'].value,'Explicit entered capacity');
remove[0].events.click();assert.equal(rows[0].hidden,true);assert.equal(rows[0].input.disabled,true);assert.equal(choice.options[1].disabled,false);assert.equal(rep.value,'');
choice.value='1';elements['lease-add-owner'].events.click();assert.equal(rows[0].hidden,false);
type.value='juristic_person';type.events.change();assert.equal(repFields.hidden,false);assert.equal(repFields.disabled,false);assert.equal(nameLabel.textContent,'Registered entity name');assert.equal(idLabel.textContent,'Registration number');
type.value='natural_person';type.events.change();assert.equal(repFields.hidden,true);assert.equal(repFields.disabled,true);
address.value='Confirmed address';address.events.input();assert.equal(warning.hidden,true);address.value=' ';address.events.input();assert.equal(warning.hidden,false);
const template=fs.readFileSync(path.join(__dirname,'../templates/program_billing/lease_parties_structured.html'),'utf8');
assert.ok(!template.includes('name="confirm_lessors"'));assert.ok(!template.includes('name="signatures_'));
assert.ok(template.includes('pm_lease_parties.js'));assert.ok(template.includes('Lessor service address not supplied'));
console.log('Part A frontend: add/remove three owners, owner representative, natural/juristic fields and address warnings passed.');
