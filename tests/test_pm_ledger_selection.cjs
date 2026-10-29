const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync('app/static/js/pm_ledger_selection.js', 'utf8');
const callbacks = {};
const radios = [0,1,2].map((_, i) => ({checked:false, disabled:i===2, addEventListener(name, fn) {this[name]=fn;}}));
const button = {disabled:true};
const form = {addEventListener(name, fn) {callbacks[name]=fn;}};
let pageshow;
vm.runInNewContext(source, {document:{getElementById(id){return id==='property-ledger-form'?form:button;},querySelectorAll(){return radios;}}, window:{addEventListener(name, fn){pageshow=fn;}}});
assert.equal(button.disabled,true);
let prevented=false;callbacks.submit({preventDefault(){prevented=true;}});assert.equal(prevented,true);
radios[0].checked=true;radios[0].change();assert.equal(button.disabled,false);
prevented=false;callbacks.submit({preventDefault(){prevented=true;}});assert.equal(prevented,false);
// Browser radio grouping is guaranteed by the same name and form in rendered HTML.
radios[0].checked=false;radios[1].checked=true;radios[1].change();assert.equal(button.disabled,false);
radios[1].checked=false;radios[2].checked=true;pageshow();assert.equal(button.disabled,true);
vm.runInNewContext(source,{document:{getElementById(){return null;}},window:{}});
console.log('Property ledger selection controls: passed');
