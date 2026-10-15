// Execute the actual modal script with a minimal DOM; no database or network.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../templates/program_billing/owner_assignment_preview.html'), 'utf8');
const script = source.split('<script>')[1].split('</script>')[0];
class Element {
  constructor(dataset = {}) { this.dataset = dataset; this.listeners = {}; this.value = ''; this.checked = false; this.readOnly = false; this.children = []; this.textContent = ''; }
  addEventListener(name, callback) { (this.listeners[name] ||= []).push(callback); }
  fire(name) { let result; for (const callback of this.listeners[name] || []) result = callback(); return result; }
  closest() { return this; }
  focus() {}
  showModal() { this.open = true; }
  close() { this.open = false; }
  reportValidity() { return !['name','type'].includes(this.dataset.ownerField) || Boolean(this.value.trim()); }
  replaceChildren() { this.children = []; }
  appendChild(child) { this.children.push(child); }
  setAttribute(name, value) { this[name] = value; }
}
const keys = ['type', 'name', 'identity_number', 'email', 'phone', 'percentage', 'residential_address', 'postal_address'];
const fields = keys.map(key => new Element({ownerField: key}));
const ids = {};
for (const [i, key] of keys.entries()) ids['owner-' + (key === 'type' ? 'kind' : key)] = fields[i];
for (const id of ['owner-assignment-preview','owner-active','owner-postal-same','owner-property-name','owner-preview-list','owner-preview-message','owner-preview-close','owner-add-preview','owner-save-disabled','owner-existing-selection','owner-existing-percentage','owner-search']) ids[id] = new Element();
const opener = new Element({propertyId: '26', propertyName: 'Almond'});
const tabs = [new Element({ownerTab: 'new'}), new Element({ownerTab: 'existing'})];
const panels = [new Element({ownerPanel: 'new'}), new Element({ownerPanel: 'existing'})];
ids['owner-assignment-preview'].querySelectorAll = selector => selector === '[data-owner-field]' ? fields : selector === '[data-owner-panel]' ? panels : tabs;
const document = {getElementById: id => ids[id], querySelectorAll: selector => selector === '[data-open-owner]' ? [opener] : [], createElement: () => new Element()};
vm.runInNewContext(script, {document, Map, Array, Object, String});
opener.fire('click');
const same = ids['owner-postal-same'], residential = ids['owner-residential_address'], postal = ids['owner-postal_address'];
residential.value = 'Residential address'; postal.value = 'Separate postal'; same.checked = true; same.fire('change');
assert.equal(postal.value, 'Residential address'); assert.equal(postal.readOnly, true);
residential.value = 'Updated residential'; residential.fire('input'); assert.equal(postal.value, 'Updated residential');
same.checked = false; same.fire('change'); assert.equal(postal.readOnly, false);
postal.value = 'PO Box 1'; residential.value = 'Different residential'; residential.fire('input'); assert.equal(postal.value, 'PO Box 1');
fields[0].value='individual'; fields[1].value='Not saved';
ids['owner-add-preview'].fire('click');
assert.equal(fields[1].value,'Not saved'); // gate never clears unsaved details
assert.equal(panels[0].hidden,false); assert.equal(panels[1].hidden,false);
ids['owner-preview-close'].fire('click'); opener.fire('click'); assert.equal(same.checked, false); assert.equal(postal.readOnly, false);
assert.ok(source.includes('{% if not owners_enabled %}disabled{% endif %} id="owner-save-disabled"')); assert.match(source, />Save Changes<\/button>/);
ids['owner-save-disabled'].fire('click'); assert.equal(ids['owner-assignment-preview'].open, true); // gate blocks saving in preview
console.log('Owner modal checks passed: postal copy/sync, separate address, resets, disabled save-and-add, tabs and disabled saving.');

module.exports = {Element, script};
