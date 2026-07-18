import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_recalc = """  function recalculateRows() {
      adjustContainer('accounts-container', EXPECTED_ACCOUNTS, () => addAccountRow());
      adjustContainer('bulk-water-container', EXPECTED_BULK_WATER, (cid) => addMeterRow(cid));
      adjustContainer('bulk-elec-container', EXPECTED_BULK_ELEC, (cid) => addMeterRow(cid));
      adjustContainer('sub-water-container', EXPECTED_SUB_WATER, (cid) => addMeterRow(cid));
      adjustContainer('sub-elec-container', EXPECTED_SUB_ELEC, (cid) => addMeterRow(cid));
  }"""

new_recalc = """  function recalculateRows() {
      adjustContainer('accounts-container', EXPECTED_ACCOUNTS, () => addAccountRow());
      adjustContainer('bulk-water-container', EXPECTED_BULK_WATER, () => addMeterRow('bulk-water'));
      adjustContainer('bulk-elec-container', EXPECTED_BULK_ELEC, () => addMeterRow('bulk-elec'));
      adjustContainer('sub-water-container', EXPECTED_SUB_WATER, () => addMeterRow('sub-water'));
      adjustContainer('sub-elec-container', EXPECTED_SUB_ELEC, () => addMeterRow('sub-elec'));
  }"""

if old_recalc in content:
    content = content.replace(old_recalc, new_recalc)
    with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print("recalculateRows fixed.")
else:
    print("Could not find old recalculateRows logic.")
