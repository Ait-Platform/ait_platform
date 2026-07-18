import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_gather = """  function gatherAccounts(silent=false) {
    wizardData.accounts = [];
    const rows = document.querySelectorAll('.acc-row');
    let hasBulk = false;
    
    rows.forEach((row, i) => {
      const accNum = row.querySelector('.acc-num').value.trim();
      const owner = row.querySelector('.acc-owner').value.trim();
      const isBulk = row.querySelector('input[name="bulk_idx"]').checked;
      if (isBulk) hasBulk = true;
      
      wizardData.accounts.push({ id: `acc_${i}`, number: accNum, owner: owner, isBulk: isBulk });
    });

    if (!silent) {
      if (wizardData.accounts.length === 0) { alert("Please enter accounts."); return false; }
      if (!hasBulk) { alert("Please select exactly one Bulk Account."); return false; }
    }
    return true;
  }"""

new_gather = """  function gatherAccounts(silent=false) {
    wizardData.accounts = [];
    const rows = document.querySelectorAll('.acc-row');
    let hasBulk = false;
    let accNumbers = new Set();
    let hasDuplicate = false;
    
    rows.forEach((row, i) => {
      const accNum = row.querySelector('.acc-num').value.trim();
      const owner = row.querySelector('.acc-owner').value.trim();
      const isBulk = row.querySelector('input[name="bulk_idx"]').checked;
      if (isBulk) hasBulk = true;
      
      if (accNum !== "") {
        if (accNumbers.has(accNum)) {
          hasDuplicate = true;
        }
        accNumbers.add(accNum);
      }
      
      wizardData.accounts.push({ id: `acc_${i}`, number: accNum, owner: owner, isBulk: isBulk });
    });

    if (!silent) {
      if (wizardData.accounts.length === 0) { alert("Please enter accounts."); return false; }
      if (!hasBulk) { alert("Please select exactly one Bulk Account."); return false; }
      if (hasDuplicate) { alert("Duplicate account numbers found! Each account must be unique. Please correct this before proceeding."); return false; }
    }
    return true;
  }"""

if old_gather in content:
    content = content.replace(old_gather, new_gather)
    with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Updated setup_wizard.html")
else:
    print("Could not find gatherAccounts block.")
