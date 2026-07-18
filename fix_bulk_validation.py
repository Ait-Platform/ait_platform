with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    text = f.read()

import re
m = re.search(r'function gatherAccounts\(silent=false\) \{.*?return true;\n  \}', text, re.DOTALL)
if m:
    old_code = m.group(0)
    new_code = old_code.replace(
        'if (!hasBulk) { alert("Please select exactly one Bulk Account."); return false; }',
        'if ((IS_BULK_WATER || IS_BULK_ELEC) && !hasBulk) { alert("Please select exactly one Bulk Account."); return false; }'
    )
    
    # Also fix addAccountRow to default isBulk correctly
    text2 = text.replace(old_code, new_code)
    
    m2 = re.search(r'function addAccountRow\(accNum="", ownerName="", isBulk=false\) \{', text2)
    if m2:
        text2 = text2.replace(
            'function addAccountRow(accNum="", ownerName="", isBulk=false) {',
            'function addAccountRow(accNum="", ownerName="", isBulk=false) {\n    if (!IS_BULK_WATER && !IS_BULK_ELEC) isBulk = false;'
        )
        
    with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as fw:
        fw.write(text2)
    print('Fixed gatherAccounts bulk check')
else:
    print('Regex failed')
