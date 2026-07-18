with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

import re
m = re.search(r'for mp in data\.get\(\'mapping\', \[\]\):.*?if e_id:\s*meter_acc\[e_id\] = acc_map\[acc_id\]', text, re.DOTALL)
if m:
    new_code = m.group(0) + """
        
        # Auto-link loosely if only 1 account exists
        if len(acc_map) == 1:
            only_acc_num = list(acc_map.values())[0]
            for m_item in data.get('subWater', []):
                meter_acc[m_item.get('id')] = only_acc_num
            for m_item in data.get('subElec', []):
                meter_acc[m_item.get('id')] = only_acc_num
    """
    text = text.replace(m.group(0), new_code)
    with open('app/program_billing/routes.py', 'w', encoding='utf-8') as fw:
        fw.write(text)
    print('Fixed auto-mapping for single accounts')
else:
    print('Regex failed')
