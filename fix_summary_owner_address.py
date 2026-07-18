with open('templates/program_billing/architecture_summary.html', 'r', encoding='utf-8') as f:
    text = f.read()

import re
m = re.search(r'<\!-- Step 12: Owners -->(.*?)</div>\s*</div>\s*</div>', text, re.DOTALL)
if m:
    new_html = m.group(0).replace(
        '<th class="px-6 py-3 text-left text-xs font-bold text-slate-500 uppercase">Email</th>',
        '<th class="px-6 py-3 text-left text-xs font-bold text-slate-500 uppercase">Email</th>\n                            <th class="px-6 py-3 text-left text-xs font-bold text-slate-500 uppercase">Address</th>'
    )
    new_html = new_html.replace(
        '<td class="px-6 py-3 text-slate-500">{{ o.email_address or \'-\' }}</td>',
        '<td class="px-6 py-3 text-slate-500">{{ o.email_address or \'-\' }}</td>\n                            <td class="px-6 py-3 text-slate-500">{{ o.address or \'-\' }}</td>'
    )
    text = text.replace(m.group(0), new_html)
    with open('templates/program_billing/architecture_summary.html', 'w', encoding='utf-8') as fw:
        fw.write(text)
    print('Fixed owner address column')
else:
    print('Regex failed')
