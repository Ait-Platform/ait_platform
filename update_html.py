with open('templates/program_billing/architecture_summary.html', 'r', encoding='utf-8') as f:
    text = f.read()

import re
old_block = """{# Filter meters for this account #}
                            {% set acc_w_meters = [] %}
                            {% set acc_e_meters = [] %}
                            {% for m in meters if m.municipal_bill_number == acc.account_number %}
                                {% if 'water' in (m.utility_type|lower) %}
                                    {% set _ = acc_w_meters.append(m) %}
                                {% else %}
                                    {% set _ = acc_e_meters.append(m) %}
                                {% endif %}
                            {% endfor %}"""

new_block = """{# Filter meters for this account from precomputed map #}
                            {% set acc_w_meters = account_meters[acc.account_number]['water'] %}
                            {% set acc_e_meters = account_meters[acc.account_number]['elec'] %}"""

text = text.replace(old_block, new_block)

with open('templates/program_billing/architecture_summary.html', 'w', encoding='utf-8') as fw:
    fw.write(text)

print('Updated HTML')
