with open('templates/program_billing/architecture_summary.html', 'r', encoding='utf-8') as f:
    text = f.read()

import re
m = re.search(r'<\!-- Step 2: Accounts -->(.*?)<\!-- Step 8: Readings -->', text, re.DOTALL)
if m:
    new_html = """<!-- Step 2: Accounts and Meters Map -->
        <div class="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden print:border-b print:shadow-none">
            <div class="bg-slate-50 px-6 py-3 border-b border-slate-200 flex justify-between items-center">
                <h2 class="text-lg font-bold text-slate-800">2. Accounts and Meters Structure</h2>
            </div>
            <div class="p-0">
                <table class="min-w-full divide-y divide-slate-200 text-sm">
                    <thead class="bg-slate-50">
                        <tr>
                            <th class="px-6 py-3 text-left text-xs font-bold text-slate-500 uppercase">Account Number</th>
                            <th class="px-6 py-3 text-left text-xs font-bold text-slate-500 uppercase">Water Meters</th>
                            <th class="px-6 py-3 text-left text-xs font-bold text-slate-500 uppercase">Electrical Meters</th>
                        </tr>
                    </thead>
                    <tbody class="divide-y divide-slate-200 bg-white">
                        {% for acc in accounts %}
                        <tr>
                            <td class="px-6 py-4 font-bold text-indigo-700 align-top">
                                {{ acc.account_number }}
                                {% if acc.is_bulk_account %}
                                    <span class="block mt-1 text-[10px] bg-amber-100 text-amber-800 px-2 py-0.5 rounded font-bold uppercase w-max border border-amber-300">Bulk Master</span>
                                {% endif %}
                            </td>
                            
                            {# Filter meters for this account #}
                            {% set acc_w_meters = [] %}
                            {% set acc_e_meters = [] %}
                            {% for m in meters if m.municipal_bill_number == acc.account_number %}
                                {% if 'water' in (m.utility_type|lower) %}
                                    {% set _ = acc_w_meters.append(m) %}
                                {% else %}
                                    {% set _ = acc_e_meters.append(m) %}
                                {% endif %}
                            {% endfor %}
                            
                            <td class="px-6 py-4 align-top">
                                {% if acc_w_meters|length > 0 %}
                                    <div class="space-y-1">
                                    {% for m in acc_w_meters %}
                                        <div class="inline-flex items-center px-2 py-1 bg-sky-50 text-sky-700 border border-sky-200 rounded text-xs font-medium mr-2 mb-1 shadow-sm">
                                            <svg class="w-3.5 h-3.5 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 15a4 4 0 004 4h9a5 5 0 10-.1-9.999 5.002 5.002 0 10-9.78 2.096A4.001 4.001 0 003 15z"></path></svg>
                                            {{ m.meter_number }}
                                        </div>
                                    {% endfor %}
                                    </div>
                                {% else %}
                                    <span class="text-slate-400 italic text-xs">No water meter linked</span>
                                {% endif %}
                            </td>
                            
                            <td class="px-6 py-4 align-top">
                                {% if acc_e_meters|length > 0 %}
                                    <div class="space-y-1">
                                    {% for m in acc_e_meters %}
                                        <div class="inline-flex items-center px-2 py-1 bg-amber-50 text-amber-700 border border-amber-200 rounded text-xs font-medium mr-2 mb-1 shadow-sm">
                                            <svg class="w-3.5 h-3.5 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"></path></svg>
                                            {{ m.meter_number }}
                                        </div>
                                    {% endfor %}
                                    </div>
                                {% else %}
                                    <span class="text-slate-400 italic text-xs">No electrical meter linked</span>
                                {% endif %}
                            </td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
        </div>
        
        {# Unlinked / Exceptional Meters #}
        {% set unlinked_meters = [] %}
        {% for m in meters if not m.municipal_bill_number %}
            {% set _ = unlinked_meters.append(m) %}
        {% endfor %}
        
        {% if unlinked_meters|length > 0 %}
        <div class="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden print:border-b print:shadow-none mt-6">
            <div class="bg-rose-50 px-6 py-3 border-b border-rose-100 flex justify-between items-center">
                <h2 class="text-lg font-bold text-rose-800">Exceptions (Unlinked Meters)</h2>
            </div>
            <div class="p-4 flex flex-wrap gap-2">
                {% for m in unlinked_meters %}
                    <span class="px-3 py-1 bg-white text-rose-700 font-medium rounded-lg border border-rose-300 shadow-sm text-sm">
                        {{ m.meter_number }} ({{ m.utility_type }})
                    </span>
                {% endfor %}
            </div>
        </div>
        {% endif %}

        <!-- Step 8: Readings -->"""
    
    text = text.replace(m.group(0), new_html)
    with open('templates/program_billing/architecture_summary.html', 'w', encoding='utf-8') as fw:
        fw.write(text)
    print('Fixed grouped meters view')
else:
    print('Regex failed')
