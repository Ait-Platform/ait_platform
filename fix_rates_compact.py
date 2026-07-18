with open('templates/program_billing/architecture_summary.html', 'r', encoding='utf-8') as f:
    text = f.read()

import re
rates_block = re.search(r'<\!-- Step 10: Rates -->(.*?)<\!-- Step 11: Arrangements -->', text, re.DOTALL)

if rates_block:
    new_rates = """<!-- Step 10: Rates -->
        {% set ns_rates = namespace(has=false) %}
        {% for a in accounts if a.rates_amount and a.rates_amount > 0 %}
            {% set ns_rates.has = true %}
        {% endfor %}
        {% if ns_rates.has %}
        <div class="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden print:border-b print:shadow-none">
            <div class="bg-slate-50 px-6 py-3 border-b border-slate-200">
                <h2 class="text-lg font-bold text-slate-800">10. Rates</h2>
            </div>
            <div class="p-0 overflow-x-auto">
                <table class="min-w-full divide-y divide-slate-200 text-sm">
                    <tbody class="bg-white">
                        {% for acc in accounts if acc.rates_amount and acc.rates_amount > 0 %}
                        <tr class="bg-slate-100 border-t-4 border-slate-300">
                            <td colspan="4" class="px-6 py-2 font-bold text-slate-800">Account: {{ acc.account_number }}</td>
                        </tr>
                        <tr>
                            <td class="px-6 py-3 border-r border-slate-100"><span class="block text-[10px] font-bold text-slate-400 uppercase mb-1">Reference</span>{{ acc.rates_reference or '-' }}</td>
                            <td class="px-6 py-3 border-r border-slate-100"><span class="block text-[10px] font-bold text-slate-400 uppercase mb-1">Erf Details</span>{{ acc.rates_erf_details or '-' }}</td>
                            <td class="px-6 py-3 border-r border-slate-100"><span class="block text-[10px] font-bold text-slate-400 uppercase mb-1">Category</span>{{ acc.rates_property_category or '-' }}</td>
                            <td class="px-6 py-3"><span class="block text-[10px] font-bold text-slate-400 uppercase mb-1">Market Val</span><span class="font-medium text-slate-700">R {{ "{:,.0f}".format(acc.rates_market_value or 0).replace(',', ' ') }}</span></td>
                        </tr>
                        <tr class="bg-slate-50">
                            <td class="px-6 py-3 border-r border-slate-100"><span class="block text-[10px] font-bold text-slate-400 uppercase mb-1">Rateable Val</span><span class="font-medium text-slate-700">R {{ "{:,.0f}".format(acc.rates_rateable_value or 0).replace(',', ' ') }}</span></td>
                            <td class="px-6 py-3 border-r border-slate-100"><span class="block text-[10px] font-bold text-slate-400 uppercase mb-1">Gen. Randage</span>{{ "%.4f"|format(acc.rates_general_randage or 0) }}</td>
                            <td class="px-6 py-3 border-r border-slate-100"><span class="block text-[10px] font-bold text-slate-400 uppercase mb-1">SRA Randage</span>{{ "%.4f"|format(acc.rates_sra_randage or 0) }}</td>
                            <td class="px-6 py-3"><span class="block text-[10px] font-bold text-slate-400 uppercase mb-1">Deferred</span><span class="font-medium text-rose-600">R {{ "%.2f"|format(acc.rates_deferred or 0) }}</span></td>
                        </tr>
                        <tr>
                            <td class="px-6 py-3 border-r border-slate-100"><span class="block text-[10px] font-bold text-slate-400 uppercase mb-1">Gen. Monthly</span><span class="font-medium text-slate-700">R {{ "%.2f"|format(acc.rates_general_monthly or 0) }}</span></td>
                            <td class="px-6 py-3 border-r border-slate-100"><span class="block text-[10px] font-bold text-slate-400 uppercase mb-1">SRA Monthly</span><span class="font-medium text-slate-700">R {{ "%.2f"|format(acc.rates_sra_monthly or 0) }}</span></td>
                            <td colspan="2" class="px-6 py-3 bg-blue-50 text-blue-900 border-t border-blue-100">
                                <span class="block text-[10px] font-bold text-blue-500 uppercase mb-1">Total Amount</span>
                                <span class="text-lg font-bold">R {{ "%.2f"|format(acc.rates_amount) }}</span>
                            </td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
        </div>
        {% endif %}

        <!-- Step 11: Arrangements -->"""
    
    text = text.replace(rates_block.group(0), new_rates)
    
    with open('templates/program_billing/architecture_summary.html', 'w', encoding='utf-8') as f:
        f.write(text)
    print("Fixed Rates table compactness!")
else:
    print("Regex failed to match old block.")
