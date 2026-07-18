with open('templates/program_billing/architecture_summary.html', 'r', encoding='utf-8') as f:
    text = f.read()

import re
old_rates = re.search(r'<\!-- Step 10: Rates -->(.*?)\<\!-- Step 11: Arrangements --\>', text, re.DOTALL).group(1)
old_arrangements = re.search(r'<\!-- Step 11: Arrangements -->(.*?)\<\!-- Step 12: Owners --\>', text, re.DOTALL).group(1)

new_rates = """
        {% set has_rates = false %}
        {% for a in accounts if a.rates_amount and a.rates_amount > 0 %}
            {% set has_rates = true %}
        {% endfor %}
        {% if has_rates %}
        <div class="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden print:border-b print:shadow-none">
            <div class="bg-slate-50 px-6 py-3 border-b border-slate-200">
                <h2 class="text-lg font-bold text-slate-800">10. Rates</h2>
            </div>
            <div class="p-0 overflow-x-auto">
                <table class="min-w-full divide-y divide-slate-200 text-sm">
                    <thead class="bg-slate-50">
                        <tr>
                            <th class="px-4 py-3 text-left font-bold text-slate-500 uppercase whitespace-nowrap">Account</th>
                            <th class="px-4 py-3 text-left font-bold text-slate-500 uppercase whitespace-nowrap">Reference</th>
                            <th class="px-4 py-3 text-left font-bold text-slate-500 uppercase whitespace-nowrap">Erf Details</th>
                            <th class="px-4 py-3 text-left font-bold text-slate-500 uppercase whitespace-nowrap">Category</th>
                            <th class="px-4 py-3 text-left font-bold text-slate-500 uppercase whitespace-nowrap">Market Val</th>
                            <th class="px-4 py-3 text-left font-bold text-slate-500 uppercase whitespace-nowrap">Rateable Val</th>
                            <th class="px-4 py-3 text-left font-bold text-slate-500 uppercase whitespace-nowrap">Gen. Randage</th>
                            <th class="px-4 py-3 text-left font-bold text-slate-500 uppercase whitespace-nowrap">SRA Randage</th>
                            <th class="px-4 py-3 text-left font-bold text-slate-500 uppercase whitespace-nowrap">Deferred</th>
                            <th class="px-4 py-3 text-left font-bold text-slate-500 uppercase whitespace-nowrap">Gen. Monthly</th>
                            <th class="px-4 py-3 text-left font-bold text-slate-500 uppercase whitespace-nowrap">SRA Monthly</th>
                            <th class="px-4 py-3 text-left font-bold text-slate-800 uppercase whitespace-nowrap">Total Amount</th>
                        </tr>
                    </thead>
                    <tbody class="divide-y divide-slate-200 bg-white">
                        {% for acc in accounts if acc.rates_amount and acc.rates_amount > 0 %}
                        <tr>
                            <td class="px-4 py-3 font-medium whitespace-nowrap">{{ acc.account_number }}</td>
                            <td class="px-4 py-3 whitespace-nowrap">{{ acc.rates_reference or '-' }}</td>
                            <td class="px-4 py-3 whitespace-nowrap">{{ acc.rates_erf_details or '-' }}</td>
                            <td class="px-4 py-3 whitespace-nowrap">{{ acc.rates_property_category or '-' }}</td>
                            <td class="px-4 py-3 whitespace-nowrap">R {{ "{:,.0f}".format(acc.rates_market_value or 0).replace(',', ' ') }}</td>
                            <td class="px-4 py-3 whitespace-nowrap">R {{ "{:,.0f}".format(acc.rates_rateable_value or 0).replace(',', ' ') }}</td>
                            <td class="px-4 py-3 whitespace-nowrap">{{ "%.4f"|format(acc.rates_general_randage or 0) }}</td>
                            <td class="px-4 py-3 whitespace-nowrap">{{ "%.4f"|format(acc.rates_sra_randage or 0) }}</td>
                            <td class="px-4 py-3 whitespace-nowrap">R {{ "%.2f"|format(acc.rates_deferred or 0) }}</td>
                            <td class="px-4 py-3 whitespace-nowrap">R {{ "%.2f"|format(acc.rates_general_monthly or 0) }}</td>
                            <td class="px-4 py-3 whitespace-nowrap">R {{ "%.2f"|format(acc.rates_sra_monthly or 0) }}</td>
                            <td class="px-4 py-3 text-slate-800 font-bold whitespace-nowrap">R {{ "%.2f"|format(acc.rates_amount) }}</td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
        </div>
        {% endif %}
"""

new_arrangements = """
        {% set has_arrangements = false %}
        {% for a in accounts if a.ca_agreement_amount and a.ca_agreement_amount > 0 %}
            {% set has_arrangements = true %}
        {% endfor %}
        {% if has_arrangements %}
        <div class="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden print:border-b print:shadow-none">
            <div class="bg-emerald-50 px-6 py-3 border-b border-emerald-100">
                <h2 class="text-lg font-bold text-emerald-800">11. Credit Arrangements</h2>
            </div>
            <div class="p-0 overflow-x-auto">
                <table class="min-w-full divide-y divide-slate-200 text-sm">
                    <thead class="bg-slate-50">
                        <tr>
                            <th class="px-4 py-3 text-left font-bold text-slate-500 uppercase whitespace-nowrap">Account</th>
                            <th class="px-4 py-3 text-left font-bold text-slate-500 uppercase whitespace-nowrap">Contract No.</th>
                            <th class="px-4 py-3 text-left font-bold text-slate-500 uppercase whitespace-nowrap">Agreement Amt</th>
                            <th class="px-4 py-3 text-left font-bold text-slate-500 uppercase whitespace-nowrap">Installments Raised</th>
                            <th class="px-4 py-3 text-left font-bold text-slate-500 uppercase whitespace-nowrap">Installment Amt</th>
                            <th class="px-4 py-3 text-left font-bold text-slate-500 uppercase whitespace-nowrap">Amount Owing</th>
                            <th class="px-4 py-3 text-left font-bold text-slate-500 uppercase whitespace-nowrap">Periods Left</th>
                        </tr>
                    </thead>
                    <tbody class="divide-y divide-slate-200 bg-white">
                        {% for acc in accounts if acc.ca_agreement_amount and acc.ca_agreement_amount > 0 %}
                        <tr>
                            <td class="px-4 py-3 font-medium whitespace-nowrap">{{ acc.account_number }}</td>
                            <td class="px-4 py-3 whitespace-nowrap">{{ acc.ca_contract_number or '-' }}</td>
                            <td class="px-4 py-3 text-emerald-600 font-bold whitespace-nowrap">R {{ "%.2f"|format(acc.ca_agreement_amount) }}</td>
                            <td class="px-4 py-3 whitespace-nowrap">R {{ "%.2f"|format(acc.ca_installments_raised or 0) }}</td>
                            <td class="px-4 py-3 whitespace-nowrap">R {{ "%.2f"|format(acc.ca_installment_amount or 0) }}</td>
                            <td class="px-4 py-3 whitespace-nowrap">R {{ "%.2f"|format(acc.ca_amount_owing or 0) }}</td>
                            <td class="px-4 py-3 whitespace-nowrap">{{ acc.ca_remaining_periods }}</td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
        </div>
        {% endif %}
"""

text = text.replace(old_rates, new_rates)
text = text.replace(old_arrangements, new_arrangements)

with open('templates/program_billing/architecture_summary.html', 'w', encoding='utf-8') as f:
    f.write(text)
print("Updated summary tables!")
