import re

with open('templates/program_mechanic/quote_form.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Update basic inputs
content = content.replace('name="customer_name" required autofocus class="block', 'name="customer_name" required autofocus class="block" value="{{ edit_card.vehicle.client.name if edit_card and edit_card.vehicle and edit_card.vehicle.client else \'\' }}"')
content = content.replace('name="vehicle_reg" required class="block', 'name="vehicle_reg" required class="block" value="{{ edit_card.vehicle.license_plate if edit_card and edit_card.vehicle else \'\' }}"')
content = content.replace('name="vin_number" class="block', 'name="vin_number" class="block" value="{{ edit_card.vehicle.vin if edit_card and edit_card.vehicle else \'\' }}"')
content = content.replace('name="make" class="block', 'name="make" class="block" value="{{ edit_card.vehicle.make if edit_card and edit_card.vehicle else \'\' }}"')
content = content.replace('name="model" class="block', 'name="model" class="block" value="{{ edit_card.vehicle.model if edit_card and edit_card.vehicle else \'\' }}"')
content = content.replace('name="year" class="block', 'name="year" class="block" value="{{ edit_card.vehicle.year if edit_card and edit_card.vehicle else \'\' }}"')
content = content.replace('name="mileage" class="block', 'name="mileage" class="block" value="{{ edit_card.vehicle.mileage if edit_card and edit_card.vehicle else \'\' }}"')

old_labor = '''                <tbody id="labor-body" class="divide-y divide-indigo-100">
                  <tr class="labor-row">
                    <td class="px-2 py-3"><input type="text" name="labor_desc[]" list="labor-tasks" class="w-full text-sm border-indigo-300 rounded focus:ring-indigo-500 focus:border-indigo-500 shadow-sm font-medium" placeholder="e.g. Diagnostics" required></td>
                    <td class="px-2 py-3"><input type="time" name="labor_in[]" class="w-full text-sm border-indigo-300 rounded focus:ring-indigo-500 focus:border-indigo-500 shadow-sm time-input" onchange="calcRow(this)"></td>
                    <td class="px-2 py-3"><input type="time" name="labor_out[]" class="w-full text-sm border-indigo-300 rounded focus:ring-indigo-500 focus:border-indigo-500 shadow-sm time-input" onchange="calcRow(this)"></td>
                    <td class="px-2 py-3"><input type="number" step="0.01" name="labor_rate[]" class="w-24 text-sm border-indigo-300 rounded focus:ring-indigo-500 focus:border-indigo-500 shadow-sm rate-input" value="350.00" oninput="calcRow(this)"></td>
                    <td class="px-2 py-3 text-sm font-bold text-indigo-900 hours-display">0.00</td>
                    <td class="px-2 py-3 text-sm font-black text-indigo-900 text-right total-display">0.00</td>
                    <td class="px-2 py-3 text-center"><button type="button" onclick="this.closest('tr').remove(); calcGrand()" class="text-rose-500 hover:text-rose-700 bg-white rounded-full p-1 shadow-sm">&times;</button></td>
                  </tr>
                </tbody>'''

new_labor = '''                <tbody id="labor-body" class="divide-y divide-indigo-100">
                  {% if edit_card and edit_card.labor_lines %}
                    {% for labor in edit_card.labor_lines %}
                    <tr class="labor-row">
                      <td class="px-2 py-3"><input type="text" name="labor_desc[]" list="labor-tasks" class="w-full text-sm border-indigo-300 rounded focus:ring-indigo-500 focus:border-indigo-500 shadow-sm font-medium" placeholder="e.g. Diagnostics" required value="{{ labor.description }}"></td>
                      <td class="px-2 py-3"><input type="time" name="labor_in[]" class="w-full text-sm border-indigo-300 rounded focus:ring-indigo-500 focus:border-indigo-500 shadow-sm time-input" onchange="calcRow(this)" value="{{ labor.time_in }}"></td>
                      <td class="px-2 py-3"><input type="time" name="labor_out[]" class="w-full text-sm border-indigo-300 rounded focus:ring-indigo-500 focus:border-indigo-500 shadow-sm time-input" onchange="calcRow(this)" value="{{ labor.time_out }}"></td>
                      <td class="px-2 py-3"><input type="number" step="0.01" name="labor_rate[]" class="w-24 text-sm border-indigo-300 rounded focus:ring-indigo-500 focus:border-indigo-500 shadow-sm rate-input" value="{{ "%.2f"|format(labor.rate_per_hour) }}" oninput="calcRow(this)"></td>
                      <td class="px-2 py-3 text-sm font-bold text-indigo-900 hours-display">{{ "%.2f"|format(labor.hours) }}</td>
                      <td class="px-2 py-3 text-sm font-black text-indigo-900 text-right total-display">{{ "%.2f"|format(labor.hours * labor.rate_per_hour) }}</td>
                      <td class="px-2 py-3 text-center"><button type="button" onclick="this.closest('tr').remove(); calcGrand()" class="text-rose-500 hover:text-rose-700 bg-white rounded-full p-1 shadow-sm">&times;</button></td>
                    </tr>
                    {% endfor %}
                  {% else %}
                  <tr class="labor-row">
                    <td class="px-2 py-3"><input type="text" name="labor_desc[]" list="labor-tasks" class="w-full text-sm border-indigo-300 rounded focus:ring-indigo-500 focus:border-indigo-500 shadow-sm font-medium" placeholder="e.g. Diagnostics" required></td>
                    <td class="px-2 py-3"><input type="time" name="labor_in[]" class="w-full text-sm border-indigo-300 rounded focus:ring-indigo-500 focus:border-indigo-500 shadow-sm time-input" onchange="calcRow(this)"></td>
                    <td class="px-2 py-3"><input type="time" name="labor_out[]" class="w-full text-sm border-indigo-300 rounded focus:ring-indigo-500 focus:border-indigo-500 shadow-sm time-input" onchange="calcRow(this)"></td>
                    <td class="px-2 py-3"><input type="number" step="0.01" name="labor_rate[]" class="w-24 text-sm border-indigo-300 rounded focus:ring-indigo-500 focus:border-indigo-500 shadow-sm rate-input" value="350.00" oninput="calcRow(this)"></td>
                    <td class="px-2 py-3 text-sm font-bold text-indigo-900 hours-display">0.00</td>
                    <td class="px-2 py-3 text-sm font-black text-indigo-900 text-right total-display">0.00</td>
                    <td class="px-2 py-3 text-center"><button type="button" onclick="this.closest('tr').remove(); calcGrand()" class="text-rose-500 hover:text-rose-700 bg-white rounded-full p-1 shadow-sm">&times;</button></td>
                  </tr>
                  {% endif %}
                </tbody>'''

content = content.replace(old_labor, new_labor)

with open('templates/program_mechanic/quote_form.html', 'w', encoding='utf-8') as f:
    f.write(content)
