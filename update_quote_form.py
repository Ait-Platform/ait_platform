import re

with open('templates/program_mechanic/quote_form.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Labor
labor_regex = r'<tbody id="labor-body" class="divide-y divide-indigo-100">.*?</tbody>'
labor_tbody = '''<tbody id="labor-body" class="divide-y divide-indigo-100">
                  {% if edit_card and edit_card.labor_lines %}
                    {% for l in edit_card.labor_lines %}
                    <tr class="labor-row">
                      <td class="px-2 py-3"><input type="text" name="labor_desc[]" list="labor-tasks" class="w-full text-sm border-indigo-300 rounded focus:ring-indigo-500 focus:border-indigo-500 shadow-sm font-medium" value="{{ l.description }}" required></td>
                      <td class="px-2 py-3"><input type="time" name="labor_in[]" class="w-full text-sm border-indigo-300 rounded focus:ring-indigo-500 focus:border-indigo-500 shadow-sm time-input" value="{{ l.time_in }}" onchange="calcRow(this)"></td>
                      <td class="px-2 py-3"><input type="time" name="labor_out[]" class="w-full text-sm border-indigo-300 rounded focus:ring-indigo-500 focus:border-indigo-500 shadow-sm time-input" value="{{ l.time_out }}" onchange="calcRow(this)"></td>
                      <td class="px-2 py-3"><input type="number" step="0.01" name="labor_rate[]" class="w-24 text-sm border-indigo-300 rounded focus:ring-indigo-500 focus:border-indigo-500 shadow-sm rate-input" value="{{ l.rate_per_hour }}" oninput="calcRow(this)"></td>
                      <td class="px-2 py-3 text-sm font-bold text-indigo-900 hours-display">{{ l.hours }}</td>
                      <td class="px-2 py-3 text-sm font-black text-indigo-900 text-right total-display">{{ "%.2f"|format(l.hours * l.rate_per_hour) }}</td>
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
content = re.sub(labor_regex, labor_tbody, content, flags=re.DOTALL)

# 2. Parts
parts_regex = r'<tbody id="part-body" class="divide-y divide-emerald-100">.*?</tbody>'
parts_tbody = '''<tbody id="part-body" class="divide-y divide-emerald-100">
                  {% if edit_card and edit_card.part_lines %}
                    {% for p in edit_card.part_lines %}
                    <tr class="part-row">
                      <td class="px-2 py-3"><input type="number" name="part_qty[]" class="w-full text-sm border-emerald-300 rounded focus:ring-emerald-500 focus:border-emerald-500 shadow-sm qty-input" value="{{ p.quantity }}" min="1" oninput="calcPartRow(this)"></td>
                      <td class="px-2 py-3">
                        <input type="text" name="part_desc[]" class="w-full text-sm border-emerald-300 rounded focus:ring-emerald-500 focus:border-emerald-500 shadow-sm font-medium" value="{{ p.description }}" list="catalog-parts-list" oninput="handlePartSelect(this)" required>
                      </td>
                      <td class="px-2 py-3"><input type="number" step="0.01" name="part_rate[]" class="w-32 text-sm border-emerald-300 rounded focus:ring-emerald-500 focus:border-emerald-500 shadow-sm rate-input" value="{{ p.markup_price }}" oninput="calcPartRow(this)"></td>
                      <td class="px-2 py-3 text-sm font-black text-emerald-900 text-right total-display">{{ "%.2f"|format(p.quantity * p.markup_price) }}</td>
                      <td class="px-2 py-3 text-center"><button type="button" onclick="this.closest('tr').remove(); calcGrand()" class="text-rose-500 hover:text-rose-700 bg-white rounded-full p-1 shadow-sm">&times;</button></td>
                    </tr>
                    {% endfor %}
                  {% else %}
                  <tr class="part-row">
                    <td class="px-2 py-3"><input type="number" name="part_qty[]" class="w-full text-sm border-emerald-300 rounded focus:ring-emerald-500 focus:border-emerald-500 shadow-sm qty-input" value="1" min="1" oninput="calcPartRow(this)"></td>
                    <td class="px-2 py-3">
                      <input type="text" name="part_desc[]" class="w-full text-sm border-emerald-300 rounded focus:ring-emerald-500 focus:border-emerald-500 shadow-sm font-medium" placeholder="e.g. Engine Oil" list="catalog-parts-list" oninput="handlePartSelect(this)" required>
                    </td>
                    <td class="px-2 py-3"><input type="number" step="0.01" name="part_rate[]" class="w-32 text-sm border-emerald-300 rounded focus:ring-emerald-500 focus:border-emerald-500 shadow-sm rate-input" value="0.00" oninput="calcPartRow(this)"></td>
                    <td class="px-2 py-3 text-sm font-black text-emerald-900 text-right total-display">0.00</td>
                    <td class="px-2 py-3 text-center"><button type="button" onclick="this.closest('tr').remove(); calcGrand()" class="text-rose-500 hover:text-rose-700 bg-white rounded-full p-1 shadow-sm">&times;</button></td>
                  </tr>
                  {% endif %}
                </tbody>'''
content = re.sub(parts_regex, parts_tbody, content, flags=re.DOTALL)

with open('templates/program_mechanic/quote_form.html', 'w', encoding='utf-8') as f:
    f.write(content)
