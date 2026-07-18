with open('templates/admin/modules_control.html', 'r', encoding='utf-8') as f:
    content = f.read()

start_idx = content.find('<table')
end_idx = content.find('</table>') + len('</table>')

new_table = """
<table class="min-w-full divide-y divide-slate-200">
  <thead class="bg-slate-50">
    <tr>
      <th class="px-4 py-3 text-left text-xs font-semibold text-slate-500 uppercase tracking-wider">No.</th>
      <th class="px-4 py-3 text-left text-xs font-semibold text-slate-500 uppercase tracking-wider">Module (Slug)</th>
      <th class="px-4 py-3 text-center text-xs font-semibold text-slate-500 uppercase tracking-wider">Visible on Welcome?</th>
      <th class="px-4 py-3 text-center text-xs font-semibold text-slate-500 uppercase tracking-wider">Sandbox Mode?</th>
    </tr>
  </thead>
  <tbody class="divide-y divide-slate-100 bg-white">
    {% set slugs = ['loss', 'reading', 'home', 'tutor', 'budget', 'billing', 'cultural_fire', 'practice_crm', 'hds', 'receptionist', 'spv', 'adv_math', 'mechanic'] %}
    {% for s in slugs %}
    <tr class="hover:bg-slate-50 transition-colors">
      <td class="px-4 py-3 text-sm text-slate-500 font-medium">{{ loop.index }}</td>
      <td class="px-4 py-3 text-sm font-bold text-slate-800 capitalize">{{ s | replace('_', ' ') }}</td>
      
      <!-- Visibility Checkbox -->
      <td class="px-4 py-3 text-center">
        <label class="inline-flex items-center cursor-pointer justify-center">
          <input type="hidden" name="visibility_{{ s }}" id="vis_{{ s }}" value="{{ settings.get('visibility_'~s, 'visible') }}">
          <input type="checkbox" class="form-checkbox h-5 w-5 text-emerald-500 rounded border-2 border-slate-300 focus:ring-emerald-500 outline-none transition duration-150 cursor-pointer"
                 onchange="document.getElementById('vis_{{ s }}').value = this.checked ? 'visible' : 'hidden';"
                 {% if settings.get('visibility_'~s, 'visible') == 'visible' %}checked{% endif %}>
        </label>
      </td>

      <!-- Sandbox Checkbox -->
      <td class="px-4 py-3 text-center">
        <label class="inline-flex items-center cursor-pointer justify-center">
          <input type="hidden" name="yoco_mode_{{ s }}" id="yoco_{{ s }}" value="{{ settings.get('yoco_mode_'~s, 'sandbox') }}">
          <input type="checkbox" class="form-checkbox h-5 w-5 text-amber-500 rounded border-2 border-slate-300 focus:ring-amber-500 outline-none transition duration-150 cursor-pointer"
                 onchange="document.getElementById('yoco_{{ s }}').value = this.checked ? 'sandbox' : 'live';"
                 {% if settings.get('yoco_mode_'~s, 'sandbox') == 'sandbox' %}checked{% endif %}>
        </label>
      </td>
    </tr>
    {% endfor %}
  </tbody>
</table>
"""

if start_idx != -1 and end_idx != -1:
    content = content[:start_idx] + new_table + content[end_idx:]
    with open('templates/admin/modules_control.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Success")
else:
    print("Table not found")
