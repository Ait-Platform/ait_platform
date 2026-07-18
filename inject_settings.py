with open('templates/admin/settings.html', 'r', encoding='utf-8') as f:
    content = f.read()

injection_point = '      <!-- Adv Math -->'
parts = content.split(injection_point)

if len(parts) == 2:
    new_section = """
      <!-- Module Visibility & Environment -->
      <form method="POST" action="{{ url_for('admin_bp.settings_save') }}" class="mb-8">
        <div class="bg-white border border-slate-200 rounded-xl shadow-sm overflow-hidden">
          <div class="bg-slate-50 border-b border-slate-200 px-6 py-4">
            <h3 class="text-lg font-semibold text-slate-800">Platform Modules Control</h3>
            <p class="text-sm text-slate-500 mt-1">Manage visibility on the Welcome page and Yoco Payment mode for each program.</p>
          </div>
          <div class="p-6">
            <div class="overflow-x-auto">
              <table class="min-w-full divide-y divide-slate-200">
                <thead>
                  <tr>
                    <th class="px-3 py-2 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">Module (Slug)</th>
                    <th class="px-3 py-2 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">Welcome Page Visibility</th>
                    <th class="px-3 py-2 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">Yoco Mode</th>
                  </tr>
                </thead>
                <tbody class="divide-y divide-slate-100">
                  {% set slugs = ['loss', 'reading', 'home', 'tutor', 'budget', 'billing', 'cultural_fire', 'practice_crm', 'hds', 'receptionist', 'spv', 'adv_math', 'mechanic'] %}
                  {% for s in slugs %}
                  <tr>
                    <td class="px-3 py-3 text-sm font-medium text-slate-700 capitalize">{{ s | replace('_', ' ') }}</td>
                    <td class="px-3 py-3">
                      <select name="visibility_{{ s }}" class="block w-full rounded-md border border-slate-300 py-1.5 px-3 text-sm shadow-sm focus:border-indigo-500 focus:ring-indigo-500 bg-white">
                        <option value="visible" {% if settings.get('visibility_'~s, 'visible') == 'visible' %}selected{% endif %}>Visible</option>
                        <option value="hidden" {% if settings.get('visibility_'~s, 'visible') == 'hidden' %}selected{% endif %}>Hidden</option>
                      </select>
                    </td>
                    <td class="px-3 py-3">
                      <select name="yoco_mode_{{ s }}" class="block w-full rounded-md border border-slate-300 py-1.5 px-3 text-sm shadow-sm focus:border-indigo-500 focus:ring-indigo-500 bg-white">
                        <option value="sandbox" {% if settings.get('yoco_mode_'~s, 'sandbox') == 'sandbox' %}selected{% endif %}>Sandbox</option>
                        <option value="live" {% if settings.get('yoco_mode_'~s, 'sandbox') == 'live' %}selected{% endif %}>Live</option>
                      </select>
                    </td>
                  </tr>
                  {% endfor %}
                </tbody>
              </table>
            </div>
            
            <div class="mt-6 flex justify-end">
              <button type="submit" class="bg-indigo-600 hover:bg-indigo-700 text-white font-medium py-2 px-4 rounded-lg text-sm shadow-sm transition">
                Save Controls
              </button>
            </div>
          </div>
        </div>
      </form>
"""
    new_content = parts[0] + new_section + injection_point + parts[1]
    with open('templates/admin/settings.html', 'w', encoding='utf-8') as f:
        f.write(new_content)
    print("Success")
else:
    print("Could not find injection point.")
