import re

with open('templates/program_billing/manager_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Replace the entire 6-tile grid with a focused 2-tile flow.
old_grid_start = '<div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-6">'
old_grid_end = '<!-- Setup Modal -->'

# We use regex to replace everything between grid_start and Setup Modal
pattern = re.compile(re.escape(old_grid_start) + r'.*?' + r'(?=<!-- Setup Modal -->)', re.DOTALL)

new_grid = """<div class="grid grid-cols-1 md:grid-cols-2 gap-6">
          
          <!-- Tile 1: Set Property Map -->
          {% if draft_property %}
            <div class="block rounded-xl border p-6 shadow-sm bg-white border-blue-200 relative group transition hover:shadow-md">
              <div class="font-semibold text-blue-800 text-lg">1. Property Map: {{ draft_property.name }}</div>
              <div class="mt-1 text-sm text-slate-600">You are currently setting up the architecture for this property.</div>
              
              <div class="mt-6 flex space-x-3">
                <button onclick="openEditDraftModal()" class="text-sm bg-slate-100 hover:bg-slate-200 text-slate-700 font-bold py-2 px-4 rounded border border-slate-300 transition shadow-sm">
                  Edit Map Details
                </button>
                <button onclick="document.getElementById('setupModal').classList.remove('hidden')" class="text-sm bg-emerald-50 hover:bg-emerald-100 text-emerald-600 font-bold py-2 px-4 rounded border border-emerald-200 transition shadow-sm">
                  Start New Property
                </button>
              </div>
            </div>
            
            <!-- Edit Draft Modal -->
            <div id="editDraftModal" class="hidden fixed inset-0 bg-slate-900 bg-opacity-50 flex items-center justify-center z-50">
              <div class="bg-white rounded-xl shadow-lg max-w-md w-full p-6">
                <div class="flex justify-between items-center mb-4">
                  <h3 class="text-xl font-bold text-slate-800">Edit Property Map</h3>
                  <button onclick="closeEditDraftModal()" class="text-slate-400 hover:text-slate-600">&times;</button>
                </div>
                <form method="POST" action="{{ url_for('billing_bp.edit_draft', property_id=draft_property.id) }}">
                  <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
                  <div class="space-y-4">
                    <div>
                      <label class="block text-sm font-semibold text-slate-700 mb-1">Property Name</label>
                      <input type="text" name="property_name" value="{{ draft_property.name }}" required class="w-full rounded border-2 border-slate-300 px-3 py-2 outline-none focus:border-blue-500">
                    </div>
                    <div class="grid grid-cols-2 gap-4">
                      <div>
                        <label class="block text-sm font-semibold text-slate-700 mb-1">Total Bill Accounts</label>
                        <input type="number" name="expected_bills" value="{{ draft_property.expected_bills }}" min="1" required class="w-full rounded border-2 border-slate-300 px-3 py-2 outline-none focus:border-blue-500">
                      </div>
                      <div>
                        <label class="block text-sm font-semibold text-slate-700 mb-1">Total Sub-Accounts</label>
                        <input type="number" name="expected_tenants" value="{{ draft_property.expected_tenants }}" min="1" required class="w-full rounded border-2 border-slate-300 px-3 py-2 outline-none focus:border-blue-500">
                      </div>
                    </div>
                    <div class="grid grid-cols-2 gap-4">
                      <div>
                        <label class="block text-sm font-semibold text-slate-700 mb-1">Total Water Meters</label>
                        <input type="number" name="expected_water_meters" value="{{ draft_property.expected_water_meters|default(0) }}" min="0" required class="w-full rounded border-2 border-slate-300 px-3 py-2 outline-none focus:border-blue-500">
                      </div>
                      <div>
                        <label class="block text-sm font-semibold text-slate-700 mb-1">Total Elec Meters</label>
                        <input type="number" name="expected_elec_meters" value="{{ draft_property.expected_elec_meters|default(0) }}" min="0" required class="w-full rounded border-2 border-slate-300 px-3 py-2 outline-none focus:border-blue-500">
                      </div>
                    </div>
                    <div class="grid grid-cols-2 gap-4 mt-4">
                      <div>
                        <label class="block text-sm font-semibold text-slate-700 mb-1">Bulk Water?</label>
                        <select name="is_bulk_water" class="w-full rounded border-2 border-slate-300 px-3 py-2 outline-none focus:border-blue-500 bg-white">
                          <option value="1" {% if draft_property.is_bulk_water %}selected{% endif %}>Yes</option>
                          <option value="0" {% if not draft_property.is_bulk_water %}selected{% endif %}>No</option>
                        </select>
                      </div>
                      <div>
                        <label class="block text-sm font-semibold text-slate-700 mb-1">Bulk Electrical?</label>
                        <select name="is_bulk_elec" class="w-full rounded border-2 border-slate-300 px-3 py-2 outline-none focus:border-blue-500 bg-white">
                          <option value="1" {% if draft_property.is_bulk_elec %}selected{% endif %}>Yes</option>
                          <option value="0" {% if not draft_property.is_bulk_elec %}selected{% endif %}>No</option>
                        </select>
                      </div>
                    </div>
                    <div class="pt-4 flex justify-end">
                      <button type="button" onclick="closeEditDraftModal()" class="mr-2 px-4 py-2 text-slate-600 font-semibold hover:bg-slate-100 rounded">Cancel</button>
                      <button type="submit" class="bg-blue-600 hover:bg-blue-700 text-white font-bold py-2 px-6 rounded shadow">Save Changes</button>
                    </div>
                  </div>
                </form>
              </div>
            </div>
            <script>
              function openEditDraftModal() { document.getElementById('editDraftModal').classList.remove('hidden'); }
              function closeEditDraftModal() { document.getElementById('editDraftModal').classList.add('hidden'); }
            </script>
          {% else %}
            <button onclick="document.getElementById('setupModal').classList.remove('hidden')" class="text-left block rounded-xl border p-8 shadow-sm transition hover:shadow-md bg-white border-slate-200 hover:border-blue-300 group">
              <div class="font-bold text-slate-800 text-xl group-hover:text-blue-700 flex items-center">
                <span class="w-8 h-8 rounded-full bg-blue-100 text-blue-700 flex items-center justify-center mr-3 text-sm">1</span>
                Set Property Map
              </div>
              <div class="mt-3 text-sm text-slate-600 leading-relaxed">Initialize a new property by defining the total meters, expected accounts, and bulk configurations.</div>
            </button>
          {% endif %}
          
          <!-- Tile 2: Launch Architecture Wizard -->
          {% if draft_property %}
            <a href="{{ url_for('billing_bp.manual_capture') }}?property_id={{ draft_property.id }}" class="block rounded-xl border p-8 shadow-sm transition hover:shadow-md bg-emerald-50 border-emerald-200 hover:bg-emerald-100 group relative overflow-hidden">
              <div class="absolute -right-4 -top-4 w-24 h-24 bg-emerald-200 rounded-full opacity-50"></div>
              <div class="font-bold text-emerald-900 text-xl group-hover:text-emerald-700 flex items-center relative z-10">
                <span class="w-8 h-8 rounded-full bg-emerald-200 text-emerald-800 flex items-center justify-center mr-3 text-sm">2</span>
                Launch Architecture Wizard
              </div>
              <div class="mt-3 text-sm text-emerald-800 leading-relaxed relative z-10">Step into the 11-step master wizard to configure accounts, meters, readings, and arrears.</div>
              <div class="mt-4 inline-block font-bold text-emerald-700 text-sm group-hover:underline relative z-10">Open Wizard &rarr;</div>
            </a>
          {% else %}
            <div class="block rounded-xl border p-8 shadow-sm bg-slate-50 border-slate-200 opacity-60 cursor-not-allowed">
              <div class="font-bold text-slate-800 text-xl flex items-center">
                <span class="w-8 h-8 rounded-full bg-slate-200 text-slate-500 flex items-center justify-center mr-3 text-sm">2</span>
                Launch Architecture Wizard
              </div>
              <div class="mt-3 text-sm text-slate-600 leading-relaxed">Locked until a property map is set.</div>
            </div>
          {% endif %}
          
        </div>
      </div>
      
      """

content = pattern.sub(new_grid, content)

# 2. Add "View Map" and "Edit Map" buttons to the property table
old_actions = """                      <a href="{{ url_for('billing_bp.property_hub', property_id=row.property_id) }}" class="text-sm bg-blue-100 hover:bg-blue-200 text-blue-700 font-medium py-1.5 px-3 rounded-lg transition" title="Readings & Bills">Hub</a>
                      <a href="{{ url_for('billing_bp.view_property', property_id=row.property_id) }}" class="text-sm bg-slate-100 hover:bg-slate-200 text-slate-700 font-medium py-1.5 px-3 rounded-lg transition" title="View / Edit Property Details">View / Edit</a>"""

new_actions = """                      <a href="{{ url_for('billing_bp.architecture_summary', property_id=row.property_id) }}" class="text-sm bg-emerald-100 hover:bg-emerald-200 text-emerald-800 font-bold py-1.5 px-3 rounded-lg transition border border-emerald-200 shadow-sm" title="View Architecture Map">View Map</a>
                      <!-- To edit map, we inject them back into manual_capture route for that property -->
                      <!-- To do this, we should change the property status back to draft_manual if it isn't -->
                      <!-- For now, just link to manual_capture -->
                      <a href="{{ url_for('billing_bp.manual_capture') }}?property_id={{ row.property_id }}" class="text-sm bg-slate-100 hover:bg-slate-200 text-slate-700 font-bold py-1.5 px-3 rounded-lg transition border border-slate-300 shadow-sm" title="Edit Architecture Map">Edit Map</a>
                      
                      <a href="{{ url_for('billing_bp.property_hub', property_id=row.property_id) }}" class="text-sm bg-blue-100 hover:bg-blue-200 text-blue-700 font-medium py-1.5 px-3 rounded-lg transition" title="Readings & Bills">Hub</a>"""

content = content.replace(old_actions, new_actions)

with open('templates/program_billing/manager_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("manager_dashboard.html updated with redesigned tiles.")
