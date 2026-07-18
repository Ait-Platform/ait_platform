with open('templates/program_billing/manager_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

import re

old_tile1_regex = r"<!-- Tile 1: Setup Properties -->\s*{% if draft_property %}\s*<div class=\"block rounded-xl border p-6 shadow-sm bg-slate-50 border-slate-200 opacity-75 cursor-not-allowed\">\s*<div class=\"font-semibold text-slate-900\">1\. Setup Properties</div>\s*<div class=\"mt-1 text-sm text-slate-600\">Locked\. You must finish setting up '{{ draft_property\.name }}' first\.</div>\s*</div>\s*{% else %}"

new_tile1 = """<!-- Tile 1: Setup Properties -->
{% if draft_property %}
<div class="block rounded-xl border p-6 shadow-sm bg-white border-blue-200 relative group transition hover:shadow-md">
  <div class="font-semibold text-blue-800">1. Active Setup: {{ draft_property.name }}</div>
  <div class="mt-1 text-sm text-slate-600">You are currently setting up this property.</div>
  
  <div class="mt-4 flex space-x-3">
    <button onclick="openEditDraftModal()" class="text-xs bg-slate-100 hover:bg-slate-200 text-slate-700 font-bold py-1.5 px-3 rounded border border-slate-300 transition">
      Edit Details
    </button>
    <form action="{{ url_for('billing_bp.delete_draft', property_id=draft_property.id) }}" method="POST" class="inline" onsubmit="return confirm('Are you sure you want to delete this draft and start over?');">
      <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
      <button type="submit" class="text-xs bg-red-50 hover:bg-red-100 text-red-600 font-bold py-1.5 px-3 rounded border border-red-200 transition">
        Cancel Setup
      </button>
    </form>
  </div>
</div>

<!-- Edit Draft Modal -->
<div id="editDraftModal" class="hidden fixed inset-0 bg-slate-900 bg-opacity-50 flex items-center justify-center z-50">
  <div class="bg-white rounded-xl shadow-lg max-w-md w-full p-6">
    <div class="flex justify-between items-center mb-4">
      <h3 class="text-xl font-bold text-slate-800">Edit Property Setup</h3>
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
            <label class="block text-sm font-semibold text-slate-700 mb-1">Statements</label>
            <input type="number" name="expected_bills" value="{{ draft_property.expected_bills }}" min="1" required class="w-full rounded border-2 border-slate-300 px-3 py-2 outline-none focus:border-blue-500">
          </div>
          <div>
            <label class="block text-sm font-semibold text-slate-700 mb-1">Tenants/Units</label>
            <input type="number" name="expected_tenants" value="{{ draft_property.expected_tenants }}" min="1" required class="w-full rounded border-2 border-slate-300 px-3 py-2 outline-none focus:border-blue-500">
          </div>
        </div>
        <div class="grid grid-cols-2 gap-4">
          <div>
            <label class="block text-sm font-semibold text-slate-700 mb-1">Bulk Meter?</label>
            <select name="is_bulk_metered" class="w-full rounded border-2 border-slate-300 px-3 py-2 outline-none focus:border-blue-500 bg-white">
              <option value="1" {% if draft_property.is_bulk_metered %}selected{% endif %}>Yes</option>
              <option value="0" {% if not draft_property.is_bulk_metered %}selected{% endif %}>No</option>
            </select>
          </div>
          <div>
            <label class="block text-sm font-semibold text-slate-700 mb-1">Sub-Meters</label>
            <input type="number" name="expected_sub_meters" value="{{ draft_property.expected_sub_meters }}" min="0" required class="w-full rounded border-2 border-slate-300 px-3 py-2 outline-none focus:border-blue-500">
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
{% else %}"""

content = re.sub(old_tile1_regex, new_tile1, content, count=1)

with open('templates/program_billing/manager_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Tile 1 regex replacement executed.")
