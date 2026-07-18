import re

with open('templates/program_billing/manager_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Update the Add Property Tile
old_button = """<button onclick="document.getElementById('setupModal').classList.remove('hidden')" class="text-left block rounded-xl border p-8 shadow-sm transition hover:shadow-md bg-white border-slate-200 hover:border-blue-300 group">"""
new_button = """<button onclick="openSetupModal()" class="text-left block rounded-xl border-2 p-8 shadow transition hover:shadow-md bg-blue-50 border-blue-400 hover:border-blue-600 group">"""
content = content.replace(old_button, new_button)

# 2. Update the input field to have an ID
old_input = """<input type="text" name="property_name" required autofocus class="w-full border-2 border-slate-400 rounded-lg px-3 py-3 text-lg font-bold focus:ring-2 focus:ring-blue-500 focus:outline-none" placeholder="e.g. Sunset Apartments">"""
new_input = """<input type="text" id="property_name_input" name="property_name" required class="w-full border-2 border-slate-400 rounded-lg px-3 py-3 text-lg font-bold focus:ring-2 focus:ring-blue-500 focus:outline-none" placeholder="e.g. Sunset Apartments">"""
content = content.replace(old_input, new_input)

# 3. Add JS function at the end
js_to_add = """
<script>
function openSetupModal() {
  const modal = document.getElementById('setupModal');
  modal.classList.remove('hidden');
  setTimeout(() => {
    document.getElementById('property_name_input').focus();
  }, 100);
}
</script>
"""
if "openSetupModal()" not in content:
    content = content.replace('{% endblock %}', js_to_add + '\n{% endblock %}')

with open('templates/program_billing/manager_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)

# Now, update routes.py to capitalize the property name
with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    routes = f.read()

old_prop_name_def = 'prop_name = request.form.get("property_name", "Draft Property").strip()'
new_prop_name_def = 'prop_name = request.form.get("property_name", "Draft Property").strip().title()'

routes = routes.replace(old_prop_name_def, new_prop_name_def)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(routes)

print("Modifications applied.")
