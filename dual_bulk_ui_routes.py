import re

# UPDATE MANAGER DASHBOARD HTML
with open('templates/program_billing/manager_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. New Property Setup
old_new_setup = """            <div class="mb-4">
              <label class="block text-sm font-bold text-slate-700 mb-2">Is this a Bulk Metered property?</label>
              <select name="is_bulk" class="w-full border-2 border-slate-400 rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:outline-none">
                <option value="no">No, just standard individual meters</option>
                <option value="yes">Yes, it has a main bulk meter and sub-meters</option>
              </select>
            </div>
            <div class="mb-6">
              <label class="block text-sm font-bold text-slate-700 mb-2">If Bulk, how many sub-meters are linked?</label>
              <input type="number" name="sub_meters" min="0" value="0" class="w-full border-2 border-slate-400 rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:outline-none">
            </div>"""

new_new_setup = """            <div class="grid grid-cols-2 gap-4 mb-6">
              <div>
                <label class="block text-sm font-bold text-slate-700 mb-2">Master Bulk Water Meter?</label>
                <select name="is_bulk_water" class="w-full border-2 border-slate-400 rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:outline-none">
                  <option value="no">No</option>
                  <option value="yes">Yes</option>
                </select>
              </div>
              <div>
                <label class="block text-sm font-bold text-slate-700 mb-2">Master Bulk Electrical Meter?</label>
                <select name="is_bulk_elec" class="w-full border-2 border-slate-400 rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:outline-none">
                  <option value="no">No</option>
                  <option value="yes">Yes</option>
                </select>
              </div>
            </div>"""

content = content.replace(old_new_setup, new_new_setup)

# 2. Edit Property Setup
old_edit_setup = """        <div class="grid grid-cols-2 gap-4 mt-4">
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
        </div>"""

new_edit_setup = """        <div class="grid grid-cols-2 gap-4 mt-4">
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
        </div>"""

content = content.replace(old_edit_setup, new_edit_setup)

with open('templates/program_billing/manager_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)

# UPDATE ROUTES.PY
with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    r_content = f.read()

# 1. onboarding_start_setup
old_start_route = """    is_bulk = request.form.get('is_bulk') == 'yes'
    sub_meters = int(request.form.get('sub_meters') or 0)
    w_meters = int(request.form.get('expected_water_meters', 0))
    e_meters = int(request.form.get('expected_elec_meters', 0))
    
    prop = BilProperty(
        name=prop_name,
        expected_bills=bills,
        expected_tenants=tenants,
        expected_water_meters=w_meters,
        expected_elec_meters=e_meters,
        manager_id=current_user.id,
        onboarding_status='draft_manual',
        is_bulk_metered=is_bulk
    )"""

new_start_route = """    is_bw = request.form.get('is_bulk_water') == 'yes'
    is_be = request.form.get('is_bulk_elec') == 'yes'
    w_meters = int(request.form.get('expected_water_meters', 0))
    e_meters = int(request.form.get('expected_elec_meters', 0))
    
    prop = BilProperty(
        name=prop_name,
        expected_bills=bills,
        expected_tenants=tenants,
        expected_water_meters=w_meters,
        expected_elec_meters=e_meters,
        manager_id=current_user.id,
        onboarding_status='draft_manual',
        is_bulk_water=is_bw,
        is_bulk_elec=is_be
    )"""
r_content = r_content.replace(old_start_route, new_start_route)

# 2. edit_draft
old_edit_route = """        prop.expected_tenants = int(request.form.get("expected_tenants") or prop.expected_tenants)
        prop.is_bulk_metered = int(request.form.get("is_bulk_metered") or prop.is_bulk_metered)
        prop.expected_sub_meters = int(request.form.get("expected_sub_meters") or prop.expected_sub_meters)
        prop.expected_water_meters = int(request.form.get("expected_water_meters") or prop.expected_water_meters)
        prop.expected_elec_meters = int(request.form.get("expected_elec_meters") or prop.expected_elec_meters)"""

new_edit_route = """        prop.expected_tenants = int(request.form.get("expected_tenants") or prop.expected_tenants)
        prop.is_bulk_water = int(request.form.get("is_bulk_water") or 0) == 1
        prop.is_bulk_elec = int(request.form.get("is_bulk_elec") or 0) == 1
        prop.expected_water_meters = int(request.form.get("expected_water_meters") or prop.expected_water_meters)
        prop.expected_elec_meters = int(request.form.get("expected_elec_meters") or prop.expected_elec_meters)"""
r_content = r_content.replace(old_edit_route, new_edit_route)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(r_content)

print("UI and Routes updated for dual bulk.")
