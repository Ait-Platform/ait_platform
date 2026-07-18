import re

with open('templates/program_billing/manager_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# For start_setup form:
old_start = """          <div>
            <label class="block text-sm font-semibold text-slate-700 mb-1">Statement(s) <span class="text-rose-500">*</span></label>
            <input type="number" name="expected_tenants" min="1" required class="w-full rounded border-2 border-slate-300 px-3 py-2 outline-none focus:border-blue-500">
            <p class="text-xs text-slate-500 mt-1">Total units/tenants to be billed.</p>
          </div>
        </div>
        
        <div class="grid grid-cols-2 gap-4">"""

new_start = """          <div>
            <label class="block text-sm font-semibold text-slate-700 mb-1">Statement(s) <span class="text-rose-500">*</span></label>
            <input type="number" name="expected_tenants" min="1" required class="w-full rounded border-2 border-slate-300 px-3 py-2 outline-none focus:border-blue-500">
            <p class="text-xs text-slate-500 mt-1">Total units/tenants to be billed.</p>
          </div>
        </div>

        <div class="grid grid-cols-2 gap-4">
          <div>
            <label class="block text-sm font-semibold text-slate-700 mb-1">Total Water Meters <span class="text-rose-500">*</span></label>
            <input type="number" name="expected_water_meters" min="0" value="0" required class="w-full rounded border-2 border-slate-300 px-3 py-2 outline-none focus:border-blue-500">
          </div>
          <div>
            <label class="block text-sm font-semibold text-slate-700 mb-1">Total Elec Meters <span class="text-rose-500">*</span></label>
            <input type="number" name="expected_elec_meters" min="0" value="0" required class="w-full rounded border-2 border-slate-300 px-3 py-2 outline-none focus:border-blue-500">
          </div>
        </div>
        
        <div class="grid grid-cols-2 gap-4 mt-4">"""

if old_start in content:
    content = content.replace(old_start, new_start)
else:
    print("Could not find start setup fields!")

with open('templates/program_billing/manager_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)

# Now, let's update routes.py
with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    routes_content = f.read()

# Update edit_draft
old_edit = """        prop.expected_sub_meters = int(request.form.get("expected_sub_meters") or prop.expected_sub_meters)"""
new_edit = """        prop.expected_sub_meters = int(request.form.get("expected_sub_meters") or prop.expected_sub_meters)
        prop.expected_water_meters = int(request.form.get("expected_water_meters") or prop.expected_water_meters)
        prop.expected_elec_meters = int(request.form.get("expected_elec_meters") or prop.expected_elec_meters)"""
routes_content = routes_content.replace(old_edit, new_edit)

# Update onboarding_start_setup
old_start_setup = """    prop = BilProperty(
        name=prop_name,
        expected_bills=bills,
        expected_tenants=tenants,
        manager_id=current_user.id,
        onboarding_status='draft_manual',
        is_bulk_metered=is_bulk
    )"""

new_start_setup = """    w_meters = int(request.form.get('expected_water_meters', 0))
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
routes_content = routes_content.replace(old_start_setup, new_start_setup)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(routes_content)

print("Updated backend routes and manager dashboard form.")
