import re

with open('templates/program_practice_crm/staff.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_edit_form = """                <form method="POST" action="{{ url_for('practice_crm_bp.staff_edit', pu_id=pu.id) }}" class="flex flex-col sm:flex-row gap-4 sm:items-end">
                  <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
                  <div class="flex-1">
                    <label class="block text-sm font-bold text-slate-700 mb-1">Full Name</label>
                    <input type="text" name="name" value="{{ u.name }}" class="w-full rounded border-2 border-slate-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 px-3 py-2 bg-white">
                  </div>
                  <div class="flex-1">
                    <label class="block text-sm font-bold text-slate-700 mb-1">Phone Number</label>
                    <input type="text" id="phone-input-{{ pu.id }}" name="phone" value="{{ pu.phone or '' }}" class="w-full rounded border-2 border-slate-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 px-3 py-2 bg-white">
                  </div>
                  <div class="sm:w-auto w-full pt-2 sm:pt-0">"""

new_edit_form = """                <form method="POST" action="{{ url_for('practice_crm_bp.staff_edit', pu_id=pu.id) }}" class="flex flex-col sm:flex-row gap-4 sm:items-end flex-wrap">
                  <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
                  <div class="flex-1 min-w-[200px]">
                    <label class="block text-sm font-bold text-slate-700 mb-1">Full Name</label>
                    <input type="text" name="name" value="{{ u.name }}" class="w-full rounded border-2 border-slate-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 px-3 py-2 bg-white">
                  </div>
                  <div class="flex-1 min-w-[200px]">
                    <label class="block text-sm font-bold text-slate-700 mb-1">Phone Number</label>
                    <input type="text" id="phone-input-{{ pu.id }}" name="phone" value="{{ pu.phone or '' }}" class="w-full rounded border-2 border-slate-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 px-3 py-2 bg-white">
                  </div>
                  <div class="flex-1 min-w-[200px]">
                    <label class="block text-sm font-bold text-slate-700 mb-1">Reset Password</label>
                    <input type="password" name="password" autocomplete="new-password" placeholder="Leave blank to keep current" class="w-full rounded border-2 border-slate-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 px-3 py-2 bg-white">
                  </div>
                  <div class="sm:w-auto w-full pt-2 sm:pt-0">"""

if old_edit_form in content:
    content = content.replace(old_edit_form, new_edit_form)
    with open('templates/program_practice_crm/staff.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Updated staff.html")
else:
    print("Could not find the target string in staff.html")
