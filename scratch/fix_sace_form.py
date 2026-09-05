import re

with open('templates/admin/security/sace_management.html', 'r', encoding='utf-8') as f:
    text = f.read()

old_form = """                            <div>
                                <label class="block text-sm font-medium text-slate-700">Password</label>
                                <input type="password" name="password" required class="mt-1 block w-full rounded-md border-slate-300 shadow-sm focus:border-teal-500 focus:ring-teal-500">
                            </div>"""

new_form = """                            <div>
                                <label class="block text-sm font-medium text-slate-700">Password</label>
                                <input type="password" name="password" required class="mt-1 block w-full rounded-md border-slate-300 shadow-sm focus:border-teal-500 focus:ring-teal-500">
                            </div>
                            <div>
                                <label class="block text-sm font-medium text-slate-700">SACE Activity to Evaluate</label>
                                <select name="assigned_subject_slug" required class="mt-1 block w-full rounded-md border-slate-300 shadow-sm focus:border-teal-500 focus:ring-teal-500">
                                    <option value="sace_participant">Reading (LITRE Method)</option>
                                    <option value="sace_home">HOME (Math)</option>
                                    <!-- Add more specific SACE slugs here as they get approved -->
                                </select>
                            </div>"""

text = text.replace(old_form, new_form)

with open('templates/admin/security/sace_management.html', 'w', encoding='utf-8') as f:
    f.write(text)

