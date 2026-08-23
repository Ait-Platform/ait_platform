import re

with open('templates/program_mechanic/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_textarea = '''<textarea id="tc-textarea" name="terms_conditions" rows="4" class="block w-full rounded-lg border-2 border-indigo-200 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm p-3 transition">{{ active_shop.terms_conditions if active_shop else '' }}</textarea>'''
new_textarea = '''<textarea id="tc-textarea" name="terms_and_conditions" rows="4" class="block w-full rounded-lg border-2 border-indigo-200 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm p-3 transition">{{ active_shop.terms_and_conditions if active_shop else '' }}</textarea>'''

content = content.replace(old_textarea, new_textarea)

with open('templates/program_mechanic/dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
