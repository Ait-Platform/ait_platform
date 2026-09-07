import re

file_path = 'templates/uip/reception/new_interaction.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

old_select = '''                <select name="resident_email" class="w-full py-3 rounded border-slate-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 text-slate-700 bg-white">
                    <option value="">-- Anonymous / Unregistered Walk-in --</option>
                    <optgroup label="Good Standing (Paid Up)">
                        <option value="john.smith@example.com">John Smith (14 Manor Drive) - john.smith@example.com</option>
                        <option value="sarah.jenkins@example.com">Sarah Jenkins (22 Oak Avenue) - sarah.jenkins@example.com</option>
                        <option value="david.lee@example.com">David Lee (5 Pine Street) - david.lee@example.com</option>
                    </optgroup>
                    <optgroup label="Arrears (Not Paid)">
                        <option value="mike.ross@example.com">Mike Ross (11b Valley Road) - mike.ross@example.com</option>
                        <option value="emma.stone@example.com">Emma Stone (88 Ridge Way) - emma.stone@example.com</option>
                    </optgroup>
                </select>'''

new_select = '''                <select name="resident_email" class="w-full py-3 rounded border-slate-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 text-slate-700 bg-white">
                    <option value="">-- Anonymous / Unregistered Walk-in --</option>
                    <option value="john.smith@example.com">John Smith (14 Manor Drive) - john.smith@example.com</option>
                    <option value="sarah.jenkins@example.com">Sarah Jenkins (22 Oak Avenue) - sarah.jenkins@example.com</option>
                    <option value="david.lee@example.com">David Lee (5 Pine Street) - david.lee@example.com</option>
                    <option value="mike.ross@example.com">Mike Ross (11b Valley Road) - mike.ross@example.com</option>
                    <option value="emma.stone@example.com">Emma Stone (88 Ridge Way) - emma.stone@example.com</option>
                </select>'''

text = text.replace(old_select, new_select)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)

