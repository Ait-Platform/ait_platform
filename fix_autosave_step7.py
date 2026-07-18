import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix Step 7
old_w = 'onchange="refreshDropdownExclusivity()"'
new_w = 'onchange="refreshDropdownExclusivity(); triggerAutoSave();"'
content = content.replace(old_w, new_w)

# Also let's fix Step 6 inputs just in case they don't autosave either!
old_stolen = 'class="exc-stolen-num w-full'
new_stolen = 'class="exc-stolen-num w-full" onchange="triggerAutoSave()"'
content = content.replace(old_stolen, new_stolen)

old_ds = 'class="exc-date-stolen w-full'
new_ds = 'class="exc-date-stolen w-full" onchange="triggerAutoSave()"'
content = content.replace(old_ds, new_ds)

old_dr = 'class="exc-date-replaced w-full'
new_dr = 'class="exc-date-replaced w-full" onchange="triggerAutoSave()"'
content = content.replace(old_dr, new_dr)

old_rep = 'class="exc-replacement-id w-full'
new_rep = 'class="exc-replacement-id w-full" onchange="triggerAutoSave()"'
content = content.replace(old_rep, new_rep)

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Added missing triggerAutoSave() to Step 6 and 7.")
