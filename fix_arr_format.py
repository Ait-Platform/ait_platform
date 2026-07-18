with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    text = f.read()

old_arr_amount = """<input type="number" class="arr-amount w-full rounded border border-slate-400 pl-8 pr-3 py-1.5 text-sm outline-none focus:border-blue-500 focus:ring-2 focus:ring-blue-100 shadow-inner transition bg-white" value="${savedArr.amount || ''}" onchange="triggerAutoSave()" step="0.01">"""

new_arr_amount = """<input type="text" class="arr-amount w-full rounded border border-slate-400 pl-8 pr-3 py-1.5 text-sm outline-none focus:border-blue-500 focus:ring-2 focus:ring-blue-100 shadow-inner transition bg-white" value="${savedArr.amount ? formatNumberSpace(savedArr.amount) : ''}" oninput="this.value=formatNumberSpace(this.value)" onchange="triggerAutoSave()">"""

text = text.replace(old_arr_amount, new_arr_amount)

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(text)
print("Updated .arr-amount input!")
