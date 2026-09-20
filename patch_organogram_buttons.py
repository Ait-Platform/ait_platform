with open("templates/program_uip/dashboards/secretary_organogram.html", "r", encoding="utf-8") as f:
    text = f.read()

# 1. Edit Button (Rose)
old_edit = 'class="inline-flex items-center px-2 py-1 rounded bg-white border border-slate-200 text-slate-600 hover:text-indigo-700 hover:border-indigo-300 hover:bg-indigo-50 shadow-sm transition text-xs font-bold" title="Edit Seat"'
new_edit = 'class="inline-flex items-center px-2 py-1 rounded bg-rose-50 border border-rose-200 text-rose-700 hover:text-rose-800 hover:border-rose-300 hover:bg-rose-100 shadow-sm transition text-xs font-bold" title="Edit Seat"'
text = text.replace(old_edit, new_edit)

# 2. Assign Button (Emerald)
old_assign = 'class="inline-flex items-center px-2 py-1 rounded bg-white border border-slate-200 text-slate-600 hover:text-emerald-700 hover:border-emerald-300 hover:bg-emerald-50 shadow-sm transition text-xs font-bold" title="Assign Member"'
new_assign = 'class="inline-flex items-center px-2 py-1 rounded bg-emerald-50 border border-emerald-200 text-emerald-700 hover:text-emerald-800 hover:border-emerald-300 hover:bg-emerald-100 shadow-sm transition text-xs font-bold" title="Assign Member"'
text = text.replace(old_assign, new_assign)

# 3. Photo Button (Amber)
old_photo = 'class="inline-flex items-center px-2 py-1 rounded bg-white border border-slate-200 text-slate-600 hover:text-amber-700 hover:border-amber-300 hover:bg-amber-50 shadow-sm transition text-xs font-bold" title="Upload Photo"'
new_photo = 'class="inline-flex items-center px-2 py-1 rounded bg-amber-50 border border-amber-200 text-amber-700 hover:text-amber-800 hover:border-amber-300 hover:bg-amber-100 shadow-sm transition text-xs font-bold" title="Upload Photo"'
text = text.replace(old_photo, new_photo)

with open("templates/program_uip/dashboards/secretary_organogram.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated organogram buttons")
