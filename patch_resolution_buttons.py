with open("templates/program_uip/dashboards/resolution_view.html", "r", encoding="utf-8") as f:
    text = f.read()

# YEA Button
old_yea = 'class="flex-1 py-3 rounded-lg font-black text-white bg-emerald-600 hover:bg-emerald-700 transition shadow-sm text-center"'
new_yea = 'class="flex-1 py-3 rounded-lg font-black text-emerald-700 bg-emerald-50 border border-emerald-200 hover:bg-emerald-100 hover:border-emerald-300 transition shadow-sm text-center"'
text = text.replace(old_yea, new_yea)

# NAY Button
old_nay = 'class="flex-1 py-3 rounded-lg font-black text-white bg-red-600 hover:bg-red-700 transition shadow-sm text-center"'
new_nay = 'class="flex-1 py-3 rounded-lg font-black text-rose-700 bg-rose-50 border border-rose-200 hover:bg-rose-100 hover:border-rose-300 transition shadow-sm text-center"'
text = text.replace(old_nay, new_nay)

# ABSTAIN Button
old_abstain = 'class="flex-1 py-3 rounded-lg font-black text-slate-600 bg-white border border-slate-300 hover:bg-slate-50 transition shadow-sm text-center"'
new_abstain = 'class="flex-1 py-3 rounded-lg font-black text-amber-700 bg-amber-50 border border-amber-200 hover:bg-amber-100 hover:border-amber-300 transition shadow-sm text-center"'
text = text.replace(old_abstain, new_abstain)

with open("templates/program_uip/dashboards/resolution_view.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated voting buttons")
