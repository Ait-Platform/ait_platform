with open("templates/program_uip/dashboards/resolution_view.html", "r", encoding="utf-8") as f:
    text = f.read()

import re

# 1. Update voting button colors to 'robot' styling
old_yea = '<button type="submit" name="vote" value="YEA" class="flex-1 py-3 rounded-lg font-black text-white bg-emerald-600 hover:bg-emerald-700 transition shadow-sm text-center">'
new_yea = '<button type="submit" name="vote" value="YEA" class="flex-1 py-3 rounded-lg font-black text-emerald-700 bg-emerald-50 border border-emerald-200 hover:bg-emerald-100 hover:border-emerald-300 transition shadow-sm text-center">'
text = text.replace(old_yea, new_yea)

old_nay = '<button type="submit" name="vote" value="NAY" class="flex-1 py-3 rounded-lg font-black text-white bg-red-600 hover:bg-red-700 transition shadow-sm text-center">'
new_nay = '<button type="submit" name="vote" value="NAY" class="flex-1 py-3 rounded-lg font-black text-rose-700 bg-rose-50 border border-rose-200 hover:bg-rose-100 hover:border-rose-300 transition shadow-sm text-center">'
text = text.replace(old_nay, new_nay)

old_abstain = '<button type="submit" name="vote" value="ABSTAIN" class="flex-1 py-3 rounded-lg font-black text-slate-600 bg-white border border-slate-300 hover:bg-slate-50 transition shadow-sm text-center">'
new_abstain = '<button type="submit" name="vote" value="ABSTAIN" class="flex-1 py-3 rounded-lg font-black text-amber-700 bg-amber-50 border border-amber-200 hover:bg-amber-100 hover:border-amber-300 transition shadow-sm text-center">'
text = text.replace(old_abstain, new_abstain)

# 2. Replace Manual Override block with 72-Hour Notice
override_pattern = r'{% if current_appointment and current_appointment\.position\.lower\(\) in \[.*?\] %}\s*<div class="bg-slate-900.*?</div>\s*{% endif %}'
notice = """<div class="bg-indigo-50 rounded-xl shadow-sm p-6 border border-indigo-100 mt-6">
                <h3 class="font-bold text-indigo-900 text-lg mb-2"><i class="fas fa-clock text-indigo-500 mr-2"></i> 72-Hour Voting Window</h3>
                <p class="text-xs text-indigo-700 leading-relaxed">This resolution is governed by a strict 72-hour digital voting window. If a 100% quorum is not reached within 3 days, this resolution will automatically be locked and escalated to the Public Meeting Agenda for formal ratification.</p>
            </div>"""
text = re.sub(override_pattern, notice, text, flags=re.DOTALL)

with open("templates/program_uip/dashboards/resolution_view.html", "w", encoding="utf-8") as f:
    f.write(text)

print("Restored and patched base resolution_view.html")
