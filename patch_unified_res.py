with open("templates/program_uip/dashboards/resolution_view.html", "r", encoding="utf-8") as f:
    text = f.read()

import re

# We want to replace the "Manual Override" block specifically.
override_pattern = r'{% if current_appointment[^%]+%}\s*<div class="bg-slate-900 rounded-xl shadow-sm p-6 text-white">\s*<h3 class="font-bold text-lg mb-2"><i class="fas fa-exclamation-triangle.*?</div>\s*{% endif %}'

notice = """
            <div class="bg-indigo-50 rounded-xl shadow-sm p-6 border border-indigo-100 mt-6">
                <h3 class="font-bold text-indigo-900 text-lg mb-2"><i class="fas fa-clock text-indigo-500 mr-2"></i> 72-Hour Voting Window</h3>
                <p class="text-xs text-indigo-700 leading-relaxed">This resolution is governed by a strict 72-hour digital voting window. If a 100% quorum is not reached within 3 days, this resolution will automatically be locked and escalated to the Public Meeting Agenda for formal ratification.</p>
            </div>
"""

text = re.sub(override_pattern, notice, text, flags=re.DOTALL)

with open("templates/program_uip/dashboards/resolution_view.html", "w", encoding="utf-8") as f:
    f.write(text)

print("Replaced manual override correctly.")
