import re

with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Replace the lobby box
old_box = """<div class="p-4 bg-indigo-50 border border-indigo-200 shadow-sm rounded-lg">
                              <h3 class="font-bold text-indigo-900 mb-2 text-lg"><i class="fas fa-users mr-2 text-indigo-600"></i>Waiting for Participants</h3>
                              <p class="text-slate-700 text-base leading-relaxed">Wait for participants to log in. Click Start to begin the Reading Activity.</p>
                          </div>"""

new_box = """<div class="p-4 bg-indigo-50 border border-indigo-200 shadow-sm rounded-lg">
                              <h3 class="font-bold text-indigo-900 mb-2 text-lg"><i class="fas fa-play-circle mr-2 text-green-600"></i>Workshop Ready</h3>
                              <p class="text-slate-700 text-base leading-relaxed">Click Start to begin the Reading Activity.</p>
                          </div>"""

if old_box in text:
    text = text.replace(old_box, new_box)
    with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
        f.write(text)
    print("Fixed Lobby text styling")
else:
    print("Could not find lobby block.")
