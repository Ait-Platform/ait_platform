import re

with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Fix the faint text and change CPTD to Reading Activity
old_block = """<h3 class="font-bold text-indigo-300 mb-2"><i class="fas fa-users mr-2"></i>Waiting for Participants</h3>
                            <p class="text-slate-300 text-sm leading-relaxed">Wait for teachers to log in. Click Start to begin the CPTD Activity.</p>"""

new_block = """<h3 class="font-bold text-white mb-2 text-lg"><i class="fas fa-users mr-2 text-indigo-400"></i>Waiting for Participants</h3>
                            <p class="text-slate-100 text-base leading-relaxed">Wait for participants to log in. Click Start to begin the Reading Activity.</p>"""

if old_block in text:
    text = text.replace(old_block, new_block)
    with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
        f.write(text)
    print("Fixed the faint text and updated copy")
else:
    print("Block not found exactly. Searching dynamically...")
    if "Waiting for Participants" in text:
        text = text.replace("text-indigo-300", "text-white text-lg")
        text = text.replace("text-slate-300 text-sm", "text-slate-100 text-base")
        text = text.replace("CPTD Activity", "Reading Activity")
        with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
            f.write(text)
        print("Fixed dynamically")
