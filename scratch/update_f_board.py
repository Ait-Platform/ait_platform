import re

with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace 'Join the Room' and 'Please open your Participant App.'
old_lobby = """<h2 class="text-4xl font-bold text-indigo-400 mb-4">Join the Room</h2>
                        <p class="text-xl text-slate-500 mb-8">Please open your Participant App.</p>"""

new_lobby = """<h2 class="text-4xl font-bold text-indigo-400 mb-4">Welcome</h2>
                        <p class="text-xl text-slate-500 mb-8">Waiting for teachers to check in...</p>"""

content = content.replace(old_lobby, new_lobby)

with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated facilitator dashboard text")
