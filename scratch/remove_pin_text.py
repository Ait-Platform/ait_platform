import re

with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace the giant PIN display
old_pin_display = """                        <p class="text-xl text-slate-300 mb-8">Open your Participant App and enter PIN:</p>
                        <div class="text-[6rem] font-black tracking-widest text-white mb-8 border-4 border-indigo-500 px-12 py-4 rounded-xl bg-slate-800">8842</div>"""
new_pin_display = """                        <p class="text-xl text-slate-500 mb-8">Please open your Participant App.</p>"""
content = content.replace(old_pin_display, new_pin_display)

# Replace the controls lobby text
old_controls_text = """<p class="text-slate-300 text-sm leading-relaxed">Instruct teachers to enter PIN 8842. Once everyone is connected, click Start to begin the CPTD Activity.</p>"""
new_controls_text = """<p class="text-slate-300 text-sm leading-relaxed">Wait for teachers to log in. Click Start to begin the CPTD Activity.</p>"""
content = content.replace(old_controls_text, new_controls_text)

with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Removed PIN text from facilitator dashboard")
