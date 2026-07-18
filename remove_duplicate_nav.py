import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

bad_buttons = """        <div class="mt-8 flex justify-between">
          <button type="button" onclick="prevStep()" class="px-6 py-2 bg-slate-200 text-slate-700 font-bold rounded-lg hover:bg-slate-300 transition">Back</button>
          <button type="button" onclick="nextStep()" class="px-6 py-2 bg-blue-600 text-white font-bold rounded-lg hover:bg-blue-700 transition">Next Step</button>
        </div>"""

content = content.replace(bad_buttons, "")

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Removed accidental step-specific navigation buttons.")
