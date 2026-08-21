import re

with open('templates/program_mechanic/invoice_view.html', 'r', encoding='utf-8') as f:
    content = f.read()

badge_code = '''        {% if 'SAMPLE' in job_card.job_number %}
        <div class="absolute top-0 right-0 bg-red-600 text-white font-black text-xl py-2 px-12 transform rotate-45 translate-x-12 translate-y-6 shadow-md opacity-80 pointer-events-none z-50">
          PREVIEW ONLY
        </div>
        {% endif %}'''

# Add badge inside the main white container (max-w-4xl)
content = content.replace('<div class="max-w-4xl mx-auto bg-white rounded-xl shadow-lg border border-slate-200 overflow-hidden relative">', '<div class="max-w-4xl mx-auto bg-white rounded-xl shadow-lg border border-slate-200 overflow-hidden relative">\n' + badge_code)

with open('templates/program_mechanic/invoice_view.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated invoice_view.html with PREVIEW badge")
