import re

with open('templates/program_sace/about.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Change SACE Login button to SACE Registration
old_button = """<a href="{{ url_for('auth_bp.login') }}" class="inline-flex items-center px-8 py-4 border border-transparent text-lg font-bold rounded-xl shadow-sm text-teal-700 bg-teal-50 hover:bg-teal-100 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-teal-500 transition-all duration-200">
                SACE Login"""

new_button = """<a href="{{ url_for('auth_bp.register_decision', subject='sace_hub') }}" class="inline-flex items-center px-8 py-4 border border-transparent text-lg font-bold rounded-xl shadow-sm text-teal-700 bg-teal-50 hover:bg-teal-100 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-teal-500 transition-all duration-200">
                Access SACE Hub"""

content = content.replace(old_button, new_button)

with open('templates/program_sace/about.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated SACE about page CTA")
