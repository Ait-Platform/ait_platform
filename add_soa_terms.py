import re

with open('templates/program_debtors/soa_template.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_footer = '''        <!-- Footer/Payment info -->
        <div class="mt-8 pt-4 border-t border-slate-200">
            {% if bank_account %}'''

new_footer = '''        <!-- Terms and Conditions -->
        {% if profile and profile.terms_and_conditions %}
        <div class="mt-8 pt-4 border-t border-slate-200 text-xs text-slate-500">
            <h4 class="font-bold text-slate-700 mb-1">Terms & Conditions</h4>
            <div class="whitespace-pre-line">{{ profile.terms_and_conditions }}</div>
        </div>
        {% endif %}
        
        <!-- Footer/Payment info -->
        <div class="mt-8 pt-4 border-t border-slate-200">
            {% if bank_account %}'''

content = content.replace(old_footer, new_footer)

with open('templates/program_debtors/soa_template.html', 'w', encoding='utf-8') as f:
    f.write(content)
