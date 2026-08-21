import re

with open('templates/program_debtors/soa_template.html', 'r', encoding='utf-8') as f:
    content = f.read()

terms_html = '''
        {% if profile and profile.terms_and_conditions %}
        <div class="mt-12 pt-6 border-t border-gray-200 text-xs text-gray-500">
            <h4 class="font-bold text-gray-700 mb-2">Terms & Conditions</h4>
            <div class="whitespace-pre-line">{{ profile.terms_and_conditions }}</div>
        </div>
        {% endif %}
'''

# insert before <!-- Footer -->
content = content.replace('<!-- Footer -->', terms_html + '\n        <!-- Footer -->')

with open('templates/program_debtors/soa_template.html', 'w', encoding='utf-8') as f:
    f.write(content)
