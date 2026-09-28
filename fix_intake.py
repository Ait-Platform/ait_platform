import re

filepath = 'templates/program_uip/dashboards/secretary_intake.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

bad_button = '''{% if claim.type == 'mo_claim' %}
                                <button type="button" onclick="openVerifyModal('{{ claim.id }}', '{{ claim.user_name }}')" class="text-indigo-600 hover:text-indigo-900 font-bold mr-4 transition">
                                    <i class="fas fa-link mr-1"></i> Verify via Mandate
                                </button>
                                {% endif %}'''

# If the exact class isn't exactly like that, let me use regex.
content = re.sub(
    r'\{%\s*if claim\.type == \'mo_claim\'\s*%\}\s*<button type="button" onclick="openVerifyModal\(\'\{\{ claim\.id \}\}\', \'\{\{ claim\.user_name \}\}\'\)"[^>]*>\s*<i class="fas fa-link mr-1"></i> Verify via Mandate\s*</button>\s*\{%\s*endif\s*%\}',
    r'''<button type="button" onclick="openVerifyModal('{{ claim.id }}', '{{ claim.user_name }}')" class="text-indigo-600 hover:text-indigo-900 font-bold mr-4 transition">
                                    <i class="fas fa-link mr-1"></i> Verify via Mandate
                                </button>''',
    content,
    flags=re.DOTALL
)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Intake html fixed")
