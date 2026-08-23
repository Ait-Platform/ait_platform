import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Look for the exact block:
# {% if job_card.status == 'Quote' %}
#     <form method="POST" action="{{ url_for('mechanic_bp.accept_quote', id=job_card.id) }}" class="inline m-0">
#         <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
#         <button type="submit" class="px-4 py-2 bg-green-600 text-white font-bold rounded-lg hover:bg-green-700 shadow-sm transition text-sm">Confirm Job Card (Create Tax Invoice)</button>
#     </form>
# {% endif %}

regex = r'\{% if job_card\.status == \'Quote\' %\}\s*<form method="POST" action="\{\{ url_for\(\'mechanic_bp\.accept_quote\', id=job_card\.id\) \}\}" class="inline m-0">\s*<input type="hidden" name="csrf_token" value="\{\{ csrf_token\(\) \}\}">\s*<button type="submit" class="px-4 py-2 bg-green-600 text-white font-bold rounded-lg hover:bg-green-700 shadow-sm transition text-sm">Confirm Job Card \(Create Tax Invoice\)</button>\s*</form>\s*\{% endif %\}'

content = re.sub(regex, '', content, flags=re.DOTALL)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
