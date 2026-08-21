import sys

with open('templates/program_debtors/soa_template.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Extends
content = content.replace("{% extends 'layout.html' %}", "{% if not is_pdf %}{% extends 'layout.html' %}{% endif %}")

# 2. Block head (adding html, head, body)
old_head = "{% block head %}"
new_head = '''{% block head %}
{% if is_pdf %}
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
{% endif %}'''
content = content.replace(old_head, new_head, 1)

old_endhead = "{% endblock %}"
new_endhead = '''{% if is_pdf %}
</head>
<body style="background: white; margin: 0; padding: 0;">
{% endif %}
{% endblock %}'''
parts = content.split(old_endhead, 1)
content = parts[0] + new_endhead + parts[1]

# 3. Block content end (closing body, html)
old_content = "{% block content %}"
parts = content.rsplit(old_endhead, 1)
new_end_content = '''{% if is_pdf %}
</body>
</html>
{% endif %}
{% endblock %}'''
content = parts[0] + new_end_content + parts[1]


# 4. _external=True for logos
content = content.replace(
    '''<img src="{{ url_for('static', filename='uploads/mechanic/' + profile.logo_url) }}" alt="Letterhead" class="w-full object-cover">''',
    '''<img src="{{ url_for('static', filename='uploads/mechanic/' + profile.logo_url, _external=True) }}" alt="Letterhead" class="w-full object-cover">'''
)
content = content.replace(
    '''<img src="{{ url_for('static', filename='uploads/mechanic/' + profile.logo_url) }}" alt="Company Logo" class="max-h-24 object-contain">''',
    '''<img src="{{ url_for('static', filename='uploads/mechanic/' + profile.logo_url, _external=True) }}" alt="Company Logo" class="max-h-24 object-contain">'''
)

# 5. Hide the top control bar
target_controls = '''<div class="max-w-4xl mx-auto mb-8 mt-6 no-print">
  <div class="bg-white rounded-xl shadow overflow-hidden border border-slate-200">
    <div class="h-3 bg-blue-600"></div>
    <div class="p-6">
      <!-- Row 1: Title and Back button -->
      <div class="flex justify-between items-center mb-4 border-b pb-4">
        <h1 class="text-2xl font-bold text-slate-800">Statement Preview</h1>
        {% if return_url %}
          <a href="{{ return_url }}" class="px-4 py-2 bg-gray-200 text-gray-700 rounded hover:bg-gray-300 font-semibold text-sm shadow-sm transition">Back</a>
        {% else %}
          <a href="{{ url_for('debtors_bp.debtor_view', debtor_id=debtor.id) }}" class="px-4 py-2 bg-gray-200 text-gray-700 rounded hover:bg-gray-300 font-semibold text-sm shadow-sm transition">Back</a>
        {% endif %}
      </div>
      <!-- Row 2: Action buttons -->
      <div class="flex gap-3 justify-end mt-4">
        <button onclick="window.print()" class="inline-flex items-center gap-2 rounded-lg bg-gray-800 px-5 py-2.5 text-sm font-semibold text-white shadow-sm hover:bg-gray-700 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-gray-900 transition">
          <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M5 4v3H4a2 2 0 00-2 2v3a2 2 0 002 2h1v2a2 2 0 002 2h6a2 2 0 002-2v-2h1a2 2 0 002-2V9a2 2 0 00-2-2h-1V4a2 2 0 00-2-2H7a2 2 0 00-2 2zm8 0H7v3h6V4zm0 8H7v4h6v-4z" clip-rule="evenodd" /></svg>
          Print PDF
        </button>
        <button onclick="document.getElementById('email-soa-modal').classList.remove('hidden')" class="inline-flex items-center gap-2 rounded-lg bg-blue-600 px-5 py-2.5 text-sm font-semibold text-white shadow-sm hover:bg-blue-500 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-blue-600 transition">
          <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" viewBox="0 0 20 20" fill="currentColor"><path d="M2.003 5.884L10 9.882l7.997-3.998A2 2 0 0016 4H4a2 2 0 00-1.997 1.884z" /><path d="M18 8.118l-8 4-8-4V14a2 2 0 002 2h12a2 2 0 002-2V8.118z" /></svg>
          Email to Client
        </button>
      </div>
    </div>
  </div>
</div>'''

content = content.replace(target_controls, "{% if not is_pdf %}\n" + target_controls + "\n{% endif %}")

# 6. Hide flash messages
target_flashes = '''<div class="max-w-4xl mx-auto mb-4 no-print">
    {% include "partials/flash_messages.html" %}
</div>'''
content = content.replace(target_flashes, "{% if not is_pdf %}\n" + target_flashes + "\n{% endif %}")

# 7. Hide Email SOA Modal
target_modal = '''<!-- Email SOA Modal -->'''
target_modal_end = '''    </div>
  </div>
</div>'''

# The modal ends right before {% endblock %}.
import re
# Wrap the modal entirely
content = re.sub(r'(<!-- Email SOA Modal -->.*?)\n\s*{%\s*endif\s*%}\s*{%\s*if is_pdf\s*%}', r'{% if not is_pdf %}\n\1\n{% endif %}\n{% endif %}\n{% if is_pdf %}', content, flags=re.DOTALL)

with open('templates/program_debtors/soa_template.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("soa_template.html fixed for good!")
