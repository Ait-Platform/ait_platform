import sys

with open('templates/program_debtors/soa_template.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Fix extends
if "{% extends 'layout.html' %}" in content:
    content = content.replace("{% extends 'layout.html' %}", "{% if not is_pdf %}{% extends 'layout.html' %}{% endif %}")

# 2. Fix block head
old_head = "{% block head %}"
new_head = '''{% block head %}
{% if is_pdf %}
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
{% endif %}'''
if old_head in content:
    content = content.replace(old_head, new_head, 1)

# 3. End block head / start body
old_endhead = "{% endblock %}"
new_endhead = '''{% if is_pdf %}
</head>
<body style="background: white; margin: 0; padding: 0;">
{% endif %}
{% endblock %}'''
# Be careful to only replace the FIRST {% endblock %} which closes head
if "{% endblock %}" in content:
    parts = content.split("{% endblock %}", 1)
    content = parts[0] + new_endhead + parts[1]

# 4. End body at the end of block content
old_content = "{% block content %}"
# The end of block content is at the very end of the file, let's find the LAST endblock
parts = content.rsplit("{% endblock %}", 1)
new_end_content = '''{% if is_pdf %}
</body>
</html>
{% endif %}
{% endblock %}'''
if len(parts) == 2:
    content = parts[0] + new_end_content + parts[1]


# 5. Fix _external=True for logos
old_logo_1 = '''<img src="{{ url_for('static', filename='uploads/mechanic/' + profile.logo_url) }}" alt="Letterhead" class="w-full object-cover">'''
new_logo_1 = '''<img src="{{ url_for('static', filename='uploads/mechanic/' + profile.logo_url, _external=True) }}" alt="Letterhead" class="w-full object-cover">'''
content = content.replace(old_logo_1, new_logo_1)

old_logo_2 = '''<img src="{{ url_for('static', filename='uploads/mechanic/' + profile.logo_url) }}" alt="Company Logo" class="max-h-24 object-contain">'''
new_logo_2 = '''<img src="{{ url_for('static', filename='uploads/mechanic/' + profile.logo_url, _external=True) }}" alt="Company Logo" class="max-h-24 object-contain">'''
content = content.replace(old_logo_2, new_logo_2)

# Strip out no-print blocks completely if is_pdf, so we don't even rely on CSS!
old_no_print_1 = '''<div class="max-w-4xl mx-auto mb-8 mt-6 no-print">'''
new_no_print_1 = '''{% if not is_pdf %}<div class="max-w-4xl mx-auto mb-8 mt-6 no-print">'''
content = content.replace(old_no_print_1, new_no_print_1)
# That block ends before the flash messages. Let's find:
#       </div>
#     </div>
#   </div>
# <div class="max-w-4xl mx-auto mb-4 no-print">
target_end_1 = '''    </div>
  </div>
<div class="max-w-4xl mx-auto mb-4 no-print">'''
new_end_1 = '''    </div>
  </div>{% endif %}
{% if not is_pdf %}<div class="max-w-4xl mx-auto mb-4 no-print">'''
content = content.replace(target_end_1, new_end_1)

target_end_2 = '''      {% include "partials/flash_messages.html" %}
  </div>
  <div class="max-w-4xl mx-auto bg-white p-10 shadow-lg print-container min-h-[1056px] border border-gray-200 relative">'''
new_end_2 = '''      {% include "partials/flash_messages.html" %}
  </div>{% endif %}
  <div class="max-w-4xl mx-auto bg-white p-10 shadow-lg print-container min-h-[1056px] border border-gray-200 relative">'''
content = content.replace(target_end_2, new_end_2)

with open('templates/program_debtors/soa_template.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("soa_template.html patched!")
