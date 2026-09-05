import re

file_path = 'templates/shared/ait_certificate_layout.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# Change header
old_header = r'<div class="header-title">AIT</div>\s*<div class="header-subtitle">Archoney Institute of Technology</div>'
new_header = '<div class="header-title">Archoney Institute of Technology</div>'
text = re.sub(old_header, new_header, text)

# Add extra_details block
old_cert_row = r'<tr>\s*<th>Certificate Number</th>\s*<td colspan="3">\{% block certificate_number %\}\{\{ certificate_id \}\}\{% endblock %\}</td>\s*</tr>'
new_cert_row = '''<tr>
      <th>Certificate Number</th>
      <td colspan="3">{% block certificate_number %}{{ certificate_id }}{% endblock %}</td>
    </tr>
    {% block extra_details %}{% endblock %}'''
text = re.sub(old_cert_row, new_cert_row, text)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
