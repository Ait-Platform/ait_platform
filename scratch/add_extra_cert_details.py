import re

file_path = 'templates/program_sace/post_test/certificate_pdf.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

extra = '''
{% block extra_details %}
<tr>
    <th>Workshop Date</th>
    <td>{{ completed_date }}</td>
    <th>Location</th>
    <td>Sandton Convention Centre, JHB</td>
</tr>
{% endblock %}
'''

if '{% block extra_details %}' not in text:
    text = text.replace('{% block programme_name %}', extra + '\n{% block programme_name %}')

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
