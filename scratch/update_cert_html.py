import re

file_path = 'templates/program_sace/post_test/certificate_pdf.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

# Replace the static competencies table
old_comp = '''<table class="comp-table">
  <tr>
    <th>Module</th>
    <th>Status</th>
  </tr>
  <tr>
    <td>LITRE Methodology & Practical Engagement</td>
    <td>Completed</td>
  </tr>
</table>'''

new_comp = '''<table class="comp-table">
  <tr>
    <th>Classroom Application / Practical Demonstration</th>
    <th>Status</th>
  </tr>
  {% if answers and answers.competencies %}
    {% for comp in answers.competencies %}
    <tr>
      <td>{{ comp }}</td>
      <td>Demonstrated</td>
    </tr>
    {% endfor %}
  {% else %}
    <tr>
      <td>LITRE Methodology & Practical Engagement</td>
      <td>Completed</td>
    </tr>
  {% endif %}
</table>'''

text = text.replace(old_comp, new_comp)

# Add the overall result block (it extends the base template, so we can override the block)
overall_result_block = '''
{% block overall_result %}
  {% if answers and answers.score is defined %}
    Overall Result: {{ answers.score }}%
  {% else %}
    Overall Result: Pass
  {% endif %}
{% endblock %}
'''

text += overall_result_block

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)

