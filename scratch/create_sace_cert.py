html_content = '''{% extends "shared/ait_certificate_layout.html" %}

{% block document_title_meta %}SACE Completion Certificate{% endblock %}
{% block document_title %}Completion Certificate{% endblock %}

{% block learner_name %}{{ learner_name }}{% endblock %}
{% block issue_date %}{{ completed_date }}{% endblock %}
{% block certificate_number %}{{ certificate_id }}{% endblock %}
{% block programme_name %}Sace approved activity : i learn to read using the LiTRE Method{% endblock %}

{% block achievement %}
Successfully completed the Workshop Simulation
{% endblock %}

{% block competencies %}
<table class="comp-table">
  <tr>
    <th>Module</th>
    <th>Status</th>
  </tr>
  <tr>
    <td>LITRE Methodology & Practical Engagement</td>
    <td>Completed</td>
  </tr>
</table>
{% endblock %}

{% block disclaimer %}
This certificate recognises successful participation and completion of the SACE endorsed workshop simulation.
{% endblock %}
'''

import os
os.makedirs('templates/program_sace/post_test', exist_ok=True)
with open('templates/program_sace/post_test/certificate_pdf.html', 'w', encoding='utf-8') as f:
    f.write(html_content)
