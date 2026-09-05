import re
html_path = 'templates/program_sace/provisioning_map.html'
with open(html_path, 'r', encoding='utf-8') as f:
    html = f.read()

# Replace the secure_view links with direct static links
old_link_1 = "{{ url_for('sace_bp.secure_view', doc_type='app_form_1', return_to='control_centre') }}"
new_link_1 = "{{ url_for('static', filename='pdf/App_Form_1.pdf') }}"
html = html.replace(old_link_1, new_link_1)

old_link_2 = "{{ url_for('sace_bp.secure_view', doc_type='app_form_2', return_to='control_centre') }}"
new_link_2 = "{{ url_for('static', filename='pdf/App_Form_2.pdf') }}"
html = html.replace(old_link_2, new_link_2)

old_link_3 = "{{ url_for('sace_bp.secure_view', doc_type='f_cv', return_to='control_centre') }}"
new_link_3 = "{{ url_for('static', filename='pdf/Facilitator_CVs.pdf') }}"
html = html.replace(old_link_3, new_link_3)

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(html)
