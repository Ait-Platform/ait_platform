import re

html_path = 'templates/program_sace/provisioning_map.html'
with open(html_path, 'r', encoding='utf-8') as f:
    html = f.read()

# 1. Title
html = html.replace('I Learn to Read Using the LITRE Method', 'I Learn to Read Using the LITRE')

# 2. URLs
html = html.replace("url_for('sace_bp.secure_view', doc_type='app_form_1')", "url_for('sace_bp.secure_view', doc_type='app_form_1', return_to='control_centre')")
html = html.replace("url_for('sace_bp.secure_view', doc_type='app_form_2')", "url_for('sace_bp.secure_view', doc_type='app_form_2', return_to='control_centre')")
html = html.replace("url_for('sace_bp.secure_view', doc_type='f_cv')", "url_for('sace_bp.secure_view', doc_type='f_cv', return_to='control_centre')")

# 3. Remove Banner
banner_start = html.find('<!-- Guest Registration Banner -->')
if banner_start != -1:
    banner_end = html.find('{% endif %}', banner_start) + len('{% endif %}')
    html = html[:banner_start - 10] + html[banner_end:]

# 4. Overflow Modal
old_modal_div = '<div class="bg-white rounded-2xl shadow-2xl w-full max-w-2xl overflow-hidden border border-slate-200">'
new_modal_div = '<div class="bg-white rounded-2xl shadow-2xl w-full max-w-2xl overflow-hidden border border-slate-200 max-h-[95vh] flex flex-col">'
html = html.replace(old_modal_div, new_modal_div)

old_modal_body = '<div class="p-8">'
new_modal_body = '<div class="p-8 overflow-y-auto">'
html = html.replace(old_modal_body, new_modal_body, 1)

# 5. Add forced modal at the end of block content
forced_modal = '''
<!-- Post-Pledge Registration Modal -->
{% if has_pledged and not current_user.is_authenticated %}
<div id="reg-modal" class="fixed inset-0 z-[70] bg-slate-900 bg-opacity-75 flex items-center justify-center p-4 backdrop-blur-sm pointer-events-auto">
    <div class="bg-white rounded-xl shadow-2xl w-full max-w-md overflow-hidden border border-slate-200 text-center relative">
        <div class="p-8">
            <div class="w-16 h-16 bg-emerald-100 rounded-full flex items-center justify-center mx-auto mb-4">
                <i class="fas fa-check text-emerald-600 text-3xl"></i>
            </div>
            <h3 class="font-black text-2xl text-slate-800 mb-2">Pledge Accepted!</h3>
            <p class="text-slate-600 mb-6">Thank you for acknowledging the IP Pledge. To generate your secure access links and return to this dashboard in the future, you must register a free account now.<br><br>Going forward, you will access this Control Centre via the standard AIT Sign In page.</p>
            <a href="{{ url_for('auth_bp.register', next=request.path) }}" class="block w-full py-3 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-lg shadow-md transition text-lg mb-1">
                Register My Account
            </a>
        </div>
    </div>
</div>
{% endif %}

{% endblock %}
'''

# Replace the LAST {% endblock %} with our modal + endblock
html = html.rsplit('{% endblock %}', 1)[0] + forced_modal

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(html)
