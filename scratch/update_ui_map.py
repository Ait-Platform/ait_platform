import re

html_path = 'templates/program_sace/provisioning_map.html'
with open(html_path, 'r', encoding='utf-8') as f:
    html = f.read()

# 1. Update title
html = html.replace('I Learn to Read Using the LITRE Method', 'I Learn to Read Using the LITRE')

# 2. Update secure view links
html = html.replace("url_for('sace_bp.secure_view', doc_type='app_form_1')", "url_for('sace_bp.secure_view', doc_type='app_form_1', return_to='control_centre')")
html = html.replace("url_for('sace_bp.secure_view', doc_type='app_form_2')", "url_for('sace_bp.secure_view', doc_type='app_form_2', return_to='control_centre')")
html = html.replace("url_for('sace_bp.secure_view', doc_type='f_cv')", "url_for('sace_bp.secure_view', doc_type='f_cv', return_to='control_centre')")

# 3. Remove the guest banner
banner_start = html.find('<!-- Guest Registration Banner -->')
if banner_start != -1:
    # Find the end of the if block
    banner_end = html.find('{% endif %}', banner_start) + len('{% endif %}')
    html = html[:banner_start - 30] + html[banner_end:]

# 4. Fix Pledge Modal overflow
old_modal_div = '<div class="bg-white rounded-2xl shadow-2xl w-full max-w-2xl overflow-hidden border border-slate-200">'
new_modal_div = '<div class="bg-white rounded-2xl shadow-2xl w-full max-w-2xl overflow-hidden border border-slate-200 max-h-[95vh] flex flex-col">'
html = html.replace(old_modal_div, new_modal_div)

# Also the modal body needs overflow-y-auto
old_modal_body = '<div class="p-8">'
new_modal_body = '<div class="p-8 overflow-y-auto">'
# Be careful to only replace the pledge modal body
html = html.replace(old_modal_body, new_modal_body, 1) # First occurrence is pledge modal

# 5. Add Registration Modal and Auto-Trigger
reg_modal = '''
<!-- Post-Pledge Registration Modal -->
<div id="reg-modal" class="fixed inset-0 z-[70] hidden bg-slate-900 bg-opacity-75 flex items-center justify-center p-4 backdrop-blur-sm">
    <div class="bg-white rounded-xl shadow-2xl w-full max-w-md overflow-hidden border border-slate-200 text-center relative">
        <div class="p-8">
            <div class="w-16 h-16 bg-emerald-100 rounded-full flex items-center justify-center mx-auto mb-4">
                <i class="fas fa-check text-emerald-600 text-3xl"></i>
            </div>
            <h3 class="font-black text-2xl text-slate-800 mb-2">Pledge Accepted!</h3>
            <p class="text-slate-600 mb-6">Thank you for acknowledging the IP Pledge. To save your progress and securely manage your auditors across devices, please register a free account now.</p>
            <a href="{{ url_for('auth_bp.register', next=request.path) }}" class="block w-full py-3 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-lg shadow-md transition text-lg mb-3">
                Register Account
            </a>
            <button onclick="document.getElementById('reg-modal').classList.add('hidden')" class="text-slate-400 hover:text-slate-600 font-bold text-sm transition">
                I'll do this later
            </button>
        </div>
    </div>
</div>

{% with messages = get_flashed_messages(category_filter=["sace_reg_prompt"]) %}
{% if messages %}
<script>
    document.addEventListener("DOMContentLoaded", function() {
        document.getElementById('reg-modal').classList.remove('hidden');
    });
</script>
{% endif %}
{% endwith %}
'''

html = html.replace('{% endblock %}', reg_modal + '\n{% endblock %}')

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(html)
