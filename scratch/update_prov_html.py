import re

html_path = 'templates/program_sace/provisioning_map.html'
with open(html_path, 'r', encoding='utf-8') as f:
    html = f.read()

# Change the back button from pointing to dashboard to pointing to the Welcome Page (public_bp.welcome)
header_old = '''<a href="{{ url_for('sace_bp.dashboard') }}" class="px-4 py-2 text-sm font-medium text-gray-700 bg-gray-100 rounded-lg hover:bg-gray-200 transition text-right">
                    <i class="fas fa-arrow-left mr-1"></i> Back
                </a>'''
header_new = '''<a href="{{ url_for('public_bp.welcome') }}" class="px-4 py-2 text-sm font-medium text-gray-700 bg-gray-100 rounded-lg hover:bg-gray-200 transition text-right">
                    <i class="fas fa-sign-out-alt mr-1"></i> Exit Control Centre
                </a>'''
html = html.replace(header_old, header_new)


# Inject edit button
auditor_row_old = '''<div class="flex items-center space-x-4">
                        <span class="px-3 py-1 rounded-full text-xs font-bold border {% if aud.status == 'Completed & Locked Out' %}bg-slate-100 text-slate-500 border-slate-200{% elif aud.status == 'In Progress' %}bg-blue-50 text-blue-600 border-blue-200{% else %}bg-green-50 text-green-600 border-green-200{% endif %}">
                            {{ aud.status }}
                        </span>
                        <div class="text-xs text-slate-400 whitespace-nowrap">
                            <i class="far fa-clock mr-1"></i> {{ aud.date }}
                        </div>
                    </div>'''
auditor_row_new = '''<div class="flex items-center space-x-4">
                        <span class="px-3 py-1 rounded-full text-xs font-bold border {% if 'Completed' in aud.status %}bg-slate-100 text-slate-500 border-slate-200{% elif 'Progress' in aud.status %}bg-blue-50 text-blue-600 border-blue-200{% else %}bg-green-50 text-green-600 border-green-200{% endif %}">
                            {{ aud.status }}
                        </span>
                        <div class="text-xs text-slate-400 whitespace-nowrap">
                            <i class="far fa-clock mr-1"></i> {{ aud.date }}
                        </div>
                        <button onclick="openEditModal('{{ aud.id }}', '{{ aud.first_name }}', '{{ aud.last_name }}', '{{ aud.email }}')" class="text-indigo-400 hover:text-indigo-600 transition ml-2" title="Edit Auditor">
                            <i class="fas fa-edit"></i>
                        </button>
                    </div>'''
html = html.replace(auditor_row_old, auditor_row_new)


# Inject Modal HTML and JS at the end of content block
modal_html = '''
<!-- Edit Modal -->
<div id="edit-modal" class="fixed inset-0 z-50 hidden bg-slate-900 bg-opacity-50 flex items-center justify-center p-4">
    <div class="bg-white rounded-xl shadow-xl w-full max-w-lg overflow-hidden">
        <div class="bg-indigo-600 p-4 flex justify-between items-center text-white">
            <h3 class="font-bold text-lg">Edit Provisioned Auditor</h3>
            <button onclick="closeEditModal()" class="text-indigo-200 hover:text-white transition"><i class="fas fa-times"></i></button>
        </div>
        <form id="edit-form" method="POST" action="" class="p-6">
            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
            <div class="space-y-4">
                <div>
                    <label class="block text-xs font-bold text-slate-500 uppercase mb-1">First Name</label>
                    <input type="text" name="first_name" id="edit-first-name" required class="w-full p-3 bg-white border border-slate-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 transition shadow-sm">
                </div>
                <div>
                    <label class="block text-xs font-bold text-slate-500 uppercase mb-1">Last Name</label>
                    <input type="text" name="last_name" id="edit-last-name" required class="w-full p-3 bg-white border border-slate-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 transition shadow-sm">
                </div>
                <div>
                    <label class="block text-xs font-bold text-slate-500 uppercase mb-1">Email Address</label>
                    <input type="email" name="email" id="edit-email" required class="w-full p-3 bg-white border border-slate-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 transition shadow-sm">
                </div>
            </div>
            <div class="mt-8 flex justify-end space-x-3 border-t border-slate-100 pt-4">
                <button type="button" onclick="closeEditModal()" class="px-4 py-2 text-slate-500 hover:bg-slate-100 rounded-lg font-medium transition">Cancel</button>
                <button type="submit" class="px-6 py-2 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-lg shadow transition">Save & Resend Invite</button>
            </div>
        </form>
    </div>
</div>

<script>
function openEditModal(id, firstName, lastName, email) {
    document.getElementById('edit-form').action = "/sace/provisioning/edit_auditor/" + id;
    document.getElementById('edit-first-name').value = firstName;
    document.getElementById('edit-last-name').value = lastName;
    document.getElementById('edit-email').value = email;
    document.getElementById('edit-modal').classList.remove('hidden');
}
function closeEditModal() {
    document.getElementById('edit-modal').classList.add('hidden');
}
</script>
'''

html = html.replace('{% endif %}\n\n    </div>\n</div>\n{% endblock %}', '{% endif %}\n\n    </div>\n</div>\n' + modal_html + '\n{% endblock %}')

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(html)
