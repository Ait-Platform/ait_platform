html_content = '''{% extends "layout.html" %}
{% block title %}SACE Control Centre{% endblock %}
{% block flashes %}{% endblock %}

{% block page_wrap_classes %}
mx-auto max-w-6xl px-4 py-8
{% endblock %}

{% block content %}
<div class="mb-6 flex flex-col md:flex-row justify-between items-start md:items-end border-b border-slate-200 pb-4">
    <div>
        <h1 class="text-3xl font-black text-slate-900 flex items-center">
            <i class="fas fa-sliders-h text-indigo-600 mr-3"></i> SACE Control Centre
        </h1>
        <p class="text-slate-500 mt-1">Provider Activity Management & Endorsement</p>
    </div>
    <div class="mt-4 md:mt-0">
        <a href="{{ url_for('public_bp.welcome') }}" class="px-4 py-2 text-sm font-bold text-slate-600 bg-white border border-slate-300 rounded-lg hover:bg-slate-50 shadow-sm transition">
            <i class="fas fa-sign-out-alt mr-1"></i> Exit Platform
        </a>
    </div>
</div>

<div class="mb-6">
    {% include "partials/flash_messages.html" %}
</div>

<!-- Dashboard Grid -->
<div class="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-8">
    
    <!-- Card 1: IP Pledge -->
    <div class="bg-white rounded-xl shadow-sm border {% if not has_pledged %}border-red-300 bg-red-50{% else %}border-emerald-200{% endif %} p-6 flex flex-col">
        <div class="flex items-center justify-between mb-4">
            <h3 class="font-bold text-lg text-slate-800"><i class="fas fa-file-signature text-slate-400 mr-2"></i> IP Pledge</h3>
            {% if not has_pledged %}
                <span class="px-2 py-1 bg-red-100 text-red-700 text-xs font-bold rounded">Required</span>
            {% else %}
                <span class="px-2 py-1 bg-emerald-100 text-emerald-700 text-xs font-bold rounded"><i class="fas fa-check mr-1"></i> Signed</span>
            {% endif %}
        </div>
        <p class="text-sm text-slate-500 mb-6 flex-grow">Acknowledge the Intellectual Property confidentiality terms to unlock auditor provisioning.</p>
        <button onclick="openPledgeModal()" class="w-full py-2 rounded-lg font-bold transition {% if not has_pledged %}bg-indigo-600 hover:bg-indigo-700 text-white shadow-md{% else %}bg-slate-100 hover:bg-slate-200 text-slate-700 border border-slate-200{% endif %}">
            {% if not has_pledged %}Review & Sign Pledge{% else %}View Signed Pledge{% endif %}
        </button>
    </div>

    <!-- Card 2: App Documents -->
    <div class="bg-white rounded-xl shadow-sm border border-slate-200 p-6 flex flex-col">
        <div class="flex items-center mb-4">
            <h3 class="font-bold text-lg text-slate-800"><i class="fas fa-folder-open text-amber-500 mr-2"></i> Provider Submission</h3>
        </div>
        <p class="text-sm text-slate-500 mb-4">Official AIT application documents for SACE endorsement review.</p>
        <div class="space-y-2 flex-grow">
            <a href="{{ url_for('sace_bp.secure_view', doc_type='app_form_1') }}" target="_blank" class="flex items-center p-2 hover:bg-slate-50 rounded border border-transparent hover:border-slate-200 transition group">
                <i class="fas fa-file-pdf text-red-400 mr-3 group-hover:scale-110 transition-transform"></i>
                <span class="text-sm font-medium text-slate-700 group-hover:text-indigo-600">Application Form (Part 1)</span>
            </a>
            <a href="{{ url_for('sace_bp.secure_view', doc_type='app_form_2') }}" target="_blank" class="flex items-center p-2 hover:bg-slate-50 rounded border border-transparent hover:border-slate-200 transition group">
                <i class="fas fa-file-pdf text-red-400 mr-3 group-hover:scale-110 transition-transform"></i>
                <span class="text-sm font-medium text-slate-700 group-hover:text-indigo-600">Application Form (Part 2)</span>
            </a>
            <a href="{{ url_for('sace_bp.secure_view', doc_type='f_cv') }}" target="_blank" class="flex items-center p-2 hover:bg-slate-50 rounded border border-transparent hover:border-slate-200 transition group">
                <i class="fas fa-id-badge text-blue-400 mr-3 group-hover:scale-110 transition-transform"></i>
                <span class="text-sm font-medium text-slate-700 group-hover:text-indigo-600">Facilitator CVs</span>
            </a>
        </div>
    </div>

    <!-- Card 3: Audit Logs -->
    <div class="bg-white rounded-xl shadow-sm border border-slate-200 p-6 flex flex-col">
        <div class="flex items-center mb-4">
            <h3 class="font-bold text-lg text-slate-800"><i class="fas fa-history text-indigo-500 mr-2"></i> Platform Tracking</h3>
        </div>
        <p class="text-sm text-slate-500 mb-6 flex-grow">Monitor real-time interactions, logins, and evaluation completions by provisioned auditors.</p>
        <a href="{{ url_for('sace_bp.audit_report') }}" class="w-full block text-center py-2 bg-slate-800 hover:bg-slate-700 text-white rounded-lg font-bold shadow-md transition">
            View Live Audit Logs
        </a>
    </div>
</div>

<!-- Full Width Card: Auditor Provisioning -->
<div class="bg-white rounded-xl shadow-sm border border-slate-200 overflow-hidden relative min-h-[300px]">
    {% if not has_pledged %}
    <!-- Locked Overlay -->
    <div class="absolute inset-0 bg-slate-50/90 backdrop-blur-sm z-10 flex flex-col items-center justify-center p-6 text-center">
        <i class="fas fa-lock text-5xl text-slate-300 mb-4"></i>
        <h3 class="text-xl font-bold text-slate-700 mb-2">Provisioning Locked</h3>
        <p class="text-slate-500 max-w-md">You must review and accept the AIT Intellectual Property Pledge before you can generate secure access links for Auditors.</p>
        <button onclick="openPledgeModal()" class="mt-6 px-6 py-2 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-lg shadow transition">
            Sign Pledge Now
        </button>
    </div>
    {% endif %}

    <div class="p-6 border-b border-slate-100 bg-slate-50">
        <h3 class="text-lg font-bold text-slate-800"><i class="fas fa-users-cog text-indigo-500 mr-2"></i> Auditor Provisioning</h3>
        <p class="text-sm text-slate-500 mt-1">Generate secure, one-time access links for your designated evaluators.</p>
    </div>
    
    <div class="p-6">
        <!-- Add Auditor Form -->
        <form action="{{ url_for('sace_bp.provision_auditor') }}" method="POST" class="flex flex-col md:flex-row gap-4 mb-8 bg-indigo-50 p-4 rounded-lg border border-indigo-100">
            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
            <div class="flex-1">
                <input type="text" name="first_name" placeholder="First Name" required class="w-full p-3 bg-white border border-slate-300 rounded-lg focus:ring-2 focus:ring-indigo-500 transition shadow-sm">
            </div>
            <div class="flex-1">
                <input type="text" name="last_name" placeholder="Last Name" required class="w-full p-3 bg-white border border-slate-300 rounded-lg focus:ring-2 focus:ring-indigo-500 transition shadow-sm">
            </div>
            <div class="flex-1">
                <input type="email" name="email" placeholder="Email Address" required class="w-full p-3 bg-white border border-slate-300 rounded-lg focus:ring-2 focus:ring-indigo-500 transition shadow-sm">
            </div>
            <div class="flex items-center">
                <button type="submit" class="w-full md:w-auto px-6 py-3 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-lg shadow transition whitespace-nowrap">
                    <i class="fas fa-paper-plane mr-2"></i> Send Invite
                </button>
            </div>
        </form>

        <!-- Provisioned Auditors List -->
        <h4 class="font-bold text-slate-700 mb-4 border-b border-slate-100 pb-2">Active Auditor Access ({{ auditors|length }})</h4>
        
        {% if auditors %}
        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
            {% for aud in auditors %}
            <div class="p-4 border border-slate-200 rounded-lg hover:border-indigo-300 transition bg-white flex justify-between items-start group">
                <div class="flex items-start">
                    <div class="w-10 h-10 rounded-full bg-indigo-100 flex items-center justify-center text-indigo-700 font-bold mr-3 shrink-0">
                        {{ aud.first_name[0] }}{{ aud.last_name[0] }}
                    </div>
                    <div>
                        <div class="font-bold text-slate-800 text-sm">{{ aud.first_name }} {{ aud.last_name }}</div>
                        <div class="text-xs text-slate-500 mb-2">{{ aud.email }}</div>
                        <span class="px-2 py-1 rounded text-[10px] font-bold border uppercase tracking-wider {% if 'Completed' in aud.status %}bg-slate-100 text-slate-500 border-slate-200{% elif 'Progress' in aud.status %}bg-blue-50 text-blue-600 border-blue-200{% else %}bg-emerald-50 text-emerald-600 border-emerald-200{% endif %}">
                            {{ aud.status }}
                        </span>
                    </div>
                </div>
                <button onclick="openEditModal('{{ aud.id }}', '{{ aud.first_name }}', '{{ aud.last_name }}', '{{ aud.email }}')" class="text-slate-300 hover:text-indigo-600 transition p-2 bg-slate-50 rounded hover:bg-indigo-50 opacity-0 group-hover:opacity-100" title="Edit Auditor">
                    <i class="fas fa-edit"></i>
                </button>
            </div>
            {% endfor %}
        </div>
        {% else %}
        <div class="text-center py-8">
            <div class="w-16 h-16 bg-slate-50 rounded-full flex items-center justify-center mx-auto mb-3">
                <i class="fas fa-users-slash text-slate-300 text-2xl"></i>
            </div>
            <p class="text-slate-400 text-sm">No auditors have been provisioned yet.</p>
        </div>
        {% endif %}
    </div>
</div>

<!-- Pledge Modal -->
<div id="pledge-modal" class="fixed inset-0 z-[60] hidden bg-slate-900 bg-opacity-75 flex items-center justify-center p-4 backdrop-blur-sm">
    <div class="bg-white rounded-2xl shadow-2xl w-full max-w-2xl overflow-hidden border border-slate-200">
        <div class="bg-slate-900 p-6 flex justify-between items-center">
            <h3 class="font-black text-xl text-white tracking-wide"><i class="fas fa-lock text-emerald-400 mr-3"></i> Intellectual Property Pledge</h3>
            <button onclick="closePledgeModal()" class="text-slate-400 hover:text-white transition"><i class="fas fa-times text-xl"></i></button>
        </div>
        <div class="p-8">
            <div class="bg-slate-50 border border-slate-200 rounded-xl p-6 text-sm text-slate-700 leading-relaxed space-y-4 shadow-inner mb-6">
                <p>By accessing the AIT Provider Platform, you acknowledge and agree to the following terms on behalf of SACE and its appointed representatives:</p>
                <ul class="list-disc pl-5 space-y-2 font-medium">
                    <li>The <strong>LITRE Blending Machine</strong> methodology, physical apparatus designs, and digital simulators are the exclusive Intellectual Property of AIT.</li>
                    <li>All SACE-appointed individuals granted access via this portal are bound by strict confidentiality and non-disclosure obligations.</li>
                    <li>Secure access links are generated strictly for the purpose of activity evaluation and endorsement, and may not be shared, duplicated, or utilized for unauthorized training.</li>
                    <li>Any attempt to bypass digital rights management, download secure documents, or reproduce the methodology without written authorization is strictly prohibited.</li>
                </ul>
            </div>
            
            {% if not has_pledged %}
            <form action="{{ url_for('sace_bp.provisioning_pledge') }}" method="POST">
                <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                <div class="flex items-center justify-between mt-4">
                    <button type="button" onclick="closePledgeModal()" class="px-6 py-3 text-slate-500 hover:text-slate-700 font-bold transition">Cancel</button>
                    <button type="submit" class="px-8 py-3 bg-emerald-600 hover:bg-emerald-700 text-white text-lg font-black rounded-xl shadow-lg hover:shadow-xl transition flex items-center group">
                        <i class="fas fa-check-circle mr-3 group-hover:scale-110 transition-transform"></i> I Acknowledge & Accept
                    </button>
                </div>
            </form>
            {% else %}
            <div class="mt-4 p-4 bg-emerald-50 border border-emerald-200 rounded-lg text-emerald-800 flex items-center justify-center font-bold">
                <i class="fas fa-check-circle text-2xl mr-3"></i> You have successfully acknowledged these terms.
            </div>
            <div class="mt-6 flex justify-end">
                <button type="button" onclick="closePledgeModal()" class="px-6 py-2 bg-slate-200 hover:bg-slate-300 text-slate-700 font-bold rounded-lg transition">Close</button>
            </div>
            {% endif %}
        </div>
    </div>
</div>

<!-- Edit Auditor Modal -->
<div id="edit-modal" class="fixed inset-0 z-[60] hidden bg-slate-900 bg-opacity-75 flex items-center justify-center p-4 backdrop-blur-sm">
    <div class="bg-white rounded-xl shadow-2xl w-full max-w-lg overflow-hidden border border-slate-200">
        <div class="bg-indigo-600 p-5 flex justify-between items-center text-white">
            <h3 class="font-bold text-lg"><i class="fas fa-user-edit mr-2"></i> Edit Provisioned Auditor</h3>
            <button onclick="closeEditModal()" class="text-indigo-200 hover:text-white transition"><i class="fas fa-times text-xl"></i></button>
        </div>
        <form id="edit-form" method="POST" action="" class="p-6">
            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
            <div class="space-y-4">
                <div>
                    <label class="block text-xs font-bold text-slate-500 uppercase mb-1">First Name</label>
                    <input type="text" name="first_name" id="edit-first-name" required class="w-full p-3 bg-slate-50 border border-slate-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:bg-white transition">
                </div>
                <div>
                    <label class="block text-xs font-bold text-slate-500 uppercase mb-1">Last Name</label>
                    <input type="text" name="last_name" id="edit-last-name" required class="w-full p-3 bg-slate-50 border border-slate-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:bg-white transition">
                </div>
                <div>
                    <label class="block text-xs font-bold text-slate-500 uppercase mb-1">Email Address</label>
                    <input type="email" name="email" id="edit-email" required class="w-full p-3 bg-slate-50 border border-slate-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:bg-white transition">
                </div>
            </div>
            <div class="mt-8 flex justify-end space-x-3 border-t border-slate-100 pt-5">
                <button type="button" onclick="closeEditModal()" class="px-5 py-2 text-slate-500 hover:bg-slate-100 rounded-lg font-bold transition">Cancel</button>
                <button type="submit" class="px-6 py-2 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-lg shadow-md transition"><i class="fas fa-paper-plane mr-2"></i> Save & Resend</button>
            </div>
        </form>
    </div>
</div>

<script>
function openPledgeModal() {
    document.getElementById('pledge-modal').classList.remove('hidden');
}
function closePledgeModal() {
    document.getElementById('pledge-modal').classList.add('hidden');
}

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
{% endblock %}'''

with open('templates/program_sace/provisioning_map.html', 'w', encoding='utf-8') as f:
    f.write(html_content)
