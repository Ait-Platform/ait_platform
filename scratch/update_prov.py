import re

file_path = 'templates/program_sace/provisioning_map.html'

new_content = '''{% extends "layout.html" %}
{% block title %}Sace Provisioning Map{% endblock %}
{% block flashes %}{% endblock %}

{% block page_wrap_classes %}
mx-auto max-w-5xl px-6 py-10
{% endblock %}

{% block content %}
<div class="bg-white rounded-xl shadow-lg overflow-hidden border border-gray-100 mb-12">
    <!-- Color Strip -->
    <div class="h-2 w-full bg-indigo-600"></div>
    
    <div class="p-6 sm:p-8">
        
        <!-- Row 1 (Header) -->
        <div class="flex flex-col sm:flex-row justify-between items-start sm:items-center mb-6 border-b border-gray-100 pb-4">
            <div>
                <h1 class="text-2xl font-bold text-gray-900 flex items-center">
                    <i class="fas fa-users-cog mr-3 text-indigo-600"></i>
                    SACE Provisioning Map
                </h1>
                <p class="text-slate-500 mt-1 font-medium text-sm">LITRE Blending Machine Administration</p>
            </div>
            <a href="{{ url_for('sace_bp.dashboard') }}" class="mt-4 sm:mt-0 px-4 py-2 text-sm font-medium text-gray-700 bg-gray-100 rounded-lg hover:bg-gray-200 transition">
                <i class="fas fa-arrow-left mr-1"></i> Back
            </a>
        </div>

        <!-- Flash Messages (Inside Tile) -->
        <div class="mb-6">
            {% include "partials/flash_messages.html" %}
        </div>

        {% if not has_pledged %}
        <!-- PRE-PLEDGE INTRO EXPLAINER -->
        <div class="bg-indigo-50 border border-indigo-100 rounded-xl p-6 mb-8 text-indigo-900 shadow-sm">
            <h2 class="text-xl font-bold mb-3">Welcome, SACE Administrator</h2>
            <p class="mb-3 text-sm leading-relaxed">
                Thank you for your guidance regarding the SACE endorsement process for the <strong>LITRE Method Reading Programme</strong>.
            </p>
            <p class="mb-3 text-sm leading-relaxed">
                Because our programme utilizes patented physical methodology and proprietary interactive software, we have set up this secure, dedicated portal for SACE to conduct its evaluation.
                Through this portal, you will be able to securely provision access for your designated SACE Auditors and track their progress.
            </p>
            <div class="bg-white p-4 rounded border border-indigo-100 mt-4">
                <p class="font-bold text-sm mb-2">How to use this portal:</p>
                <ol class="list-decimal pl-5 text-sm space-y-1">
                    <li>Acknowledge the brief Intellectual Property pledge below.</li>
                    <li>Enter the names and emails of your selected SACE Auditors.</li>
                    <li>The system will automatically email them secure, one-time access links.</li>
                </ol>
            </div>
        </div>

        <!-- PATENT PLEDGE GATE -->
        <div class="bg-white rounded-xl border-2 border-indigo-100 p-8 text-center max-w-2xl mx-auto">
            <i class="fas fa-lock text-5xl text-indigo-300 mb-6"></i>
            <h2 class="text-2xl font-bold text-slate-800 mb-4">Intellectual Property Pledge</h2>
            <div class="text-left bg-slate-50 p-6 rounded-lg border border-slate-200 mb-6 text-sm text-slate-600 leading-relaxed">
                <p class="mb-4">Before proceeding to the provisioning tools, you must acknowledge the following:</p>
                <ul class="list-disc pl-5 space-y-2 font-medium">
                    <li>The LITRE Blending Machine and its digital simulators are the exclusive Intellectual Property of AIT.</li>
                    <li>SACE-appointed individuals granted access via this portal are bound by confidentiality.</li>
                    <li>Access is provided strictly for the purpose of activity evaluation and endorsement.</li>
                </ul>
            </div>
            
            <form action="{{ url_for('sace_bp.provisioning_pledge') }}" method="POST">
                <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                <button type="submit" class="w-full py-4 bg-indigo-600 hover:bg-indigo-700 text-white text-lg font-black rounded-xl shadow-lg hover:shadow-xl transition flex items-center justify-center group">
                    <i class="fas fa-check-circle mr-3 group-hover:scale-110 transition-transform"></i> Accept and Proceed
                </button>
            </form>
        </div>
        
        {% else %}
        
        <!-- PROVISIONING DASHBOARD -->
        
        <!-- Explainer -->
        <div class="bg-indigo-50 border border-indigo-200 rounded-xl p-6 mb-8 shadow-sm flex items-start">
            <i class="fas fa-info-circle text-indigo-500 text-2xl mr-4 mt-1"></i>
            <div>
                <h3 class="text-lg font-bold text-indigo-900 mb-2">Auditor Provisioning</h3>
                <p class="text-indigo-800 text-sm leading-relaxed">
                    Auditors are the SACE-appointed individuals designated to evaluate this activity. 
                    By adding an Auditor below, you are granting them a secure, one-time-use access pass to the AIT Provider Platform. 
                    They will be able to review the linear presentation and complete the interactive rubric. 
                    Once an Auditor submits their final evaluation, their access is automatically and permanently revoked to protect AIT's intellectual property.
                </p>
            </div>
        </div>

        <!-- Add Auditor Form -->
        <div class="bg-white rounded-xl shadow-sm border border-slate-200 p-6 mb-8">
            <h3 class="text-lg font-bold text-slate-800 mb-4 border-b border-slate-100 pb-2">Add New Auditor</h3>
            <form action="{{ url_for('sace_bp.provision_auditor') }}" method="POST" class="flex flex-col sm:flex-row gap-4">
                <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                <div class="flex-1">
                    <label class="block text-xs font-bold text-slate-500 uppercase mb-1">First Name</label>
                    <input type="text" name="first_name" required autofocus class="w-full p-3 bg-white border border-slate-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 transition shadow-sm">
                </div>
                <div class="flex-1">
                    <label class="block text-xs font-bold text-slate-500 uppercase mb-1">Last Name</label>
                    <input type="text" name="last_name" required class="w-full p-3 bg-white border border-slate-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 transition shadow-sm">
                </div>
                <div class="flex-1">
                    <label class="block text-xs font-bold text-slate-500 uppercase mb-1">Email Address</label>
                    <input type="email" name="email" required class="w-full p-3 bg-white border border-slate-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 transition shadow-sm">
                </div>
                <div class="flex items-end">
                    <button type="submit" class="w-full sm:w-auto px-6 py-3 bg-slate-800 hover:bg-slate-700 text-white font-bold rounded-lg shadow transition flex items-center justify-center h-[50px]">
                        <i class="fas fa-plus mr-2"></i> Provision
                    </button>
                </div>
            </form>
        </div>

        <!-- Provisioned Auditors List -->
        <div class="bg-white rounded-xl shadow-sm border border-slate-200 p-0 mb-8 overflow-hidden">
            <div class="p-6 border-b border-slate-100 bg-slate-50 flex justify-between items-center">
                <h3 class="text-lg font-bold text-slate-800">Provisioned Individuals</h3>
                <span class="px-3 py-1 bg-indigo-100 text-indigo-700 text-xs font-bold rounded-full">{{ auditors|length }} Active</span>
            </div>
            
            {% if auditors %}
            <div class="divide-y divide-slate-100">
                {% for aud in auditors %}
                <div class="p-4 sm:p-6 flex flex-col sm:flex-row sm:items-center justify-between hover:bg-slate-50 transition">
                    <div class="flex items-center mb-4 sm:mb-0">
                        <div class="w-10 h-10 rounded-full bg-indigo-100 flex items-center justify-center text-indigo-700 font-bold mr-4 shrink-0">
                            {{ aud.first_name[0] }}{{ aud.last_name[0] }}
                        </div>
                        <div>
                            <div class="font-bold text-slate-800">{{ aud.first_name }} {{ aud.last_name }}</div>
                            <div class="text-sm text-slate-500">{{ aud.email }}</div>
                        </div>
                    </div>
                    <div class="flex items-center space-x-4">
                        <span class="px-3 py-1 rounded-full text-xs font-bold border {% if aud.status == 'Completed & Locked Out' %}bg-slate-100 text-slate-500 border-slate-200{% elif aud.status == 'In Progress' %}bg-blue-50 text-blue-600 border-blue-200{% else %}bg-green-50 text-green-600 border-green-200{% endif %}">
                            {{ aud.status }}
                        </span>
                        <div class="text-xs text-slate-400 whitespace-nowrap">
                            <i class="far fa-clock mr-1"></i> {{ aud.date }}
                        </div>
                    </div>
                </div>
                {% endfor %}
            </div>
            {% else %}
            <div class="p-12 text-center">
                <i class="fas fa-user-shield text-4xl text-slate-300 mb-4"></i>
                <h4 class="text-lg font-bold text-slate-600 mb-1">No Auditors Provisioned</h4>
                <p class="text-sm text-slate-400">Add a SACE-appointed individual above to grant them access.</p>
            </div>
            {% endif %}
        </div>

        <!-- Audit Logs Quick Link -->
        <div class="mt-8 text-center">
            <a href="{{ url_for('sace_bp.audit_report') }}" class="inline-flex items-center px-6 py-3 bg-white border border-slate-200 hover:border-indigo-300 text-slate-600 hover:text-indigo-700 font-bold rounded-lg shadow-sm transition">
                <i class="fas fa-history mr-2"></i> View Platform Audit Logs
            </a>
        </div>
        
        {% endif %}

    </div>
</div>
{% endblock %}
'''

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(new_content)
