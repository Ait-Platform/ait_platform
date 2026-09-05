content = '''{% extends "layout.html" %}
{% block title %}SACE Activity Evaluation Hub{% endblock %}

{% block content %}
<div class="bg-slate-50 min-h-screen py-10 px-4">
    <div class="max-w-4xl mx-auto">
        
        <!-- Header -->
        <div class="bg-indigo-900 rounded-t-2xl p-8 text-white shadow-lg relative overflow-hidden">
            <div class="absolute top-0 right-0 p-4 opacity-10">
                <i class="fas fa-tasks text-9xl"></i>
            </div>
            <div class="relative z-10 flex justify-between items-center">
                <div>
                    <h1 class="text-3xl font-black mb-2">SACE Activity Evaluation Hub</h1>
                    <p class="text-indigo-200 text-lg">LITRE Reading Programme Validation Flow</p>
                </div>
                <a href="{{ url_for('sace_bp.dashboard') }}" class="px-5 py-2 bg-indigo-800 hover:bg-indigo-700 rounded-lg text-white font-semibold transition border border-indigo-700">
                    Exit
                </a>
            </div>
        </div>

        <!-- Roadmap Container -->
        <div class="bg-white p-8 rounded-b-2xl shadow-xl border-x border-b border-slate-200">
            
            {% set total = 7 %}
            {% set completed = 0 %}
            {% if progress.reviewer_guide %}{% set completed = completed + 1 %}{% endif %}
            {% if progress.app_form %}{% set completed = completed + 1 %}{% endif %}
            {% if progress.patent %}{% set completed = completed + 1 %}{% endif %}
            {% if progress.annexures %}{% set completed = completed + 1 %}{% endif %}
            {% if progress.ppp %}{% set completed = completed + 1 %}{% endif %}
            {% if progress.demo_cert %}{% set completed = completed + 1 %}{% endif %}
            {% if progress.reading_cert %}{% set completed = completed + 1 %}{% endif %}
            {% set percent = (completed / total * 100) | round | int %}

            <!-- Progress Bar -->
            <div class="mb-10 bg-slate-50 p-6 rounded-xl border border-slate-100">
                <div class="flex justify-between items-end mb-2">
                    <span class="font-bold text-slate-700">Overall Progress</span>
                    <span class="font-black text-indigo-600 text-2xl">{{ percent }}%</span>
                </div>
                <div class="w-full bg-slate-200 rounded-full h-4">
                    <div class="bg-indigo-500 h-4 rounded-full transition-all duration-1000" style="width: {{ percent }}%"></div>
                </div>
                {% if percent == 100 %}
                <div class="mt-4 p-3 bg-green-100 text-green-800 rounded-lg border border-green-200 font-bold text-center">
                    <i class="fas fa-check-circle mr-2"></i> Evaluation 100% Complete. You may exit the hub.
                </div>
                {% endif %}
                
                {% with messages = get_flashed_messages(with_categories=true) %}
                  {% if messages %}
                    <div class="mt-4">
                    {% for category, message in messages %}
                      <div class="p-3 rounded bg-red-100 text-red-700 border border-red-200 text-sm font-bold">
                        {{ message }}
                      </div>
                    {% endfor %}
                    </div>
                  {% endif %}
                {% endwith %}
            </div>

            <!-- Vertical List Layout -->
            <div class="space-y-4">
                
                <!-- Reviewer Guide -->
                <div class="flex items-center justify-between p-4 bg-white border border-slate-200 rounded-lg hover:border-indigo-300 transition shadow-sm">
                    <div class="flex items-center w-1/3">
                        <i class="fas fa-book text-indigo-400 mr-3 text-xl"></i>
                        <span class="font-bold text-slate-700">Reviewer Guide</span>
                    </div>
                    <div class="w-1/3 text-center">
                        <a href="{{ url_for('sace_bp.secure_view', doc_type='reviewer_guide') }}" class="px-4 py-2 bg-slate-100 hover:bg-slate-200 text-slate-700 text-sm font-bold rounded-md transition">View Document</a>
                    </div>
                    <div class="w-1/3 flex justify-end">
                        {% if progress.reviewer_guide %}<i class="fas fa-check-circle text-green-500 text-2xl"></i>{% else %}<span class="w-6 h-6 rounded-full border-2 border-slate-300"></span>{% endif %}
                    </div>
                </div>

                <!-- Application Form -->
                <div class="flex items-center justify-between p-4 bg-white border border-slate-200 rounded-lg hover:border-indigo-300 transition shadow-sm">
                    <div class="flex items-center w-1/3">
                        <i class="fas fa-file-contract text-indigo-400 mr-3 text-xl"></i>
                        <span class="font-bold text-slate-700">Application Form</span>
                    </div>
                    <div class="w-1/3 text-center">
                        <a href="{{ url_for('sace_bp.secure_view', doc_type='app_form') }}" class="px-4 py-2 bg-slate-100 hover:bg-slate-200 text-slate-700 text-sm font-bold rounded-md transition">View Document</a>
                    </div>
                    <div class="w-1/3 flex justify-end">
                        {% if progress.app_form %}<i class="fas fa-check-circle text-green-500 text-2xl"></i>{% else %}<span class="w-6 h-6 rounded-full border-2 border-slate-300"></span>{% endif %}
                    </div>
                </div>

                <!-- Patent Docs -->
                <div class="flex items-center justify-between p-4 bg-white border border-slate-200 rounded-lg hover:border-indigo-300 transition shadow-sm">
                    <div class="flex items-center w-1/3">
                        <i class="fas fa-certificate text-indigo-400 mr-3 text-xl"></i>
                        <span class="font-bold text-slate-700">Patent Docs</span>
                    </div>
                    <div class="w-1/3 text-center">
                        <a href="{{ url_for('sace_bp.secure_view', doc_type='patent') }}" class="px-4 py-2 bg-slate-100 hover:bg-slate-200 text-slate-700 text-sm font-bold rounded-md transition">View Document</a>
                    </div>
                    <div class="w-1/3 flex justify-end">
                        {% if progress.patent %}<i class="fas fa-check-circle text-green-500 text-2xl"></i>{% else %}<span class="w-6 h-6 rounded-full border-2 border-slate-300"></span>{% endif %}
                    </div>
                </div>

                <!-- Annexures -->
                <div class="flex items-center justify-between p-4 bg-white border border-slate-200 rounded-lg hover:border-indigo-300 transition shadow-sm">
                    <div class="flex items-center w-1/3">
                        <i class="fas fa-folder-open text-indigo-400 mr-3 text-xl"></i>
                        <span class="font-bold text-slate-700">Annexures A-E</span>
                    </div>
                    <div class="w-1/3 text-center">
                        <a href="{{ url_for('sace_bp.secure_view', doc_type='annexures') }}" class="px-4 py-2 bg-slate-100 hover:bg-slate-200 text-slate-700 text-sm font-bold rounded-md transition">View Document</a>
                    </div>
                    <div class="w-1/3 flex justify-end">
                        {% if progress.annexures %}<i class="fas fa-check-circle text-green-500 text-2xl"></i>{% else %}<span class="w-6 h-6 rounded-full border-2 border-slate-300"></span>{% endif %}
                    </div>
                </div>

                <!-- PPP -->
                <div class="flex items-center justify-between p-4 bg-white border border-slate-200 rounded-lg hover:border-indigo-300 transition shadow-sm">
                    <div class="flex items-center w-1/3">
                        <i class="fas fa-desktop text-indigo-400 mr-3 text-xl"></i>
                        <span class="font-bold text-slate-700">Linear Presentation</span>
                    </div>
                    <div class="w-1/3 text-center">
                        <a href="{{ url_for('sace_bp.presentation') }}" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white text-sm font-bold rounded-md transition shadow">View Slides</a>
                    </div>
                    <div class="w-1/3 flex justify-end">
                        {% if progress.ppp %}<i class="fas fa-check-circle text-green-500 text-2xl"></i>{% else %}<span class="w-6 h-6 rounded-full border-2 border-slate-300"></span>{% endif %}
                    </div>
                </div>

                <!-- Simulator -->
                <div class="flex items-center justify-between p-4 bg-white border border-slate-200 rounded-lg hover:border-indigo-300 transition shadow-sm">
                    <div class="flex items-center w-1/3">
                        <i class="fas fa-mobile-alt text-indigo-400 mr-3 text-xl"></i>
                        <span class="font-bold text-slate-700">Interactive Demo</span>
                    </div>
                    <div class="w-1/3 text-center">
                        <a href="{{ url_for('sace_bp.simulator') }}" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white text-sm font-bold rounded-md transition shadow">Launch Simulator</a>
                    </div>
                    <div class="w-1/3 flex justify-end">
                        {% if progress.demo_cert %}<i class="fas fa-check-circle text-green-500 text-2xl"></i>{% else %}<span class="w-6 h-6 rounded-full border-2 border-slate-300"></span>{% endif %}
                    </div>
                </div>

                <!-- Reading Course -->
                <div class="flex items-center justify-between p-4 bg-white border border-slate-200 rounded-lg hover:border-indigo-300 transition shadow-sm">
                    <div class="flex items-center w-1/3">
                        <i class="fas fa-book-reader text-indigo-400 mr-3 text-xl"></i>
                        <span class="font-bold text-slate-700">Reading Video Course</span>
                    </div>
                    <div class="w-1/3 text-center">
                        <a href="{{ url_for('reading_bp.subject_home') }}" class="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white text-sm font-bold rounded-md transition shadow">Start Course</a>
                    </div>
                    <div class="w-1/3 flex justify-end">
                        {% if progress.reading_cert %}<i class="fas fa-check-circle text-green-500 text-2xl"></i>{% else %}<span class="w-6 h-6 rounded-full border-2 border-slate-300"></span>{% endif %}
                    </div>
                </div>

            </div>
        </div>
    </div>
</div>
{% endblock %}'''

with open('templates/program_sace/reading_hub.html', 'w', encoding='utf-8') as f:
    f.write(content)
