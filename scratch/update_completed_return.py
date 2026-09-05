import re

file_path = 'templates/subject_reading/completed_return.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

new_content = '''{% extends "layout.html" %}
{% block title %}Course Completed{% endblock %}

{% block content %}
<div class="max-w-xl mx-auto mt-12 mb-20">
  <div class="bg-white border rounded-2xl shadow-sm p-8 text-center">
    <div class="w-16 h-16 bg-emerald-100 text-emerald-600 rounded-full flex items-center justify-center mx-auto mb-4">
      <svg class="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"></path>
      </svg>
    </div>
    
    <h2 class="text-2xl font-bold text-slate-800 mb-3">Course Completed</h2>
    
    <p class="text-slate-600 mb-6 leading-relaxed">
      You have successfully completed the Reading Programme. 
      {% if enr and enr.completed_at %}
      (Completed on {{ enr.completed_at.strftime('%d %B %Y') }})
      {% endif %}
    </p>
    
    <div class="mb-8 flex flex-col gap-3">
        <a href="{{ url_for('reading_bp.get_certificate') }}" class="w-full inline-flex items-center justify-center bg-teal-600 hover:bg-teal-700 text-white font-medium px-4 py-3 rounded-lg transition-colors shadow-sm">
            <i class="fas fa-envelope mr-2"></i> Email My Certificate
        </a>
    </div>

    {% set is_sace = namespace(value=false) %}
    {% for s in session.get('admin_subjects', []) %}
      {% if s.startswith('sace') %}{% set is_sace.value = true %}{% endif %}
    {% endfor %}
    
    {% if not is_sace.value %}
    <div class="bg-slate-50 border border-slate-200 rounded-xl p-5 mb-8">
      <p class="text-sm text-slate-700 font-medium mb-4">Would you like to re-enroll to take the course again?</p>
      
      <form method="POST" action="{{ url_for('reading_bp.reenroll') }}">
        <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
        <button type="submit" class="w-full bg-indigo-600 hover:bg-indigo-700 text-white font-medium px-4 py-2.5 rounded-lg transition-colors">
          Yes, Re-Enroll
        </button>
      </form>
    </div>
    {% endif %}

    {% if is_sace.value %}
    <a href="{{ url_for('sace_bp.reading_hub') }}" class="text-indigo-600 hover:text-indigo-700 text-sm font-bold flex justify-center items-center">
      &larr; Return to SACE Hub
    </a>
    {% else %}
    <a href="{{ url_for('auth_bp.bridge_dashboard') }}" class="text-indigo-600 hover:text-indigo-700 text-sm font-medium">
      &larr; No, take me back to dashboard
    </a>
    {% endif %}
  </div>
</div>
{% endblock %}'''

with open(file_path, 'w', encoding='utf-8') as f: f.write(new_content)
