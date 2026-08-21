import re

with open('templates/program_mechanic/public_job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Add Letterhead at the very top of the content block
find_header = '''{% block content %}
<div class="min-h-screen bg-slate-50 py-10 px-4 sm:px-6 lg:px-8 print:bg-white print:py-0 print:px-0">
  <div class="max-w-4xl mx-auto">'''
  
replace_header = '''{% block content %}
<div class="min-h-screen bg-slate-50 py-10 px-4 sm:px-6 lg:px-8 print:bg-white print:py-0 print:px-0">
  <div class="max-w-4xl mx-auto">
  
    {% if shop and shop.use_custom_letterhead and shop.letterhead_url %}
    <div class="bg-white p-6 rounded-2xl shadow-sm border border-slate-200 mb-6 text-center print:shadow-none print:border-none print:mb-2 print:p-0">
      <img src="{{ url_for('static', filename='uploads/mechanic/' + shop.letterhead_url) }}" alt="Shop Letterhead" class="max-h-32 mx-auto rounded-lg">
    </div>
    {% endif %}'''
    
if find_header in content:
    content = content.replace(find_header, replace_header)
else:
    print("Failed to find header block!")

# 2. Remove actions from the top (keep only Print)
find_actions = '''<div class="flex flex-wrap items-center gap-3 print:hidden">
        <a href="{{ url_for('mechanic_bp.job_cards_list') }}" class="px-4 py-2 bg-white border border-slate-300 rounded-lg text-sm font-bold text-slate-700 hover:bg-slate-50 shadow-sm transition">
          &larr; Back
        </a>
        <button onclick="window.print()" class="px-4 py-2 bg-indigo-600 text-white rounded-lg text-sm font-bold hover:bg-indigo-700 shadow-sm transition flex items-center">
          <i class="fas fa-print mr-2"></i> Print / PDF
        </button>
        {% if job_card.status == 'Quote' %}
        <form method="POST" action="{{ url_for('mechanic_bp.approve_job', id=job_card.id) }}" class="inline">
          <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
          <button type="submit" class="px-4 py-2 bg-green-600 text-white rounded-lg text-sm font-bold hover:bg-green-700 shadow-sm transition flex items-center">
            <i class="fas fa-check mr-2"></i> Convert to Tax Invoice
          </button>
        </form>
        {% endif %}
        <a href="{{ url_for('mechanic_bp.email_document', id=job_card.id) }}" class="px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-bold hover:bg-blue-700 shadow-sm transition flex items-center">
          <i class="fas fa-envelope mr-2"></i> Email {{ 'Invoice' if job_card.status == 'Billed' else 'Quote' }}
        </a>
        {% if job_card.status == 'Approved' %}
        <a href="{{ url_for('mechanic_bp.capture_pop', id=job_card.id) }}" class="px-4 py-2 bg-emerald-600 text-white rounded-lg text-sm font-bold hover:bg-emerald-700 shadow-sm transition flex items-center">
          <i class="fas fa-money-bill-wave mr-2"></i> Capture POP
        </a>
        {% endif %}
      </div>'''

replace_actions = '''<div class="flex flex-wrap items-center gap-3 print:hidden">
        <button onclick="window.print()" class="px-4 py-2 bg-indigo-600 text-white rounded-lg text-sm font-bold hover:bg-indigo-700 shadow-sm transition flex items-center">
          <i class="fas fa-print mr-2"></i> Download / Print PDF
        </button>
      </div>'''

if find_actions in content:
    content = content.replace(find_actions, replace_actions)
else:
    print("Failed to find actions block!")

# 3. Remove edit buttons and modal
content = re.sub(r'<button type="button" onclick="document\.getElementById\(edit-client-modal\)\.classList\.remove\(hidden\)".*?</button>', '', content, flags=re.DOTALL)
content = re.sub(r'<button class="text-xs font-semibold.*?<i class="fas fa-edit mr-1"></i>Edit\s*</button>', '', content, flags=re.DOTALL)
content = re.sub(r'<!-- Edit Client Modal -->.*', '{% endblock %}', content, flags=re.DOTALL)

# 4. Remove onclick from phone number
content = re.sub(r'<a href="javascript:void\(0\)" onclick=".*?class="hover:text-indigo-600 transition" title="Call Client">', '<span class="text-slate-600">', content, flags=re.DOTALL)
content = content.replace('</a>\n          </p>', '</span>\n          </p>')

with open('templates/program_mechanic/public_job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Public job card template prepared.")
