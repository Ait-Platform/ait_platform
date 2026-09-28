import re

filepath = 'templates/program_uip/register_import.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# Replace {% set kind = '...' %} with {% with current_form_kind = '...' %}
# And in the form, I'll use current_form_kind
content = content.replace("{% include 'partials/hub_upload_form.html' %}", "{% with current_form_kind = 'members' %}{% include 'partials/hub_upload_form.html' %}{% endwith %}", 1)
content = content.replace("{% set kind = 'properties' %}\n                            {% include 'partials/hub_upload_form.html' %}", "{% with current_form_kind = 'properties' %}{% include 'partials/hub_upload_form.html' %}{% endwith %}")
content = content.replace("{% set kind = 'relationships' %}\n                            {% include 'partials/hub_upload_form.html' %}", "{% with current_form_kind = 'relationships' %}{% include 'partials/hub_upload_form.html' %}{% endwith %}")

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)

form_html = '''<div class="bg-white p-5 rounded-lg border border-slate-200 shadow-sm">
    <h4 class="font-bold text-slate-900 mb-3 text-sm">Upload {{ current_form_kind|title }} CSV</h4>
    
    <form method="post" enctype="multipart/form-data" class="space-y-4">
        <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
        <input type="hidden" name="kind" value="{{ current_form_kind }}">
        
        {% if preview_token and kind == current_form_kind %}
            <input type="hidden" name="preview_token" value="{{ preview_token }}">
            
            <div class="bg-emerald-50 border border-emerald-200 rounded p-4">
                <p class="text-sm font-bold text-emerald-800 mb-2"><i class="fas fa-check-circle mr-1"></i> Preview Validated ({{ summary.received }} rows)</p>
                <div class="flex space-x-4 text-xs">
                    <span class="text-emerald-700"><strong>{{ summary.created }}</strong> new</span>
                    <span class="text-blue-700"><strong>{{ summary.updated }}</strong> updated</span>
                    <span class="text-slate-600"><strong>{{ summary.unchanged }}</strong> unchanged</span>
                    <span class="{% if summary.exceptions > 0 %}text-red-700 font-bold{% else %}text-emerald-700{% endif %}"><strong>{{ summary.exceptions }}</strong> exceptions</span>
                </div>
            </div>
            
            <div>
                <label class="block text-xs font-bold text-slate-700 mb-1">Confirm File</label>
                <input type="file" name="file" accept=".csv,text/csv" required class="block w-full text-xs text-slate-500 file:mr-4 file:py-1 file:px-3 file:rounded file:border-0 file:font-semibold file:bg-indigo-50 file:text-indigo-700 border border-dashed border-slate-300">
            </div>
            
            <div class="flex justify-end pt-2">
                <button name="operation" value="commit" class="px-4 py-1.5 bg-emerald-600 hover:bg-emerald-700 text-white font-bold text-xs rounded shadow flex items-center">
                    <i class="fas fa-check mr-1.5"></i> Confirm Import
                </button>
            </div>
        {% else %}
            <div>
                <input type="file" name="file" accept=".csv,text/csv" required class="block w-full text-xs text-slate-500 file:mr-4 file:py-1 file:px-3 file:rounded file:border-0 file:font-semibold file:bg-indigo-50 file:text-indigo-700 hover:file:bg-indigo-100 border border-dashed border-slate-300 p-3">
            </div>
            
            <div class="flex justify-end pt-2">
                <button name="operation" value="preview" class="px-4 py-1.5 bg-indigo-600 hover:bg-indigo-700 text-white font-bold text-xs rounded shadow flex items-center">
                    <i class="fas fa-search mr-1.5"></i> Preview
                </button>
            </div>
        {% endif %}
    </form>
</div>'''

with open('templates/partials/hub_upload_form.html', 'w', encoding='utf-8') as f:
    f.write(form_html)

