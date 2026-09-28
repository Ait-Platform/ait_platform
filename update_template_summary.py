filepath = 'templates/program_uip/register_import.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

new_html = '''        <div class="p-8 max-w-2xl mx-auto">
            {% with current_form_kind = 'master_roll' %}{% include 'partials/hub_upload_form.html' %}{% endwith %}
        </div>
        
        {% if latest_batch %}
        <div class="border-t border-slate-200 bg-slate-50 p-6">
            <h3 class="text-md font-bold text-slate-800 mb-4">Last Upload Summary</h3>
            <div class="grid grid-cols-3 gap-4 mb-6">
                <div class="bg-white p-4 rounded border border-slate-200 shadow-sm text-center">
                    <p class="text-xs text-slate-500 font-bold uppercase tracking-wider mb-1">Status</p>
                    <p class="text-lg font-bold {% if latest_batch.status == 'COMPLETED' %}text-emerald-600{% else %}text-amber-600{% endif %}">{{ latest_batch.status }}</p>
                </div>
                <div class="bg-white p-4 rounded border border-slate-200 shadow-sm text-center">
                    <p class="text-xs text-slate-500 font-bold uppercase tracking-wider mb-1">Date</p>
                    <p class="text-lg font-bold text-slate-800">{{ latest_batch.created_at.strftime('%Y-%m-%d %H:%M') }}</p>
                </div>
                <div class="bg-white p-4 rounded border border-slate-200 shadow-sm text-center">
                    <p class="text-xs text-slate-500 font-bold uppercase tracking-wider mb-1">Exceptions</p>
                    <p class="text-lg font-bold {% if latest_exceptions|length > 0 %}text-red-600{% else %}text-emerald-600{% endif %}">{{ latest_exceptions|length }}</p>
                </div>
            </div>
            
            {% if latest_exceptions %}
            <div class="bg-white rounded border border-red-200 overflow-hidden shadow-sm">
                <div class="bg-red-50 p-3 border-b border-red-200">
                    <h4 class="text-sm font-bold text-red-800"><i class="fas fa-exclamation-triangle mr-2"></i> Failed Rows</h4>
                </div>
                <div class="overflow-x-auto max-h-64 overflow-y-auto">
                    <table class="w-full text-left text-sm whitespace-nowrap">
                        <thead class="bg-slate-50 border-b border-slate-200 text-slate-600">
                            <tr>
                                <th class="px-4 py-2 font-bold">Row</th>
                                <th class="px-4 py-2 font-bold">Reference</th>
                                <th class="px-4 py-2 font-bold">Reason</th>
                            </tr>
                        </thead>
                        <tbody class="divide-y divide-slate-100">
                            {% for exc in latest_exceptions %}
                            <tr class="hover:bg-slate-50">
                                <td class="px-4 py-2 text-slate-500">{{ exc.row_number }}</td>
                                <td class="px-4 py-2 font-mono text-xs text-slate-700">{{ exc.source_reference }}</td>
                                <td class="px-4 py-2 text-red-600">{{ exc.reason }}</td>
                            </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                </div>
            </div>
            {% endif %}
        </div>
        {% endif %}
        
        <div class="bg-slate-100 border-t border-slate-200 p-6 flex justify-end">'''

content = content.replace('''        <div class="p-8 max-w-2xl mx-auto">
            {% with current_form_kind = 'master_roll' %}{% include 'partials/hub_upload_form.html' %}{% endwith %}
        </div>
        
        <div class="bg-slate-50 border-t border-slate-200 p-6 flex justify-end">''', new_html)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Template updated with summary table")
