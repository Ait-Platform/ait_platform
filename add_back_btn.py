import re

filepath = 'templates/program_uip/register_import.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

new_ui = '''
        <div class="p-8 max-w-2xl mx-auto">
            {% with current_form_kind = 'master_roll' %}{% include 'partials/hub_upload_form.html' %}{% endwith %}
        </div>
        
        <div class="bg-slate-50 border-t border-slate-200 p-6 flex justify-end">
            <a href="{{ url_for('uip_bp.mo_dashboard' if request.path.endswith('/mo-vault/import') else 'uip_bp.secretary_workspace', org_slug=org.slug) }}" class="px-6 py-2 bg-slate-200 hover:bg-slate-300 text-slate-800 font-bold rounded-lg shadow-sm transition">
                <i class="fas fa-arrow-left mr-2"></i> Return to Dashboard
            </a>
        </div>
    </div>
</div>
{% endblock %}
'''

content = content.replace('''
        <div class="p-8 max-w-2xl mx-auto">
            {% with current_form_kind = 'master_roll' %}{% include 'partials/hub_upload_form.html' %}{% endwith %}
        </div>
    </div>
</div>
{% endblock %}''', new_ui)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Template updated with back button!")
