import re
with open("templates/program_uip/dashboards/resolution_draft.html", "r", encoding="utf-8") as f:
    text = f.read()

# Update Title
text = text.replace('name="title" list="title-suggestions" required class="', 'name="title" list="title-suggestions" required value="{{ resolution.title if resolution else \'\' }}" class="')
# Update Description
text = text.replace('placeholder="Write the formal proposal here..."></textarea>', 'placeholder="Write the formal proposal here...">{{ resolution.description if resolution else \'\' }}</textarea>')

# Update Scope and Quorum
old_scope = """                <select name="voting_scope" class="w-full p-3 border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500">
                    <option value="EXCO">Executive Committee Only (EXCO)</option>
                    <option value="PUBLIC">Public / Ratepayers</option>
                </select>"""
new_scope = """                <select name="voting_scope" class="w-full p-3 border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500">
                    <option value="EXCO" {% if resolution and resolution.voting_scope == 'EXCO' %}selected{% endif %}>Executive Committee Only (EXCO)</option>
                    <option value="PUBLIC" {% if resolution and resolution.voting_scope == 'PUBLIC' %}selected{% endif %}>Public / Ratepayers</option>
                </select>"""
text = text.replace(old_scope, new_scope)

text = text.replace('name="quorum_target" min="1" max="100" value="50"', 'name="quorum_target" min="1" max="100" value="{{ resolution.quorum_target if resolution else 50 }}"')

# Change H1 based on context
old_h1 = """            <h1 class="text-3xl font-extrabold text-slate-900 flex items-center">
                <i class="fas fa-pen-nib text-indigo-700 mr-3"></i> Draft New Resolution
            </h1>"""
new_h1 = """            <h1 class="text-3xl font-extrabold text-slate-900 flex items-center">
                <i class="fas fa-pen-nib text-indigo-700 mr-3"></i> {% if resolution %}Edit Draft{% else %}Draft New Resolution{% endif %}
            </h1>"""
text = text.replace(old_h1, new_h1)

# Add a Publish button directly to the editor if editing
old_btn = """        <div class="pt-6 border-t border-slate-100 flex justify-end">
            <button type="submit" class="ui-btn ui-btn-primary font-bold px-8 py-3 rounded-lg shadow-sm">
                <i class="fas fa-save mr-2"></i> Save to Drafts
            </button>
        </div>"""
new_btn = """        <div class="pt-6 border-t border-slate-100 flex justify-between items-center">
            {% if resolution %}
            <a href="{{ url_for('uip_bp.publish_resolution', org_slug=org.slug, res_id=resolution.id) }}" onclick="event.preventDefault(); document.getElementById('publish-form-{{ resolution.id }}').submit();" class="text-emerald-700 font-bold hover:text-emerald-900 bg-emerald-50 px-6 py-3 rounded-lg border border-emerald-200 transition shadow-sm">
                <i class="fas fa-paper-plane mr-2"></i> Publish to Voting Room
            </a>
            {% else %}
            <div></div>
            {% endif %}
            
            <button type="submit" class="ui-btn ui-btn-primary font-bold px-8 py-3 rounded-lg shadow-sm">
                <i class="fas fa-save mr-2"></i> Save Draft
            </button>
        </div>"""
text = text.replace(old_btn, new_btn)

# Add the hidden publish form at the bottom
new_form = """
{% if resolution %}
<form id="publish-form-{{ resolution.id }}" method="POST" action="{{ url_for('uip_bp.publish_resolution', org_slug=org.slug, res_id=resolution.id) }}" class="hidden">
    <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
</form>
{% endif %}
</div>
{% endblock %}"""
text = text.replace("</div>\n{% endblock %}", new_form)

with open("templates/program_uip/dashboards/resolution_draft.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated resolution_draft.html to handle editing")
