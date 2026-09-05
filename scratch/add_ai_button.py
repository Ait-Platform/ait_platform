import re

with open('templates/uip/interactions/view.html', 'r', encoding='utf-8') as f:
    text = f.read()

btn_html = """
        {% if interaction.status != 'RESOLVED' and current_role in ['manager', 'receptionist'] %}
        <div class="flex space-x-2">
            <form method="POST" action="{{ url_for('uip_bp.summarize_interaction', org_slug=org.slug, reference=interaction.reference) }}">
                <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                <button type="submit" class="bg-indigo-100 hover:bg-indigo-200 text-indigo-700 font-bold py-2 px-4 rounded-lg shadow-sm transition flex items-center">
                    <i class="fas fa-magic mr-2"></i> Luna Summary
                </button>
            </form>
            <form method="POST" action="{{ url_for('uip_bp.resolve_interaction', org_slug=org.slug, reference=interaction.reference) }}" onsubmit="return confirm('Are you sure you want to mark this interaction as resolved?');">
                <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                <button type="submit" class="bg-emerald-600 hover:bg-emerald-700 text-white font-bold py-2 px-6 rounded-lg shadow transition flex items-center">
                    <i class="fas fa-check-double mr-2"></i> Mark Resolved
                </button>
            </form>
        </div>
        {% endif %}
"""

# Replace the old single resolve button
old_btn = """        {% if interaction.status != 'RESOLVED' and current_role in ['manager', 'receptionist'] %}
        <form method="POST" action="{{ url_for('uip_bp.resolve_interaction', org_slug=org.slug, reference=interaction.reference) }}" onsubmit="return confirm('Are you sure you want to mark this interaction as resolved?');">
            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
            <button type="submit" class="bg-emerald-600 hover:bg-emerald-700 text-white font-bold py-2 px-6 rounded-lg shadow transition flex items-center">
                <i class="fas fa-check-double mr-2"></i> Mark Resolved
            </button>
        </form>
        {% endif %}"""

text = text.replace(old_btn, btn_html)

with open('templates/uip/interactions/view.html', 'w', encoding='utf-8') as f:
    f.write(text.replace('\ufeff', ''))
