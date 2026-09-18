import re

with open("templates/program_uip/dashboards/resident.html", "r", encoding="utf-8") as f:
    text = f.read()

old_block = """    {% include "partials/flash_messages.html" %}

    <div class="grid md:grid-cols-3 gap-8">"""

new_block = """    {% include "partials/flash_messages.html" %}
    
    {% if public_votes %}
    <div class="mb-8">
        <h2 class="text-xl font-bold text-slate-900 mb-4 flex items-center"><i class="fas fa-bullhorn text-indigo-600 mr-2"></i> Active Community Votes</h2>
        <div class="space-y-4">
            {% for res in public_votes %}
            <a href="{{ url_for('uip_bp.view_resolution', org_slug=org.slug, res_id=res.id) }}" class="block bg-indigo-50 border border-indigo-200 rounded-xl p-5 hover:shadow-md hover:border-indigo-400 transition">
                <div class="flex justify-between items-start">
                    <div>
                        <h3 class="text-lg font-black text-indigo-900 mb-1">{{ res.title }}</h3>
                        <p class="text-sm text-indigo-700 line-clamp-2 max-w-2xl">{{ res.description }}</p>
                    </div>
                    <span class="bg-indigo-600 text-white font-bold text-xs uppercase tracking-widest px-3 py-1.5 rounded-full shadow-sm flex items-center">
                        <i class="fas fa-vote-yea mr-2"></i> Vote Now
                    </span>
                </div>
            </a>
            {% endfor %}
        </div>
    </div>
    {% endif %}

    <div class="grid md:grid-cols-3 gap-8">"""

text = text.replace(old_block, new_block)

with open("templates/program_uip/dashboards/resident.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated resident dashboard template")
