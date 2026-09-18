import re
with open("templates/program_uip/dashboards/resolution_view.html", "r", encoding="utf-8") as f:
    text = f.read()

# I want to add a Publish button block to the right column, below the "Status & Quorum" block, if status == DRAFT.
# Let's find the closing endif of the Quorum Tracker block and insert it there.
# It ends with:
"""            <div class="bg-slate-50 border border-slate-200 rounded-xl shadow-sm p-6 text-center">
                <i class="fas fa-landmark text-4xl text-slate-300 mb-4 block"></i>
                <h3 class="font-bold text-slate-700 mb-1">Historical Record</h3>
                <p class="text-sm text-slate-500">This resolution is officially adopted and closed for voting.</p>
            </div>
            {% endif %}"""

old_quorum = """            <div class="bg-slate-50 border border-slate-200 rounded-xl shadow-sm p-6 text-center">
                <i class="fas fa-landmark text-4xl text-slate-300 mb-4 block"></i>
                <h3 class="font-bold text-slate-700 mb-1">Historical Record</h3>
                <p class="text-sm text-slate-500">This resolution is officially adopted and closed for voting.</p>
            </div>
            {% endif %}"""

new_quorum = """            {% elif resolution.status == 'DRAFT' %}
            <div class="bg-amber-50 border border-amber-200 rounded-xl shadow-sm p-6 text-center">
                <i class="fas fa-pen-nib text-4xl text-amber-300 mb-4 block"></i>
                <h3 class="font-bold text-amber-900 mb-1">Private Draft</h3>
                <p class="text-sm text-amber-700 mb-4">This resolution is invisible to the committee. When you are ready, publish it to the voting room.</p>
                <form method="POST" action="{{ url_for('uip_bp.publish_resolution', org_slug=org.slug, res_id=resolution.id) }}">
                    <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                    <button type="submit" class="w-full py-3 rounded-lg font-bold text-white bg-indigo-600 hover:bg-indigo-700 shadow-sm transition">
                        <i class="fas fa-paper-plane mr-2"></i> Publish to Committee
                    </button>
                </form>
            </div>
            {% else %}
            <div class="bg-slate-50 border border-slate-200 rounded-xl shadow-sm p-6 text-center">
                <i class="fas fa-landmark text-4xl text-slate-300 mb-4 block"></i>
                <h3 class="font-bold text-slate-700 mb-1">Historical Record</h3>
                <p class="text-sm text-slate-500">This resolution is officially adopted and closed for voting.</p>
            </div>
            {% endif %}"""

text = text.replace(old_quorum, new_quorum)

with open("templates/program_uip/dashboards/resolution_view.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated resolution_view.html with DRAFT state publish button")
