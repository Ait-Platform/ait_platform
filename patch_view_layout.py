import re
with open("templates/program_uip/dashboards/resolution_view.html", "r", encoding="utf-8") as f:
    text = f.read()

# 1. Update Title placement
old_header = """    <div class="mb-6 flex justify-between items-center">
        <div>
            <a href="{{ url_for('uip_bp.dashboard', org_slug=org.slug) }}" class="text-indigo-600 font-bold text-sm mb-2 inline-block">&larr; Back to Dashboard</a>
            <div class="text-xs font-black text-indigo-600 mb-1 tracking-widest uppercase">{{ resolution.reference }}</div>
            <h1 class="text-3xl font-extrabold text-slate-900 flex items-center">
                <i class="fas fa-file-signature text-indigo-700 mr-3"></i> {{ resolution.title }}
            </h1>
        </div>"""

new_header = """    <div class="mb-8 flex justify-between items-start">
        <div>
            <div class="text-xs font-black text-indigo-600 mb-1 tracking-widest uppercase">{{ resolution.reference }}</div>
            <h1 class="text-3xl font-extrabold text-slate-900 flex items-center mb-4">
                <i class="fas fa-file-signature text-indigo-700 mr-3"></i> {{ resolution.title }}
            </h1>
            <a href="{{ url_for('uip_bp.dashboard', org_slug=org.slug) }}" class="text-slate-500 hover:text-indigo-600 font-bold text-sm inline-flex items-center transition bg-white border border-slate-200 px-3 py-1.5 rounded-lg shadow-sm"><i class="fas fa-arrow-left mr-2"></i> Back to Dashboard</a>
        </div>"""
text = text.replace(old_header, new_header)

# 2. Extract Debate Thread and insert it into left column
# Find Debate thread using string splitting or regex
pattern = re.compile(r'(\s*<!-- Debate Thread -->.*?</div>\s*</div>\s*</div>)', re.DOTALL)
match = pattern.search(text)
if match:
    debate_block = match.group(1)
    # Remove it from current location
    text = text.replace(debate_block, "")
    
    # Now find where to insert it (after Chairman's Gavel {% endif %})
    insert_target = """            </div>
            {% endif %}
        </div>
        
        <!-- Right Column: Voting & Debate -->"""
    
    insert_replacement = """            </div>
            {% endif %}
            
""" + debate_block.lstrip() + """
        </div>
        
        <!-- Right Column: Voting & Debate -->"""
    
    text = text.replace(insert_target, insert_replacement)
    print("Debate block moved!")
else:
    print("Debate block not found!")

with open("templates/program_uip/dashboards/resolution_view.html", "w", encoding="utf-8") as f:
    f.write(text)
