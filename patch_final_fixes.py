import re
with open("templates/program_uip/dashboards/resolution_view.html", "r", encoding="utf-8") as f:
    text = f.read()

# 1. Bigger Title
old_title = """        <div>
            <div class="text-xs font-bold text-slate-500 uppercase tracking-widest mb-3">
                <i class="fas fa-landmark text-indigo-500 mr-2"></i> 
                {% if resolution.status == 'ADOPTED' %}Historical Resolution Record{% elif resolution.status == 'DRAFT' %}Drafting Desk{% else %}Digital Voting Room{% endif %}
            </div>"""

new_title = """        <div class="flex-grow pr-8">
            <div class="text-lg font-black text-slate-800 uppercase tracking-widest mb-4 border-b border-slate-200 pb-2 flex items-center">
                <i class="fas fa-landmark text-indigo-600 mr-3 text-xl"></i> 
                {% if resolution.status == 'ADOPTED' or resolution.status == 'REJECTED' %}Historical Resolution Record{% elif resolution.status == 'DRAFT' %}Drafting Desk{% else %}Digital Voting Room{% endif %}
            </div>"""
if old_title in text:
    text = text.replace(old_title, new_title)
    print("Fixed Title")
else:
    print("Failed to fix title")

# 2. Remove Debate Thread Block (Regex this time, matching exactly the block)
pattern = re.compile(r'<!-- Debate Thread -->.*?</div>\s*</div>\s*</div>\s*</div>\s*<!-- Right Column: Voting & Debate -->', re.DOTALL)
match = pattern.search(text)
if match:
    text = text.replace(match.group(0), "            </div>\n        </div>\n        \n        <!-- Right Column: Voting & Debate -->")
    print("Removed Debate Thread")
else:
    print("Failed to remove debate thread")
    
# 3. Horizontal Vote Buttons + Close Voting Button (Replacing the entire Cast Your Vote block)
old_vote_block = """            <!-- Secure Voting Block -->
            {% if resolution.status == 'PROPOSED' %}
            <div class="bg-indigo-50 border border-indigo-100 rounded-xl shadow-sm p-6">
                <h3 class="font-bold text-indigo-900 mb-4"><i class="fas fa-vote-yea mr-2"></i> Cast Your Vote</h3>
                
                {% if my_vote %}
                <div class="mb-4 text-sm text-indigo-700 bg-indigo-100 p-2 rounded text-center font-bold">
                    You securely voted: {{ my_vote.vote }}
                </div>
                {% endif %}
                
                <form method="POST" action="{{ url_for('uip_bp.vote_resolution', org_slug=org.slug, res_id=resolution.id) }}" class="space-y-2">
                    <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                    <button type="submit" name="vote" value="YEA" class="w-full py-3 rounded-lg font-black text-white bg-emerald-600 hover:bg-emerald-700 transition shadow-sm">
                        YEA
                    </button>
                    <button type="submit" name="vote" value="NAY" class="w-full py-3 rounded-lg font-black text-white bg-red-600 hover:bg-red-700 transition shadow-sm">
                        NAY
                    </button>
                    <button type="submit" name="vote" value="ABSTAIN" class="w-full py-2 rounded-lg font-bold text-slate-600 bg-white border border-slate-300 hover:bg-slate-50 transition">
                        ABSTAIN
                    </button>
                </form>
            </div>
            {% endif %}"""

new_vote_block = """            <!-- Secure Voting Block -->
            {% if resolution.status == 'PROPOSED' %}
            <div class="bg-indigo-50 border border-indigo-100 rounded-xl shadow-sm p-6">
                <h3 class="font-bold text-indigo-900 mb-4"><i class="fas fa-vote-yea mr-2"></i> Cast Your Vote</h3>
                
                {% if my_vote %}
                <div class="mb-4 text-sm text-indigo-700 bg-indigo-100 p-2 rounded text-center font-bold">
                    You securely voted: {{ my_vote.vote }}
                </div>
                {% endif %}
                
                <form method="POST" action="{{ url_for('uip_bp.vote_resolution', org_slug=org.slug, res_id=resolution.id) }}" class="flex flex-row gap-3 w-full">
                    <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                    <button type="submit" name="vote" value="YEA" class="flex-1 py-3 rounded-lg font-black text-white bg-emerald-600 hover:bg-emerald-700 transition shadow-sm text-center">
                        YEA
                    </button>
                    <button type="submit" name="vote" value="NAY" class="flex-1 py-3 rounded-lg font-black text-white bg-red-600 hover:bg-red-700 transition shadow-sm text-center">
                        NAY
                    </button>
                    <button type="submit" name="vote" value="ABSTAIN" class="flex-1 py-3 rounded-lg font-black text-slate-600 bg-white border border-slate-300 hover:bg-slate-50 transition shadow-sm text-center">
                        ABSTAIN
                    </button>
                </form>
            </div>
            
            {% if current_appointment and current_appointment.position.lower() in ["secretary", "chairman", "vice chairman", "chair", "chairperson", "vice chair"] %}
            <div class="bg-slate-900 rounded-xl shadow-sm p-6 text-white">
                <h3 class="font-bold text-lg mb-2"><i class="fas fa-forward text-amber-400 mr-2"></i> Close Voting</h3>
                <p class="text-sm text-slate-400 mb-4">Once all votes are in, move this to the live meeting for final ratification.</p>
                <form method="POST" action="{{ url_for('uip_bp.decide_resolution', org_slug=org.slug, res_id=resolution.id) }}">
                    <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                    <button type="submit" name="decision" value="TABLED" class="w-full bg-indigo-600 hover:bg-indigo-500 text-white font-bold py-3 rounded-lg transition shadow">Move to Live Meeting</button>
                </form>
            </div>
            {% endif %}
            
            {% endif %}"""

if old_vote_block in text:
    text = text.replace(old_vote_block, new_vote_block)
    print("Fixed Vote Block and added Close Voting")
else:
    print("Failed to fix vote block")

with open("templates/program_uip/dashboards/resolution_view.html", "w", encoding="utf-8") as f:
    f.write(text)
