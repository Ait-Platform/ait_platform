# First, let's inject the voting buttons properly into the 3 templates.
voting_block = """
        <!-- Right Column: Voting & Actions -->
        <div>
            {% if resolution.status == 'PROPOSED' %}
            <div class="bg-white rounded-xl shadow-sm border border-slate-200 p-6 mb-6">
                <h3 class="font-bold text-indigo-900 mb-4"><i class="fas fa-vote-yea mr-2"></i> Cast Your Vote</h3>
                
                {% if has_voted %}
                <div class="mb-4 text-sm text-indigo-700 bg-indigo-100 p-2 rounded text-center font-bold">
                    You have securely cast your vote.
                </div>
                {% endif %}
                
                <form method="POST" action="{{ VOTE_URL }}" class="flex flex-row gap-3 w-full">
                    <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                    <button type="submit" name="vote" value="YEA" class="flex-1 py-3 rounded-lg font-black text-emerald-700 bg-emerald-50 border border-emerald-200 hover:bg-emerald-100 hover:border-emerald-300 transition shadow-sm text-center">
                        YEA
                    </button>
                    <button type="submit" name="vote" value="NAY" class="flex-1 py-3 rounded-lg font-black text-rose-700 bg-rose-50 border border-rose-200 hover:bg-rose-100 hover:border-rose-300 transition shadow-sm text-center">
                        NAY
                    </button>
                    <button type="submit" name="vote" value="ABSTAIN" class="flex-1 py-3 rounded-lg font-black text-amber-700 bg-amber-50 border border-amber-200 hover:bg-amber-100 hover:border-amber-300 transition shadow-sm text-center">
                        ABSTAIN
                    </button>
                </form>
            </div>
            {% endif %}
        </div>
"""

# Let's read chairman_resolution_view.html
with open("templates/program_uip/dashboards/chairman_resolution_view.html", "r", encoding="utf-8") as f:
    c_text = f.read()
    
# It's currently ending with:
#             {% endif %}
# </div>
# {% endblock %}

# Let's just insert the voting block right before the closing </div>
# Wait, it's a 3-column grid. <div class="grid md:grid-cols-3 gap-8">
# The left column is col-span-2.
# Let's fix the layout properly.
