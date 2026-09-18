import re
with open("templates/program_uip/dashboards/resolution_view.html", "r", encoding="utf-8") as f:
    text = f.read()

# Replace ESCALATED with TABLED everywhere
text = text.replace("'ESCALATED'", "'TABLED'")
text = text.replace("ESCALATED", "TABLED")
text = text.replace("Escalated", "Tabled")

# Fix the header block for TABLED
old_banner = """        {% if resolution.status == 'TABLED' %}
        <div class="mb-6 bg-red-50 border border-red-200 rounded-xl p-6 shadow-sm">
            <h3 class="font-bold text-red-900 mb-2 text-lg"><i class="fas fa-exclamation-triangle mr-2"></i> Digital Voting Suspended</h3>
            <p class="text-sm text-red-700">A dissenting view (NAY) was logged. In accordance with round-robin governance rules, this resolution has been escalated and must be tabled at a formal live meeting.</p>
        </div>"""

new_banner = """        {% if resolution.status == 'TABLED' %}
        <div class="mb-6 bg-amber-50 border border-amber-200 rounded-xl p-6 shadow-sm">
            <h3 class="font-bold text-amber-900 mb-2 text-lg"><i class="fas fa-users mr-2"></i> Tabled for Live Meeting</h3>
            <p class="text-sm text-amber-700">Digital voting has concluded. This resolution is now waiting to be formally debated and ratified at a live committee meeting.</p>
        </div>"""
text = text.replace(old_banner, new_banner)

# We need to add the "Close Voting" button to the PROPOSED state.
# I'll put it in the Right Column under "Cast Your Vote" block.
# Wait, actually, the Ratification panel currently shows ONLY if TABLED (because it's inside the {% if resolution.status == 'TABLED' %} block from my last patch). 
# Let's add the "Close Voting" panel inside the {% if resolution.status == 'PROPOSED' %} right column.

old_vote_block = """                  <div class="mt-4 pt-4 border-t border-indigo-200">
                      <form method="POST" action="{{ url_for('uip_bp.vote_resolution', org_slug=org.slug, res_id=resolution.id) }}" class="flex gap-2">
                          <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                          <button type="submit" name="vote" value="YEA" class="flex-1 bg-emerald-600 hover:bg-emerald-500 text-white font-bold py-2 rounded transition text-sm text-center">YEA</button>
                          <button type="submit" name="vote" value="NAY" class="flex-1 bg-red-600 hover:bg-red-500 text-white font-bold py-2 rounded transition text-sm text-center">NAY</button>
                          <button type="submit" name="vote" value="ABSTAIN" class="flex-1 bg-slate-500 hover:bg-slate-400 text-white font-bold py-2 rounded transition text-sm text-center">ABSTAIN</button>
                      </form>
                  </div>
                  {% endif %}
              </div>
              {% endif %}"""

new_vote_block = """                  <div class="mt-4 pt-4 border-t border-indigo-200">
                      <form method="POST" action="{{ url_for('uip_bp.vote_resolution', org_slug=org.slug, res_id=resolution.id) }}" class="flex gap-2">
                          <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                          <button type="submit" name="vote" value="YEA" class="flex-1 bg-emerald-600 hover:bg-emerald-500 text-white font-bold py-2 rounded transition text-sm text-center">YEA</button>
                          <button type="submit" name="vote" value="NAY" class="flex-1 bg-red-600 hover:bg-red-500 text-white font-bold py-2 rounded transition text-sm text-center">NAY</button>
                          <button type="submit" name="vote" value="ABSTAIN" class="flex-1 bg-slate-500 hover:bg-slate-400 text-white font-bold py-2 rounded transition text-sm text-center">ABSTAIN</button>
                      </form>
                  </div>
                  {% endif %}
              </div>
              
              {% if current_appointment and current_appointment.position.lower() in ["secretary", "chairman", "vice chairman", "chair", "chairperson", "vice chair"] %}
              <div class="bg-slate-900 rounded-xl shadow-sm p-5 text-white">
                  <h3 class="font-bold text-lg mb-2"><i class="fas fa-forward text-amber-400 mr-2"></i> Close Voting</h3>
                  <p class="text-sm text-slate-400 mb-4">Once all votes are in, move this to the live meeting for final ratification.</p>
                  <form method="POST" action="{{ url_for('uip_bp.decide_resolution', org_slug=org.slug, res_id=resolution.id) }}">
                      <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                      <button type="submit" name="decision" value="TABLED" class="w-full bg-indigo-600 hover:bg-indigo-500 text-white font-bold py-2.5 rounded-lg transition shadow">Move to Live Meeting</button>
                  </form>
              </div>
              {% endif %}
              
              {% endif %}"""
text = text.replace(old_vote_block, new_vote_block)

# Clean up any leftover Chairman's Gavel in the left column for PROPOSED state from earlier code
old_gavel = """            <!-- Chairman's Gavel (Only visible to Chair if PROPOSED) -->
            {% if resolution.status == 'PROPOSED' and current_appointment and current_appointment.position.lower() in ["chairman", "vice chairman", "chair", "chairperson", "vice chair"] %}
            <div class="bg-slate-900 rounded-xl p-6 shadow-lg border border-slate-800 text-white flex items-center justify-between">
                <div>
                    <h3 class="font-bold text-lg mb-1"><i class="fas fa-legal text-amber-500 mr-2"></i> Chairman's Gavel</h3>
                    <p class="text-slate-400 text-sm">Legally bind the community to this resolution.</p>
                </div>
                <div class="flex gap-3">
                    <form method="POST" action="{{ url_for('uip_bp.decide_resolution', org_slug=org.slug, res_id=resolution.id) }}">
                        <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                        <button type="submit" name="decision" value="REJECTED" class="px-4 py-2 bg-red-900 hover:bg-red-800 text-red-100 rounded-lg font-bold transition">
                            Reject
                        </button>
                        
                        <button type="submit" name="decision" value="ADOPTED" class="px-6 py-2 bg-emerald-600 hover:bg-emerald-500 text-white rounded-lg font-bold shadow transition disabled:opacity-50 disabled:cursor-not-allowed" {% if not quorum_met %}disabled title="Quorum not met"{% endif %}>
                            <i class="fas fa-check-double mr-1"></i> Adopt
                        </button>
                    </form>
                </div>
            </div>
            {% endif %}"""
text = text.replace(old_gavel, "")

with open("templates/program_uip/dashboards/resolution_view.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated resolution_view.html for TABLED flow")
