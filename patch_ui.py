with open("templates/program_uip/dashboards/resolution_view.html", "r", encoding="utf-8") as f:
    text = f.read()

old_block = """            {% if current_appointment and current_appointment.position.lower() in ["secretary", "chairman", "vice chairman", "chair", "chairperson", "vice chair"] %}
            <div class="bg-slate-900 rounded-xl shadow-sm p-6 text-white">
                <h3 class="font-bold text-lg mb-2"><i class="fas fa-forward text-amber-400 mr-2"></i> Close Voting</h3>
                <p class="text-sm text-slate-400 mb-4">Once all votes are in, move this to the live meeting for final ratification.</p>
                <form method="POST" action="{{ url_for('uip_bp.decide_resolution', org_slug=org.slug, res_id=resolution.id) }}">
                    <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                    <button type="submit" name="decision" value="TABLED" class="w-full bg-indigo-600 hover:bg-indigo-500 text-white font-bold py-3 rounded-lg transition shadow">Move to Live Meeting</button>
                </form>
            </div>
            {% endif %}"""

new_block = """            {% if current_appointment and current_appointment.position.lower() in ["secretary", "chairman", "vice chairman", "chair", "chairperson", "vice chair"] %}
            <div class="bg-slate-900 rounded-xl shadow-sm p-6 text-white">
                <h3 class="font-bold text-lg mb-2"><i class="fas fa-exclamation-triangle text-amber-400 mr-2"></i> Manual Override</h3>
                <p class="text-[11px] text-slate-400 mb-4">Voting concludes automatically at 100%. Use this manual override only if a member refuses to vote (triggers Name & Shame rule).</p>
                <form method="POST" action="{{ url_for('uip_bp.decide_resolution', org_slug=org.slug, res_id=resolution.id) }}" class="flex flex-col gap-2">
                    <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                    <button type="submit" name="decision" value="ADOPTED" class="w-full bg-emerald-600 hover:bg-emerald-500 text-white font-bold py-2 rounded-lg transition shadow text-xs">Force Adopt</button>
                    <button type="submit" name="decision" value="REJECTED" class="w-full bg-rose-600 hover:bg-rose-500 text-white font-bold py-2 rounded-lg transition shadow text-xs">Force Reject</button>
                    <button type="submit" name="decision" value="TABLED" class="w-full bg-indigo-600 hover:bg-indigo-500 text-white font-bold py-2 rounded-lg transition shadow text-xs">Force Table (Live Meeting)</button>
                </form>
            </div>
            {% endif %}"""

if old_block in text:
    text = text.replace(old_block, new_block)
    with open("templates/program_uip/dashboards/resolution_view.html", "w", encoding="utf-8") as f:
        f.write(text)
    print("Patched UI successfully")
else:
    print("Could not find the block to replace")
