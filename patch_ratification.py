import re

# 1. Update the UI
with open("templates/program_uip/dashboards/resolution_view.html", "r", encoding="utf-8") as f:
    html = f.read()

old_block = """        {% if current_appointment and current_appointment.position.lower() in ["secretary", "chairman", "vice chairman", "chair", "chairperson", "vice chair"] %}
        <div class="mb-6 bg-slate-900 rounded-xl p-6 shadow-lg border border-slate-800 text-white flex items-center justify-between">
            <div>
                <h3 class="font-bold text-lg mb-1"><i class="fas fa-gavel text-amber-500 mr-2"></i> Post-Meeting Ratification</h3>
                <p class="text-slate-400 text-sm">After the live meeting concludes, formally record the outcome here.</p>
            </div>
            <div class="flex gap-3">
                <form method="POST" action="{{ url_for('uip_bp.decide_resolution', org_slug=org.slug, res_id=resolution.id) }}">
                    <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                    <button type="submit" name="decision" value="REJECTED" class="px-4 py-2 bg-red-900 hover:bg-red-800 text-red-100 rounded-lg font-bold transition">
                        Reject
                    </button>
                    <button type="submit" name="decision" value="ADOPTED" class="px-6 py-2 bg-emerald-600 hover:bg-emerald-500 text-white rounded-lg font-bold shadow transition">
                        <i class="fas fa-check-double mr-1"></i> Adopt
                    </button>
                </form>
            </div>
        </div>
        {% endif %}"""

new_block = """        {% if current_appointment and current_appointment.position.lower() in ["secretary", "chairman", "vice chairman", "chair", "chairperson", "vice chair"] %}
        <div class="mb-6 bg-slate-900 rounded-xl p-6 shadow-lg border border-slate-800 text-white">
            <div class="mb-4">
                <h3 class="font-bold text-lg mb-1"><i class="fas fa-gavel text-amber-500 mr-2"></i> Post-Meeting Ratification</h3>
                <p class="text-slate-400 text-sm">After the live meeting concludes, formally record the outcome and the live vote tally here.</p>
            </div>
            
            <form method="POST" action="{{ url_for('uip_bp.decide_resolution', org_slug=org.slug, res_id=resolution.id) }}" class="space-y-4">
                <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                
                <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                        <label class="block text-xs font-bold text-slate-400 uppercase tracking-widest mb-1">Meeting Date</label>
                        <input type="date" name="meeting_date" required class="w-full bg-slate-800 border border-slate-700 rounded-lg px-4 py-2 text-white focus:outline-none focus:border-amber-500">
                    </div>
                    <div>
                        <label class="block text-xs font-bold text-slate-400 uppercase tracking-widest mb-1">Location / Format</label>
                        <input type="text" name="meeting_location" placeholder="e.g. Community Hall or Zoom" required class="w-full bg-slate-800 border border-slate-700 rounded-lg px-4 py-2 text-white focus:outline-none focus:border-amber-500">
                    </div>
                </div>
                
                <div>
                    <label class="block text-xs font-bold text-slate-400 uppercase tracking-widest mb-1">Live Meeting Vote Tally</label>
                    <div class="grid grid-cols-3 gap-4">
                        <div class="bg-slate-800 border border-slate-700 rounded-lg p-3 flex items-center justify-between">
                            <span class="text-emerald-400 font-bold">YEA</span>
                            <input type="number" name="live_yea" min="0" required class="w-16 bg-slate-900 border border-slate-600 rounded px-2 py-1 text-white text-center">
                        </div>
                        <div class="bg-slate-800 border border-slate-700 rounded-lg p-3 flex items-center justify-between">
                            <span class="text-rose-400 font-bold">NAY</span>
                            <input type="number" name="live_nay" min="0" required class="w-16 bg-slate-900 border border-slate-600 rounded px-2 py-1 text-white text-center">
                        </div>
                        <div class="bg-slate-800 border border-slate-700 rounded-lg p-3 flex items-center justify-between">
                            <span class="text-amber-400 font-bold">ABSTAIN</span>
                            <input type="number" name="live_abstain" min="0" required class="w-16 bg-slate-900 border border-slate-600 rounded px-2 py-1 text-white text-center">
                        </div>
                    </div>
                </div>
                
                <div class="pt-4 mt-2 border-t border-slate-800 flex justify-end gap-3">
                    <button type="submit" name="decision" value="REJECTED" class="px-6 py-2.5 bg-slate-800 hover:bg-rose-900 text-rose-100 border border-slate-700 hover:border-rose-700 rounded-lg font-bold transition">
                        Reject Mandate
                    </button>
                    <button type="submit" name="decision" value="ADOPTED" class="px-8 py-2.5 bg-emerald-600 hover:bg-emerald-500 text-white rounded-lg font-bold shadow transition">
                        <i class="fas fa-gavel mr-2"></i> Formally Adopt
                    </button>
                </div>
            </form>
        </div>
        {% endif %}"""

html = html.replace(old_block, new_block)
with open("templates/program_uip/dashboards/resolution_view.html", "w", encoding="utf-8") as f:
    f.write(html)
print("Updated UI template")

# 2. Update the backend route
with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    route_text = f.read()

route_old = """    if decision == "TABLED":
        res.status = "TABLED"
        db.session.commit()
        flash("Voting closed. Resolution has been tabled for a live meeting.", "success")
        return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=res.id))
        
    res.status = decision"""

route_new = """    if decision == "TABLED":
        res.status = "TABLED"
        db.session.commit()
        flash("Voting closed. Resolution has been tabled for a live meeting.", "success")
        return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=res.id))
        
    # Capture the ratification context
    meeting_date = request.form.get("meeting_date")
    meeting_location = request.form.get("meeting_location")
    live_yea = request.form.get("live_yea")
    live_nay = request.form.get("live_nay")
    live_abstain = request.form.get("live_abstain")
    
    if meeting_date and meeting_location:
        # Store securely in the JSON column
        import datetime
        try:
            res.decision_date = datetime.datetime.strptime(meeting_date, "%Y-%m-%d").date()
        except:
            pass
            
        current_basis = res.result_basis or {}
        # we need to reassign to trigger SQLAlchemy JSON mutation detection cleanly
        new_basis = dict(current_basis)
        new_basis["ratification"] = {
            "date": meeting_date,
            "location": meeting_location,
            "votes_yea": live_yea,
            "votes_nay": live_nay,
            "votes_abstain": live_abstain,
            "recorded_by_name": current_user.name,
            "recorded_by_email": current_user.email
        }
        res.result_basis = new_basis
        
    res.status = decision"""

route_text = route_text.replace(route_old, route_new)
with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(route_text)
print("Updated backend route")
