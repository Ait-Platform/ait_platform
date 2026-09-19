with open("templates/program_uip/dashboards/resolution_draft.html", "r", encoding="utf-8") as f:
    text = f.read()

old_block = """        <div class="grid grid-cols-1 md:grid-cols-2 gap-6 pt-4 border-t border-slate-100">
            <div>
                <label class="block text-sm font-bold text-slate-700 mb-2">Voting Scope</label>
                <select name="voting_scope" class="w-full p-3 border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500">
                    <option value="EXCO_CORE" {% if resolution and resolution.voting_scope == 'EXCO_CORE' %}selected{% endif %}>Core ExCo Only (Chair, Vice, Sec, Treas)</option>
                    <option value="COMMITTEE_ALL" {% if resolution and resolution.voting_scope == 'COMMITTEE_ALL' %}selected{% endif %}>Full Committee (Core + Sub-committees)</option>
                    <option value="SUB_COMMITTEE" {% if resolution and resolution.voting_scope == 'SUB_COMMITTEE' %}selected{% endif %}>Sub-Committee</option>
                    <option value="PUBLIC" {% if resolution and resolution.voting_scope == 'PUBLIC' %}selected{% endif %}>Public / Ratepayers</option>
                </select>
            </div>
            <div>
                <label class="block text-sm font-bold text-slate-700 mb-2">Quorum Target (%)</label>
                <input type="number" name="quorum_target" min="1" max="100" value="{{ resolution.quorum_target if resolution else 50 }}" class="w-full p-3 border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500">
                <p class="text-[10px] text-amber-700 mt-2 font-bold leading-tight bg-amber-50 p-2 rounded border border-amber-100">
                    <i class="fas fa-info-circle mr-1"></i> Ensure this complies with the Companies Act and Precinct Constitution (Standard ExCo decisions typically require a minimum 50% quorum).
                </p>
            </div>
        </div>"""

new_block = """        <div class="pt-4 border-t border-slate-100">
            <label class="block text-sm font-bold text-slate-700 mb-2">Voting Scope (Participation Requirement)</label>
            <select name="voting_scope" class="w-full p-3 border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500">
                <option value="EXCO_CORE" {% if resolution and resolution.voting_scope == 'EXCO_CORE' %}selected{% endif %}>Core ExCo Only (Chair, Vice, Sec, Treas)</option>
                <option value="COMMITTEE_ALL" {% if resolution and resolution.voting_scope == 'COMMITTEE_ALL' %}selected{% endif %}>Full Committee (Core + Sub-committees)</option>
                <option value="SUB_COMMITTEE" {% if resolution and resolution.voting_scope == 'SUB_COMMITTEE' %}selected{% endif %}>Sub-Committee</option>
                <option value="PUBLIC" {% if resolution and resolution.voting_scope == 'PUBLIC' %}selected{% endif %}>Public / Ratepayers</option>
            </select>
            <p class="text-[11px] text-slate-500 mt-2 font-medium leading-tight">
                <i class="fas fa-info-circle mr-1 text-indigo-400"></i> The digital platform records participation and enforces the mandatory voting rule. Final legal quorum validation occurs at the live meeting according to the UIP Constitution.
            </p>
        </div>"""

if old_block in text:
    text = text.replace(old_block, new_block)
    with open("templates/program_uip/dashboards/resolution_draft.html", "w", encoding="utf-8") as f:
        f.write(text)
    print("Patched draft HTML")
else:
    print("Could not find old block in draft html")
