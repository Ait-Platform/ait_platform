with open("templates/program_uip/dashboards/resolution_draft.html", "r", encoding="utf-8") as f:
    text = f.read()

old_select = """                <select name="voting_scope" class="w-full p-3 border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500">
                    <option value="EXCO_CORE" {% if resolution and resolution.voting_scope == 'EXCO_CORE' %}selected{% endif %}>Core ExCo Only (Chair, Vice, Sec, Treas)</option>
                    <option value="COMMITTEE_ALL" {% if resolution and resolution.voting_scope == 'COMMITTEE_ALL' %}selected{% endif %}>Full Committee (Core + Sub-committees)</option>
                    <option value="PUBLIC" {% if resolution and resolution.voting_scope == 'PUBLIC' %}selected{% endif %}>Public / Ratepayers</option>
                </select>"""

new_select = """                <select name="voting_scope" class="w-full p-3 border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500">
                    <option value="EXCO_CORE" {% if resolution and resolution.voting_scope == 'EXCO_CORE' %}selected{% endif %}>Core ExCo Only (Chair, Vice, Sec, Treas)</option>
                    <option value="COMMITTEE_ALL" {% if resolution and resolution.voting_scope == 'COMMITTEE_ALL' %}selected{% endif %}>Full Committee (Core + Sub-committees)</option>
                    <option value="SUB_COMMITTEE" {% if resolution and resolution.voting_scope == 'SUB_COMMITTEE' %}selected{% endif %}>Sub-Committee</option>
                    <option value="PUBLIC" {% if resolution and resolution.voting_scope == 'PUBLIC' %}selected{% endif %}>Public / Ratepayers</option>
                </select>"""

text = text.replace(old_select, new_select)

with open("templates/program_uip/dashboards/resolution_draft.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated resolution_draft.html dropdown")
