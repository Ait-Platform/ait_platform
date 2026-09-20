# 1. Update HTML template
with open("templates/program_uip/dashboards/secretary_organogram.html", "r", encoding="utf-8") as f:
    text = f.read()

old_actions = """                        <div class="flex items-center justify-end space-x-2">
                            <button onclick='openEditSeatModal({{ seat.id }}, `{{ seat.title }}`, `{{ seat.group_level }}`, `{{ seat.qualifier }}`, `{{ seat.duty|default("committee_member") }}`)' class="inline-flex items-center px-2 py-1 rounded bg-rose-50 border border-rose-200 text-rose-700 hover:text-rose-800 hover:border-rose-300 hover:bg-rose-100 shadow-sm transition text-xs font-bold" title="Edit Seat">
                                <i class="fas fa-pencil-alt mr-1.5 opacity-50"></i> Edit
                            </button>
                            
                            <button onclick='openAssignModal(`{{ seat.title }}`)' class="inline-flex items-center px-2 py-1 rounded bg-emerald-50 border border-emerald-200 text-emerald-700 hover:text-emerald-800 hover:border-emerald-300 hover:bg-emerald-100 shadow-sm transition text-xs font-bold" title="Assign Member">
                                <i class="fas fa-user-plus mr-1.5 opacity-50"></i> Assign
                            </button>"""

new_actions = """                        <div class="flex items-center justify-end space-x-2">
                            {% if seat.group_level == 'CORE_EXCO' or seat.title in ['Chairperson', 'Vice-Chairperson', 'Treasurer', 'Secretary'] %}
                            <div class="inline-flex items-center px-2 py-1 rounded bg-slate-50 border border-slate-200 text-slate-400 text-xs font-bold cursor-not-allowed" title="Locked by Genesis Public Meeting">
                                <i class="fas fa-lock mr-1.5 opacity-50"></i> Genesis Locked
                            </div>
                            {% else %}
                            <button onclick='openEditSeatModal({{ seat.id }}, `{{ seat.title }}`, `{{ seat.group_level }}`, `{{ seat.qualifier }}`, `{{ seat.duty|default("committee_member") }}`)' class="inline-flex items-center px-2 py-1 rounded bg-rose-50 border border-rose-200 text-rose-700 hover:text-rose-800 hover:border-rose-300 hover:bg-rose-100 shadow-sm transition text-xs font-bold" title="Edit Seat">
                                <i class="fas fa-pencil-alt mr-1.5 opacity-50"></i> Edit
                            </button>
                            
                            <button onclick='openAssignModal(`{{ seat.title }}`)' class="inline-flex items-center px-2 py-1 rounded bg-emerald-50 border border-emerald-200 text-emerald-700 hover:text-emerald-800 hover:border-emerald-300 hover:bg-emerald-100 shadow-sm transition text-xs font-bold" title="Assign Member">
                                <i class="fas fa-user-plus mr-1.5 opacity-50"></i> Assign
                            </button>
                            {% endif %}"""

text = text.replace(old_actions, new_actions)
with open("templates/program_uip/dashboards/secretary_organogram.html", "w", encoding="utf-8") as f:
    f.write(text)

# 2. Update Backend routes
with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    routes = f.read()

import re

old_edit = """        elif action == "edit_seat":
            seat_id = request.form.get("seat_id", type=int)
            seat = UipOrganogramSeat.query.get(seat_id)
            if seat and seat.organization_id == org.id:
                seat.title = request.form.get("title", seat.title)
                seat.group_level = request.form.get("group_level", seat.group_level)
                seat.qualifier = request.form.get("qualifier", seat.qualifier)
                seat.duty = request.form.get("duty", seat.duty)
                db.session.commit()
                flash(f"Blueprint seat '{seat.title}' updated.", "success")"""

new_edit = """        elif action == "edit_seat":
            seat_id = request.form.get("seat_id", type=int)
            seat = UipOrganogramSeat.query.get(seat_id)
            if seat and seat.organization_id == org.id:
                if seat.group_level == 'CORE_EXCO' or seat.title in ['Chairperson', 'Vice-Chairperson', 'Treasurer', 'Secretary']:
                    flash(f"Genesis seat '{seat.title}' cannot be manually edited.", "danger")
                else:
                    seat.title = request.form.get("title", seat.title)
                    seat.group_level = request.form.get("group_level", seat.group_level)
                    seat.qualifier = request.form.get("qualifier", seat.qualifier)
                    seat.duty = request.form.get("duty", seat.duty)
                    db.session.commit()
                    flash(f"Blueprint seat '{seat.title}' updated.", "success")"""
routes = routes.replace(old_edit, new_edit)

old_assign = """        elif action == "assign_member":
            seat_title = request.form.get("seat_title")
            member_id = request.form.get("member_id", type=int)
            if seat_title and member_id:
                from app.models.uip_governance import UipCommitteeMember
                member = UipCommitteeMember.query.filter_by(id=member_id, organization_id=org.id).first()
                if member:
                    member.position = seat_title
                    db.session.commit()
                    flash(f"{member.name} assigned to {seat_title}.", "success")"""

new_assign = """        elif action == "assign_member":
            seat_title = request.form.get("seat_title")
            member_id = request.form.get("member_id", type=int)
            if seat_title and member_id:
                if seat_title in ['Chairperson', 'Vice-Chairperson', 'Treasurer', 'Secretary']:
                    flash(f"Genesis role '{seat_title}' is strictly tied to onboarding mandates and cannot be manually assigned here.", "danger")
                else:
                    from app.models.uip_governance import UipCommitteeMember
                    member = UipCommitteeMember.query.filter_by(id=member_id, organization_id=org.id).first()
                    if member:
                        member.position = seat_title
                        db.session.commit()
                        flash(f"{member.name} assigned to {seat_title}.", "success")"""
routes = routes.replace(old_assign, new_assign)

with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(routes)
print("Updated seat protections")
