import re

with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_committee_route = """    if role_slug == "committee_member":
        from app.models.uip_governance import UipCommitteeMember
        from sqlalchemy import func
        current_appointment = UipCommitteeMember.query.filter(
            UipCommitteeMember.organization_id == org.id,
            UipCommitteeMember.status == "CURRENT",
            func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
        ).first()
        
        if current_appointment and current_appointment.position.lower() in ["chairman", "vice chairman", "chair", "chairperson", "vice chair"]:
            from app.program_uip.presentation import executive
            return render_template("program_uip/dashboards/manager.html", org=org, overview=executive(org.id, current_user.id))
            
        return redirect(url_for("uip_bp.committee_dashboard", org_slug=org_slug))"""

new_committee_route = """    if role_slug == "committee_member":
        from app.models.uip_governance import UipCommitteeMember
        from sqlalchemy import func
        current_appointment = UipCommitteeMember.query.filter(
            UipCommitteeMember.organization_id == org.id,
            UipCommitteeMember.status == "CURRENT",
            func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
        ).first()
        
        if current_appointment:
            pos = current_appointment.position.lower()
            if pos in ["chairman", "vice chairman", "chair", "chairperson", "vice chair"]:
                from app.program_uip.presentation import executive
                return render_template("program_uip/dashboards/manager.html", org=org, overview=executive(org.id, current_user.id))
            elif pos == "treasurer":
                return redirect(url_for("uip_bp.finance_overview", org_slug=org_slug))
            
        return redirect(url_for("uip_bp.committee_dashboard", org_slug=org_slug))"""

text = text.replace(old_committee_route, new_committee_route)

# Wait, there are other places where it auto routes after verification, e.g.:
# if pos in ['chairman', 'chairperson', 'chair', 'vice chair', 'vice chairman']:
#     return redirect(url_for('uip_bp.dashboard', org_slug=org.slug))
# Let's fix those too.
text = re.sub(
    r"if pos in \['chairman', 'chairperson', 'chair', 'vice chair', 'vice chairman'\]:\s+return redirect\(url_for\('uip_bp\.dashboard', org_slug=org\.slug\)\)",
    "if pos in ['chairman', 'chairperson', 'chair', 'vice chair', 'vice chairman', 'treasurer']:\n            return redirect(url_for('uip_bp.dashboard', org_slug=org.slug))",
    text
)

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated dashboard routing for Treasurer")
