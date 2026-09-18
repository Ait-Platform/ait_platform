import re

with open("artifacts/rcm-stages123-release/app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

def_start = text.find("def router_page(org_slug):")
def_end = text.find("@uip_bp.route", def_start)

new_func = """def router_page(org_slug):
    org = g.organization
    from flask import request, redirect, url_for
    from flask_login import current_user
    from sqlalchemy import func
    force_menu = request.args.get('force')
    
    # 1. Auto-route if already verified committee
    from app.models.uip_governance import UipCommitteeMember
    appointment = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
    ).first()
    
    if appointment and not force_menu:
        from app.models.core import CoreOrganizationMember
        from app import db
        membership = CoreOrganizationMember.query.filter_by(organization_id=org.id, user_id=current_user.id).first()
        if not membership:
            membership = CoreOrganizationMember(organization_id=org.id, user_id=current_user.id, is_active=True)
            db.session.add(membership)
        elif not membership.is_active:
            membership.is_active = True
        db.session.commit()
        
        pos = appointment.position.lower()
        if pos in ['chairman', 'chairperson', 'chair', 'vice chair', 'vice chairman']:
            return redirect(url_for('uip_bp.dashboard', org_slug=org.slug))
        return redirect(url_for("uip_bp.committee_dashboard", org_slug=org.slug))
        
    # 2. Render normal tiles
    return render_template(
        "uip/router.html",
        org=org
    )
"""

if def_end == -1:
    text = text[:def_start] + new_func
else:
    text = text[:def_start] + new_func + "\n" + text[def_end:]

with open("artifacts/rcm-stages123-release/app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Fixed router_page function completely")
