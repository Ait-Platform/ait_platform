import re

with open('app/uip/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

new_routes = """
import random
from app.models.auth import User

@uip_bp.route("/<org_slug>/interaction/new", methods=["GET", "POST"])
@login_required
def new_interaction(org_slug):
    org = CoreOrganization.query.filter_by(slug=org_slug).first_or_404()
    
    # Check permissions (Receptionist or Manager)
    assignment = CoreRoleAssignment.query.filter_by(user_id=current_user.id, organization_id=org.id).first()
    if not assignment or assignment.role.slug not in ["manager", "receptionist", "committee_member"]:
        flash("You do not have permission to create interactions.", "danger")
        return redirect(url_for("uip_bp.dashboard", org_slug=org_slug))
        
    if request.method == "POST":
        creator_id = current_user.id
        # In a real app, 'contact_email' would be mapped to a resident ID via AJAX search
        resident_email = request.form.get("resident_email")
        resident = User.query.filter_by(email=resident_email).first()
        if resident:
            creator_id = resident.id # Receptionist logs it on behalf of the resident
            
        ref = f"{org.slug[:2].upper()}-{random.randint(10000, 99999)}"
        
        ix = CoreInteraction(
            reference=ref,
            organization_id=org.id,
            creator_id=creator_id, # The person reporting it
            title=request.form.get("title"),
            description=request.form.get("description"),
            channel=request.form.get("channel"),
            category=request.form.get("category"),
            interaction_type=request.form.get("category"), # Fallback for legacy Phase 3
            priority=request.form.get("priority", "NORMAL"),
            status="NEW"
        )
        db.session.add(ix)
        db.session.commit()
        
        flash(f"Interaction {ref} logged successfully.", "success")
        return redirect(url_for("uip_bp.dashboard", org_slug=org.slug))
        
    return render_template("uip/reception/new_interaction.html", org=org)
"""

if "def new_interaction" not in text:
    with open('app/uip/routes.py', 'a', encoding='utf-8') as f:
        f.write(new_routes)
    print("Added new_interaction route.")
