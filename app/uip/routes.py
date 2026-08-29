from flask import render_template, redirect, url_for, flash, request
from flask_login import login_required, current_user
from app.uip import uip_bp
from app.models.core import CoreOrganization, CoreRoleAssignment, CoreInteraction
from app.extensions import db

@uip_bp.route("/<org_slug>/dashboard")
@login_required
def dashboard(org_slug):
    org = CoreOrganization.query.filter_by(slug=org_slug).first_or_404()
    
    # Check user's role in this org
    assignment = CoreRoleAssignment.query.filter_by(user_id=current_user.id, organization_id=org.id).first()
    
    if not assignment:
        flash("You do not have an active role in this UIP.", "warning")
        return redirect(url_for("public_bp.welcome"))
        
    role_slug = assignment.role.slug
    
    # Route to the correct dashboard based on role
    if role_slug == "resident":
        interactions = CoreInteraction.query.filter_by(organization_id=org.id, creator_id=current_user.id).all()
        return render_template("uip/dashboards/resident.html", org=org, interactions=interactions)
        
    elif role_slug == "receptionist":
        open_interactions = CoreInteraction.query.filter_by(organization_id=org.id, status="open").all()
        return render_template("uip/dashboards/receptionist.html", org=org, open_interactions=open_interactions)
        
    elif role_slug == "manager":
        return render_template("uip/dashboards/manager.html", org=org)
        
    else:
        return f"Dashboard for {role_slug} under construction."
