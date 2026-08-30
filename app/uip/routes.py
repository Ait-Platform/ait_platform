from flask import render_template, redirect, url_for, flash, request
from flask_login import login_required, current_user
from app.uip import uip_bp
from app.models.core import CoreOrganization, CoreRoleAssignment, CoreInteraction
from app.extensions import db

@uip_bp.route("/<org_slug>/dashboard")
@login_required
def dashboard(org_slug):
    from flask import g
    org = g.organization
    
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


@uip_bp.route("/<org_slug>/settings", methods=["GET", "POST"])
@login_required
def org_settings(org_slug):
    from flask import g
    org = g.organization
    
    # Check permissions (only Manager or Committee Member can edit)
    assignment = CoreRoleAssignment.query.filter_by(user_id=current_user.id, organization_id=org.id).first()
    if not assignment or assignment.role.slug not in ["manager", "committee_member", "owner"]:
        flash("You do not have permission to access organisation settings.", "danger")
        return redirect(url_for("uip_bp.dashboard", org_slug=org_slug))
        
    if request.method == "POST":
        org.name = request.form.get("name")
        org.area = request.form.get("area")
        org.municipality_ref = request.form.get("municipality_ref")
        org.contact_email = request.form.get("contact_email")
        org.contact_phone = request.form.get("contact_phone")
        org.status = request.form.get("status")
        db.session.commit()
        flash("Organisation settings updated successfully.", "success")
        return redirect(url_for("uip_bp.org_settings", org_slug=org_slug))
        
    return render_template("uip/admin/settings.html", org=org)

import random
from app.models.auth import User

@uip_bp.route("/<org_slug>/interaction/new", methods=["GET", "POST"])
@login_required
def new_interaction(org_slug):
    from flask import g
    org = g.organization
    
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
