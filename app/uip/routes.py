from flask import render_template, redirect, url_for, flash, request, abort

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

        

    elif role_slug == "committee_member":
        return render_template("uip/dashboards/committee.html", org=org)
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

from datetime import datetime



from app.models.core import CoreTask, CoreRole







@uip_bp.route("/<org_slug>/interaction/<reference>")



@login_required



def view_interaction(org_slug, reference):



    from flask import g



    org = g.organization



    



    ix = CoreInteraction.query.filter_by(organization_id=org.id, reference=reference).first_or_404()



    



    # Check permissions



    assignment = CoreRoleAssignment.query.filter_by(user_id=current_user.id, organization_id=org.id).first()



    if not assignment:



        flash("You do not have access.", "danger")



        return redirect(url_for("public_bp.welcome"))



        



    # Get staff for task assignment



    staff_assignments = CoreRoleAssignment.query.filter(



        CoreRoleAssignment.organization_id == org.id,



        CoreRoleAssignment.role.has(CoreRole.slug.in_(["manager", "receptionist", "provider"]))



    ).all()



    staff_members = [a.user for a in staff_assignments]



    



    return render_template("uip/interactions/view.html", org=org, interaction=ix, staff=staff_members, current_role=assignment.role.slug)







@uip_bp.route("/<org_slug>/interaction/<reference>/task", methods=["POST"])



@login_required



def add_task(org_slug, reference):



    from flask import g



    org = g.organization



    



    ix = CoreInteraction.query.filter_by(organization_id=org.id, reference=reference).first_or_404()



    



    title = request.form.get("title")



    description = request.form.get("description")



    assignee_id = request.form.get("assignee_id")



    



    task = CoreTask(



        interaction_id=ix.id,



        assignee_id=assignee_id if assignee_id else None,



        title=title,



        description=description,



        status="pending"



    )



    db.session.add(task)



    



    # Update interaction status if it's NEW



    if ix.status == "NEW":



        ix.status = "IN_PROGRESS"



        



    db.session.commit()



    flash("Task added successfully.", "success")



    



    return redirect(url_for("uip_bp.view_interaction", org_slug=org_slug, reference=reference))







@uip_bp.route("/<org_slug>/task/<int:task_id>/complete", methods=["POST"])



@login_required



def complete_task(org_slug, task_id):



    from flask import g



    org = g.organization



    



    task = CoreTask.query.get_or_404(task_id)



    # verify task belongs to org



    if task.interaction.organization_id != org.id:



        abort(403)



        



    task.status = "completed"



    task.completed_at = datetime.utcnow()



    db.session.commit()



    



    flash("Task marked as completed.", "success")



    return redirect(url_for("uip_bp.view_interaction", org_slug=org_slug, reference=task.interaction.reference))







@uip_bp.route("/<org_slug>/interaction/<reference>/resolve", methods=["POST"])



@login_required



def resolve_interaction(org_slug, reference):



    from flask import g



    org = g.organization



    



    ix = CoreInteraction.query.filter_by(organization_id=org.id, reference=reference).first_or_404()



    



    ix.status = "RESOLVED"



    ix.closed_by = current_user.id



    ix.closed_at = datetime.utcnow()



    db.session.commit()



    



    flash("Interaction resolved successfully.", "success")



    return redirect(url_for("uip_bp.view_interaction", org_slug=org_slug, reference=reference))





from app.uip.gateway import LunaGateway



@uip_bp.route("/<org_slug>/interaction/<reference>/summarize", methods=["POST"])

@login_required

def summarize_interaction(org_slug, reference):

    from flask import g

    org = g.organization

    

    ix = CoreInteraction.query.filter_by(organization_id=org.id, reference=reference).first_or_404()

    

    # Check permissions

    assignment = CoreRoleAssignment.query.filter_by(user_id=current_user.id, organization_id=org.id).first()

    if not assignment or assignment.role.slug not in ["manager", "receptionist"]:

        abort(403)

        

    prompt = f"Please summarize this interaction briefly: {ix.description}"

    

    # Pass to AI Gateway

    result = LunaGateway.ask_luna(prompt, interaction_id=ix.id)

    

    if result["status"] == "suspended":

        flash(result["message"], "warning")

    else:

        # Add summary as a task or just flash it for now (simulating UI update)

        summary = result["message"]

        flash(f"Luna Summary: {summary} (Cost: {result['cost_cents']} credits, Remaining: {result['remaining_balance']})", "success")

        

    return redirect(url_for("uip_bp.view_interaction", org_slug=org_slug, reference=reference))


from app.models.uip import UipProvider, UipWorkOrder, UipMunicipalReferral

@uip_bp.route("/<org_slug>/interaction/<reference>/provider", methods=["POST"])
@login_required
def assign_provider(org_slug, reference):
    from flask import g
    org = g.organization
    
    ix = CoreInteraction.query.filter_by(organization_id=org.id, reference=reference).first_or_404()
    
    # Needs manager or committee
    assignment = CoreRoleAssignment.query.filter_by(user_id=current_user.id, organization_id=org.id).first()
    if not assignment or assignment.role.slug not in ["manager", "committee_member"]:
        abort(403)
        
    provider_id = request.form.get("provider_id")
    description = request.form.get("description")
    
    if provider_id:
        wo_ref = f"WO-{ix.reference}-{random.randint(100, 999)}"
        wo = UipWorkOrder(
            interaction_id=ix.id,
            provider_id=provider_id,
            reference=wo_ref,
            description=description,
            status="SENT"
        )
        db.session.add(wo)
        
        if ix.status == "NEW":
            ix.status = "IN_PROGRESS"
            
        db.session.commit()
        flash(f"Work Order {wo_ref} sent to provider.", "success")
        
    return redirect(url_for("uip_bp.view_interaction", org_slug=org_slug, reference=reference))

@uip_bp.route("/<org_slug>/interaction/<reference>/municipal", methods=["POST"])
@login_required
def escalate_municipality(org_slug, reference):
    from flask import g
    org = g.organization
    
    ix = CoreInteraction.query.filter_by(organization_id=org.id, reference=reference).first_or_404()
    
    department = request.form.get("department")
    mun_ref = request.form.get("municipality_reference")
    
    ref_record = UipMunicipalReferral(
        interaction_id=ix.id,
        department=department,
        municipality_reference=mun_ref,
        status="ESCALATED"
    )
    db.session.add(ref_record)
    
    if ix.status == "NEW":
        ix.status = "IN_PROGRESS"
        
    db.session.commit()
    flash(f"Interaction escalated to Municipality ({department}).", "info")
    
    return redirect(url_for("uip_bp.view_interaction", org_slug=org_slug, reference=reference))
