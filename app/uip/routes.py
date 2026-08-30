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

@uip_bp.route("/<org_slug>/reports")
@login_required
def org_reports(org_slug):
    from flask import g
    org = g.organization
    
    assignment = CoreRoleAssignment.query.filter_by(user_id=current_user.id, organization_id=org.id).first()
    if not assignment or assignment.role.slug not in ["manager", "committee_member"]:
        abort(403)
        
    return render_template("uip/dashboards/reports.html", org=org)

@uip_bp.route("/<org_slug>/reports/generate_ai", methods=["POST"])
@login_required
def generate_ai_report(org_slug):
    from flask import g
    from app.uip.gateway import LunaGateway
    org = g.organization
    
    # Compile raw data for Luna
    open_ix = CoreInteraction.query.filter(CoreInteraction.organization_id == org.id, CoreInteraction.status != 'RESOLVED').count()
    completed_wo = 0 # Dummy data for prototype
    
    prompt = f"Write a 3-sentence executive summary for the committee. Current metrics: {open_ix} open interactions, {completed_wo} completed work orders."
    
    result = LunaGateway.ask_luna(prompt)
    if result["status"] == "suspended":
        flash(result["message"], "warning")
    else:
        flash(f"Luna Management Summary: {result['message']} (Cost: {result['cost_cents']} AIT)", "success")
        
    return redirect(url_for("uip_bp.org_reports", org_slug=org_slug))
f r o m   f l a s k   i m p o r t   r e d i r e c t ,   u r l _ f o r  
 f r o m   a p p . u i p   i m p o r t   u i p _ b p  
  
 @ u i p _ b p . r o u t e ( " / " )  
 d e f   u i p _ s t a r t ( ) :  
         #   A   s i m p l e   l a n d i n g / e n t r y   r o u t e   t h a t   r e d i r e c t s   t o   t h e   d e m o   t e n a n t   f o r   n o w .  
         #   W e   w i l l   a s s u m e   ' m a n o r - g a r d e n s '   s i n c e   w e   s e e d e d   i t   e a r l i e r .  
         r e t u r n   r e d i r e c t ( u r l _ f o r ( ' u i p _ b p . d a s h b o a r d ' ,   o r g _ s l u g = ' m a n o r - g a r d e n s ' ) )  
 f r o m   f l a s k   i m p o r t   f l a s h ,   r e d i r e c t ,   u r l _ f o r  
  
 @ u i p _ b p . r o u t e ( " / _ s e e d " )  
 d e f   s e e d _ u i p _ l i v e ( ) :  
         f r o m   a p p . e x t e n s i o n s   i m p o r t   d b  
         f r o m   a p p . m o d e l s . a u t h   i m p o r t   A u t h S u b j e c t  
         s u b j   =   A u t h S u b j e c t . q u e r y . f i l t e r _ b y ( s l u g = ' u i p ' ) . f i r s t ( )  
         i f   n o t   s u b j :  
                 s u b j   =   A u t h S u b j e c t (  
                         s l u g = ' u i p ' ,  
                         n a m e = ' U I P   P l a t f o r m ' ,  
                         p r o g r a m _ t y p e = ' B 2 B ' ,  
                         s h o w _ o n _ w e l c o m e = T r u e ,  
                         a b o u t _ e n d p o i n t = ' u i p _ b p . u i p _ s t a r t ' ,  
                         p r o c e s s o r _ d e f a u l t = ' y o c o '  
                 )  
                 d b . s e s s i o n . a d d ( s u b j )  
         e l s e :  
                 s u b j . s h o w _ o n _ w e l c o m e   =   T r u e  
                 s u b j . a b o u t _ e n d p o i n t   =   ' u i p _ b p . u i p _ s t a r t '  
                 s u b j . p r o c e s s o r _ d e f a u l t   =   ' y o c o '  
         d b . s e s s i o n . c o m m i t ( )  
         f l a s h ( " U I P   m o d u l e   s e e d e d   i n t o   l i v e   d a t a b a s e ! " ,   " s u c c e s s " )  
         r e t u r n   r e d i r e c t ( u r l _ f o r ( ' a d m i n _ b p . m o d u l e s _ c o n t r o l ' ) )  
 