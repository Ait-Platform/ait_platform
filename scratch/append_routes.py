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
