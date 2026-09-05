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
