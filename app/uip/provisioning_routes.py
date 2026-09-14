from flask import render_template, request, g, redirect, url_for, flash
from flask_login import current_user
from datetime import datetime, timezone

from app.extensions import db
from app.models.auth import User
from app.models.core import CoreOrganizationMember
from app.models.uip import UipCommitteeMeeting, UipResolution

from . import uip_bp
from .services import audit

@uip_bp.route("/<org_slug>/provisioning", methods=["GET", "POST"])
def provisioning(org_slug):
    org = g.organization
    
    # Restrict if a founding meeting already exists
    if UipCommitteeMeeting.query.filter_by(organization_id=org.id, meeting_type="FOUNDING").first():
        flash("This organisation has already been provisioned.", "info")
        return redirect(url_for("uip_bp.router_page", org_slug=org.slug))

    if request.method == "POST":
        org_name = (request.form.get("org_name") or "").strip()
        venue = (request.form.get("venue") or "").strip()
        meeting_date = request.form.get("meeting_date")
        meeting_time = request.form.get("meeting_time")
        
        if not org_name or not venue or not meeting_date or not meeting_time:
            flash("Organisation and meeting details are required.", "danger")
            return redirect(request.url)
            
        try:
            dt_str = f"{meeting_date}T{meeting_time}:00"
            scheduled_at = datetime.fromisoformat(dt_str).replace(tzinfo=timezone.utc)
        except ValueError:
            flash("Invalid date or time.", "danger")
            return redirect(request.url)

        submitter_id = getattr(current_user, 'id', None)

        # Update Org Name (but not slug)
        org.name = org_name

        # 1. Create the Founding Meeting
        meeting = UipCommitteeMeeting(
            organization_id=org.id,
            title="Founding AGM",
            meeting_type="FOUNDING",
            scheduled_at=scheduled_at,
            location=venue,
            status="CONCLUDED"
        )
        db.session.add(meeting)
        db.session.flush()

        # 2. Process Committee Members & Close Claims
        admit_claim_ids = request.form.getlist("admit_claim_id[]")
        
        from app.models.uip_governance import UipCommitteeTerm, UipCommitteeMember
        from app.models.core import CoreInteraction
        
        term = UipCommitteeTerm(
            organization_id=org.id,
            term_name=f"Founding Term ({meeting_date})",
            created_by=submitter_id
        )
        db.session.add(term)
        db.session.flush()

        for claim_id in admit_claim_ids:
            email = (request.form.get(f"member_email_{claim_id}") or "").strip()
            name = (request.form.get(f"member_name_{claim_id}") or "").strip()
            position = (request.form.get(f"member_position_{claim_id}") or "").strip()
            
            if not email or not name:
                continue

            # Close claim
            claim = CoreInteraction.query.get(claim_id)
            if claim and claim.interaction_type in ("committee_claim", "secretary_claim"):
                claim.status = "CLOSED"
                claim.closed_by = submitter_id

            # Create a pending User account for the application login
            user = User.query.filter_by(email=email).first()
            if not user:
                user = User(name=name, email=email, is_active=0)
                db.session.add(user)
                db.session.flush()

            # Create a core membership that is pending activation
            membership = CoreOrganizationMember.query.filter_by(
                organization_id=org.id, user_id=user.id
            ).first()
            
            if not membership:
                membership = CoreOrganizationMember(
                    organization_id=org.id, 
                    user_id=user.id, 
                    is_active=False
                )
                db.session.add(membership)
                db.session.flush()
                
            # Add to Committee
            member = UipCommitteeMember(
                organization_id=org.id,
                term_id=term.id,
                email=email,
                name=name,
                position=position,
                status="CURRENT",
                created_by=submitter_id
            )
            db.session.add(member)
            
        # Also close the Pioneer's own claims if they had any
        pioneer_claims = CoreInteraction.query.filter(
            CoreInteraction.organization_id == org.id,
            CoreInteraction.creator_id == submitter_id,
            CoreInteraction.interaction_type.in_(["committee_claim", "secretary_claim"]),
            CoreInteraction.status == "OPEN"
        ).all()
        for pc in pioneer_claims:
            pc.status = "CLOSED"
            pc.closed_by = submitter_id

        # 3. Create a complimentary active entitlement so the UIP doesn't instantly dead-end
        from app.models.core import CoreOrganizationEntitlement
        from app.models.auth import AuthSubject
        uip_subj = AuthSubject.query.filter_by(slug='uip').first()
        if uip_subj:
            ent = CoreOrganizationEntitlement.query.filter_by(organization_id=org.id, subject_id=uip_subj.id).first()
            if not ent:
                ent = CoreOrganizationEntitlement(organization_id=org.id, subject_id=uip_subj.id, status="active", is_trial=True)
                db.session.add(ent)

        db.session.commit()
        
        flash("Founding committee successfully provisioned.", "success")
        return redirect(url_for("uip_bp.router_page", org_slug=org.slug))

    from app.models.core import CoreInteraction
    claims = CoreInteraction.query.filter(
        CoreInteraction.organization_id == org.id,
        CoreInteraction.interaction_type.in_(["committee_claim", "secretary_claim"]),
        CoreInteraction.status == "OPEN"
    ).all()

    return render_template("uip/provisioning.html", org=org, claims=claims)
