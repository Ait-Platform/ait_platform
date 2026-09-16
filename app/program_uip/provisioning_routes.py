import os
from flask import render_template, request, g, redirect, url_for, flash
from flask_login import current_user
from datetime import datetime, timezone

from app.extensions import db
from app.models.auth import User
from app.models.core import CoreOrganizationMember
from app.models.uip import UipCommitteeMeeting, UipResolution
from app.models.uip_governance import UipCommitteeTerm, UipCommitteeMember

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
        venue = (request.form.get("venue") or "").strip()
        meeting_date = request.form.get("meeting_date")
        meeting_time = request.form.get("meeting_time")
        
        if not venue or not meeting_date or not meeting_time:
            flash("Meeting venue, date, and time are required.", "danger")
            return redirect(request.url)
            
        try:
            dt_str = f"{meeting_date}T{meeting_time}:00"
            scheduled_at = datetime.fromisoformat(dt_str).replace(tzinfo=timezone.utc)
        except ValueError:
            flash("Invalid date or time.", "danger")
            return redirect(request.url)

        submitter_id = getattr(current_user, 'id', None)

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

        # 2. Create the First Term
        term = UipCommitteeTerm(
            organization_id=org.id,
            term_name=f"Founding Term ({meeting_date})",
            created_by=submitter_id
        )
        db.session.add(term)
        db.session.flush()

        # 3. Add the current user as the Founding Secretary
        # Ensure they have an active Core membership
        membership = CoreOrganizationMember.query.filter_by(
            organization_id=org.id, user_id=current_user.id
        ).first()
        
        if not membership:
            membership = CoreOrganizationMember(
                organization_id=org.id, 
                user_id=current_user.id, 
                is_active=True
            )
            db.session.add(membership)
        else:
            membership.is_active = True
            
        db.session.flush()
            
        # Add to Committee as Secretary
        member = UipCommitteeMember(
            organization_id=org.id,
            term_id=term.id,
            email=current_user.email,
            name=current_user.name or current_user.email.split('@')[0],
            position="Secretary",
            status="CURRENT",
            created_by=submitter_id
        )
        db.session.add(member)

        # Also create a Founding Resolution
        res = UipResolution(
            organization_id=org.id,
            meeting_id=meeting.id,
            title=f"Resolution {datetime.now().year}-1 - Founding Declaration",
            description=f"Inaugural setup of the {org.name} committee and official appointment of the Secretary.",
            status="APPROVED",
            recorded_by=submitter_id
        )
        db.session.add(res)
        
        
        db.session.commit()
        flash("Provisional setup complete! The inaugural meeting and your Secretary appointment have been officially recorded.", "success")
        return redirect(url_for("uip_bp.committee_dashboard", org_slug=org.slug))

    # GET request - just render the form
    return render_template("program_uip/provisioning.html", org=org)

