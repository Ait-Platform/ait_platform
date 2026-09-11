from flask import render_template, request, g, redirect, url_for, flash, abort
from flask_login import login_required, current_user
from datetime import datetime, timezone

from app.extensions import db
from app.models.auth import User
from app.models.core import CoreOrganizationMember
from app.models.uip import UipCommitteeMeeting, UipResolution

from . import uip_bp
from .services import audit

@uip_bp.route("/<org_slug>/founding", methods=["GET", "POST"])
@login_required
def founding(org_slug):
    org = g.organization
    
    # Identify the trusted setup user as the chronologically first member of the organization
    first_member = CoreOrganizationMember.query.filter_by(
        organization_id=org.id
    ).order_by(CoreOrganizationMember.id).first()
    
    if not first_member or first_member.user_id != current_user.id:
        abort(403, description="Only the initial setup user can perform founding provisioning.")
    
    # Restrict if a founding meeting already exists
    if UipCommitteeMeeting.query.filter_by(organization_id=org.id, meeting_type="FOUNDING").first():
        flash("Founding meeting has already been recorded.", "info")
        return redirect(url_for("uip_bp.dashboard", org_slug=org.slug))

    if request.method == "POST":
        venue = (request.form.get("venue") or "").strip()
        meeting_date = request.form.get("meeting_date")
        meeting_time = request.form.get("meeting_time")
        
        if not venue or not meeting_date or not meeting_time:
            flash("Meeting details are required.", "danger")
            return redirect(request.url)
            
        try:
            dt_str = f"{meeting_date}T{meeting_time}:00"
            scheduled_at = datetime.fromisoformat(dt_str).replace(tzinfo=timezone.utc)
        except ValueError:
            flash("Invalid date or time.", "danger")
            return redirect(request.url)

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

        # 2. Process Committee Members
        emails = request.form.getlist("member_email[]")
        names = request.form.getlist("member_name[]")
        manager_index_str = request.form.get("manager_index")
        
        try:
            manager_index = int(manager_index_str) if manager_index_str else -1
        except ValueError:
            manager_index = -1
            
        manager_user = None
        elected_user_ids = []

        for idx, (email, name) in enumerate(zip(emails, names)):
            email = email.strip()
            name = name.strip()
            if not email or not name:
                continue
                
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
            
            elected_user_ids.append(user.id)
            
            if idx == manager_index:
                manager_user = user

        # 3. Process Committee Election Resolution
        if elected_user_ids:
            election_resolution = UipResolution(
                organization_id=org.id,
                meeting_id=meeting.id,
                title="Election of Committee Members",
                description="The following individuals were elected to the committee at the founding meeting.",
                recorded_by=current_user.id,
                result_basis={"elected_committee_user_ids": elected_user_ids}
            )
            db.session.add(election_resolution)

        # 4. Process Manager Resolution
        if manager_user:
            resolution_text = (request.form.get("resolution_text") or "").strip()
            if not resolution_text:
                resolution_text = f"Resolution designating {manager_user.name} as Manager."
                
            manager_resolution = UipResolution(
                organization_id=org.id,
                meeting_id=meeting.id,
                title="Manager Designation",
                description=resolution_text,
                recorded_by=current_user.id,
                responsible_user_id=manager_user.id
            )
            db.session.add(manager_resolution)
            
        audit.record(org.id, current_user.id, "founding.provisioned", meeting)
        db.session.commit()
        
        flash("Founding committee successfully provisioned.", "success")
        return redirect(url_for("uip_bp.dashboard", org_slug=org.slug))

    return render_template("uip/founding.html", org=org)
