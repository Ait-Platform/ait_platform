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

        # 2. Process Committee Members
        emails = request.form.getlist("member_email[]")
        names = request.form.getlist("member_name[]")
        positions = request.form.getlist("member_position[]")
        manager_index_str = request.form.get("manager_index")
        
        try:
            manager_index = int(manager_index_str) if manager_index_str else -1
        except ValueError:
            manager_index = -1
            
        manager_user = None

        from app.models.uip_governance import UipCommitteeTerm, UipCommitteeMember
        
        term = UipCommitteeTerm(
            organization_id=org.id,
            term_name=f"Founding Term ({meeting_date})",
            created_by=submitter_id
        )
        db.session.add(term)
        db.session.flush()

        for idx, (email, name, position) in enumerate(zip(emails, names, positions)):
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
                
            # Add to Committee
            member = UipCommitteeMember(
                term_id=term.id,
                organization_id=org.id,
                name=name,
                email=email,
                position=position,
                status="CURRENT",
                created_by=submitter_id
            )
            db.session.add(member)
            
            if idx == manager_index:
                manager_user = user

        # 3. Process Manager Resolution
        if manager_user:
            resolution_text = (request.form.get("resolution_text") or "").strip()
            if not resolution_text:
                resolution_text = f"Resolution designating {manager_user.name} as Manager."
                
            manager_resolution = UipResolution(
                organization_id=org.id,
                meeting_id=meeting.id,
                title="Manager Designation",
                description=resolution_text,
                recorded_by=submitter_id,
                responsible_user_id=manager_user.id
            )
            db.session.add(manager_resolution)
            
        audit.record(org.id, submitter_id, "founding.provisioned", meeting)
        
        db.session.commit()
        
        flash("Founding committee successfully provisioned.", "success")
        return redirect(url_for("uip_bp.router_page", org_slug=org.slug))

    return render_template("uip/provisioning.html", org=org)
