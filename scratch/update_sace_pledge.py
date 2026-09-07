import re

routes_path = 'app/program_sace/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    text = f.read()

# Replace provisioning_map logic
old_map = '''    # Use admin user 1 as a placeholder for the unauthenticated SACE admin guest
    sace_user_id = current_user.id if current_user.is_authenticated else 1

    # Check if pledged
    pledge = SaceWorkshopInteraction.query.filter_by(user_id=sace_user_id, activity_slug="admin_patent_pledge").first()
    has_pledged = pledge is not None
    
    # Load provisioned auditors
    invites = SaceWorkshopInteraction.query.filter_by(user_id=sace_user_id, activity_slug="auditor_provisioned").order_by(SaceWorkshopInteraction.timestamp.desc()).all()'''

new_map = '''    # If they are logged in and have a session pledge, save it to DB now
    if current_user.is_authenticated and session.get('sace_admin_pledged'):
        existing = SaceWorkshopInteraction.query.filter_by(user_id=current_user.id, activity_slug="admin_patent_pledge").first()
        if not existing:
            interaction = SaceWorkshopInteraction(
                user_id=current_user.id,
                activity_slug="admin_patent_pledge",
                response_data="Admin accepted IP pledge"
            )
            db.session.add(interaction)
            db.session.commit()
        session.pop('sace_admin_pledged', None)

    # Use admin user 1 as a placeholder for the unauthenticated SACE admin guest
    sace_user_id = current_user.id if current_user.is_authenticated else 1

    # Check if pledged
    pledge = SaceWorkshopInteraction.query.filter_by(user_id=sace_user_id, activity_slug="admin_patent_pledge").first()
    has_pledged = pledge is not None or session.get('sace_admin_pledged', False)
    
    # Load provisioned auditors
    invites = SaceWorkshopInteraction.query.filter_by(user_id=sace_user_id, activity_slug="auditor_provisioned").order_by(SaceWorkshopInteraction.timestamp.desc()).all()'''

text = text.replace(old_map, new_map)

# Replace provisioning_pledge logic
old_pledge = '''def provisioning_pledge():
    from app.models.sace import SaceWorkshopInteraction
    sace_user_id = current_user.id if current_user.is_authenticated else 1
    pledge = SaceWorkshopInteraction.query.filter_by(user_id=sace_user_id, activity_slug="admin_patent_pledge").first()
    if not pledge:
        interaction = SaceWorkshopInteraction(
            user_id=sace_user_id,
            activity_slug="admin_patent_pledge",
            response_data="Admin accepted IP pledge"
        )
        db.session.add(interaction)
        db.session.commit()
        from app.models.core import CoreAuditEvent
        ip_addr = request.headers.get('X-Forwarded-For', request.remote_addr)
        audit = CoreAuditEvent(
            user_id=sace_user_id,
            action="PLEDGE_ACCEPTED",
            entity_type="SACE_PLEDGE",
            details="Admin accepted IP pledge",
            ip_address=ip_addr
        )
        db.session.add(audit)
        db.session.commit()
        flash("Intellectual Property pledge accepted. Provisioning unlocked.", "success")
    return redirect(url_for('sace_bp.provisioning_map'))'''

new_pledge = '''def provisioning_pledge():
    from app.models.sace import SaceWorkshopInteraction
    
    # Save to session so it survives registration
    session['sace_admin_pledged'] = True
    
    sace_user_id = current_user.id if current_user.is_authenticated else 1
    pledge = SaceWorkshopInteraction.query.filter_by(user_id=sace_user_id, activity_slug="admin_patent_pledge").first()
    if not pledge:
        interaction = SaceWorkshopInteraction(
            user_id=sace_user_id,
            activity_slug="admin_patent_pledge",
            response_data="Admin accepted IP pledge"
        )
        db.session.add(interaction)
        db.session.commit()
        from app.models.core import CoreAuditEvent
        ip_addr = request.headers.get('X-Forwarded-For', request.remote_addr)
        audit = CoreAuditEvent(
            user_id=sace_user_id,
            action="PLEDGE_ACCEPTED",
            entity_type="SACE_PLEDGE",
            details="Admin accepted IP pledge",
            ip_address=ip_addr
        )
        db.session.add(audit)
        db.session.commit()
        flash("Intellectual Property pledge accepted. Provisioning unlocked.", "success")
    return redirect(url_for('sace_bp.provisioning_map'))'''

text = text.replace(old_pledge, new_pledge)

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(text)
