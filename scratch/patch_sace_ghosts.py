import re

file_path = 'app/program_sace/routes.py'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

# 1) Fix provisioning_map
old_map = '''    # Use admin user 1 as a placeholder for the unauthenticated SACE admin guest
    sace_user_id = current_user.id if current_user.is_authenticated else 1

    # Check if pledged
    pledge = SaceWorkshopInteraction.query.filter_by(user_id=sace_user_id, activity_slug="admin_patent_pledge").first()
    has_pledged = pledge is not None or session.get('sace_admin_pledged', False)
    
    # Load provisioned auditors
    invites = SaceWorkshopInteraction.query.filter_by(user_id=sace_user_id, activity_slug="auditor_provisioned").order_by(SaceWorkshopInteraction.timestamp.desc()).all()'''

new_map = '''    if current_user.is_authenticated:
        pledge = SaceWorkshopInteraction.query.filter_by(user_id=current_user.id, activity_slug="admin_patent_pledge").first()
        has_pledged = pledge is not None or session.get('sace_admin_pledged', False)
        # Load provisioned auditors
        invites = SaceWorkshopInteraction.query.filter_by(user_id=current_user.id, activity_slug="auditor_provisioned").order_by(SaceWorkshopInteraction.timestamp.desc()).all()
    else:
        has_pledged = session.get('sace_admin_pledged', False)
        invites = []'''

text = text.replace(old_map, new_map)

# 2) Fix provisioning_pledge
old_pledge = '''    sace_user_id = current_user.id if current_user.is_authenticated else 1
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

new_pledge = '''    if current_user.is_authenticated:
        pledge = SaceWorkshopInteraction.query.filter_by(user_id=current_user.id, activity_slug="admin_patent_pledge").first()
        if not pledge:
            interaction = SaceWorkshopInteraction(
                user_id=current_user.id,
                activity_slug="admin_patent_pledge",
                response_data="Admin accepted IP pledge"
            )
            db.session.add(interaction)
            from app.models.core import CoreAuditEvent
            ip_addr = request.headers.get('X-Forwarded-For', request.remote_addr)
            audit = CoreAuditEvent(
                user_id=current_user.id,
                action="PLEDGE_ACCEPTED",
                entity_type="SACE_PLEDGE",
                details="Admin accepted IP pledge",
                ip_address=ip_addr
            )
            db.session.add(audit)
            db.session.commit()
            flash("Intellectual Property pledge accepted. Provisioning unlocked.", "success")
    else:
        # Just use the session, don't pollute the DB with user_id=1
        flash("Intellectual Property pledge accepted. Provisioning unlocked.", "success")
        
    return redirect(url_for('sace_bp.provisioning_map'))'''

text = text.replace(old_pledge, new_pledge)

# Let's write it back
with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
