import re
with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

# Add auto-close logic at the end of vote_resolution
old_end = """        db.session.add(new_vote)
        flash("Your secure vote has been cast.", "success")
        
    db.session.commit()
    return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=res.id))"""

new_end = """        db.session.add(new_vote)
        flash("Your secure vote has been cast.", "success")
        
    db.session.commit()
    
    # --- AUTO-CLOSE LOGIC ---
    # Check if 100% of eligible voters have cast their votes.
    votes = res.votes.all() if hasattr(res, 'votes') else []
    total_eligible = 0
    if scope in ['EXCO', 'EXCO_CORE', 'COMMITTEE_ALL', 'SUB_COMMITTEE']:
        if scope == 'EXCO_CORE':
            total_eligible = UipCommitteeMember.query.filter(
                UipCommitteeMember.organization_id == org.id, 
                UipCommitteeMember.status == "CURRENT",
                UipCommitteeMember.position.in_(["Chairperson", "Vice-Chairperson", "Secretary", "Treasurer"])
            ).count()
        else:
            total_eligible = UipCommitteeMember.query.filter_by(organization_id=org.id, status="CURRENT").count()
    else:
        from app.models.core import CoreOrganizationMember
        total_eligible = CoreOrganizationMember.query.filter_by(organization_id=org.id, is_active=True).count()
        
    if total_eligible > 0 and len(votes) >= total_eligible:
        # Everyone voted! Auto-close the resolution.
        yea_count = len([v for v in votes if v.vote == 'YEA'])
        nay_count = len([v for v in votes if v.vote == 'NAY'])
        
        if yea_count > nay_count:
            res.status = "ADOPTED"
            flash(f"Voting concluded automatically! 100% participation reached. Resolution ADOPTED ({yea_count} YEA / {nay_count} NAY).", "success")
            
            # Grant roles if it's an Access Bundle
            if res.result_basis and res.result_basis.get("type") == "access_bundle":
                from app.models.core import CoreInteraction, CoreOrganizationMember, CoreRoleAssignment, CoreRole
                claims = CoreInteraction.query.filter(
                    CoreInteraction.organization_id == org.id,
                    CoreInteraction.id.in_(res.result_basis.get("claim_ids", []))
                ).all()
                for claim in claims:
                    # Smart merge logic... wait, I can't easily duplicate the access_bundle logic here.
                    # It's better to just mark it ADOPTED and let the user do the access bundle manually?
                    # Or I can just redirect them with the flash message.
                    pass # Let's handle this carefully.
                    
        else:
            res.status = "REJECTED"
            flash(f"Voting concluded automatically! 100% participation reached. Resolution REJECTED ({yea_count} YEA / {nay_count} NAY).", "warning")
            
        db.session.commit()
    # ------------------------
    
    return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=res.id))"""

# Actually, wait. The Smart Merge logic inside decide_resolution is huge and complex. 
# If it auto-adopts, it skips the Smart Merge logic entirely!
