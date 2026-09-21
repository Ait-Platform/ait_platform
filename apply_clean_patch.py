with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

import re

# 1. Add the helper function
helper_fn = """
def _check_auto_close(org, res, db):
    from app.models.uip_governance import UipCommitteeMember
    
    scope = getattr(res, 'voting_scope', 'EXCO')
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
        # Digital voting is purely a temperature check.
        # Regardless of the outcome, the resolution MUST transition to a live meeting (TABLED)
        res.status = "TABLED"
        db.session.commit()
        return f"100% participation reached! Digital voting concluded. Resolution is now TABLED for the live meeting."
    return None
"""
if "def _check_auto_close" not in text:
    text = helper_fn + "\n" + text

# 2. Replace the old auto-close in vote_resolution
old_auto = """    # --- AUTO-CLOSE LOGIC ---
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
        yea_count = len([v for v in votes if v.vote == 'YEA'])
        nay_count = len([v for v in votes if v.vote == 'NAY'])
        
        if yea_count > nay_count:
            execute_resolution_adoption(org, res, db)
            db.session.commit()
            flash(f"Voting concluded automatically! 100% participation reached. Resolution ADOPTED.", "success")
        else:
            res.status = "REJECTED"
            db.session.commit()
            flash(f"Voting concluded automatically! 100% participation reached. Resolution REJECTED.", "success")"""

new_auto = """    # --- AUTO-CLOSE LOGIC ---
    close_msg = _check_auto_close(org, res, db)
    if close_msg:
        flash(close_msg, "success")"""

text = text.replace(old_auto, new_auto)

# 3. Add auto-close to chairman and treasurer
c_old = """        db.session.add(new_vote)
        flash("Your vote has been recorded successfully.", "success")
        
    db.session.commit()
    
    return redirect(url_for("uip_bp.chairman_view_resolution", org_slug=org.slug, res_id=res_id))"""
c_new = """        db.session.add(new_vote)
        flash("Your vote has been recorded successfully.", "success")
        
    db.session.commit()
    
    close_msg = _check_auto_close(org, res, db)
    if close_msg:
        flash(close_msg, "success")
        
    return redirect(url_for("uip_bp.chairman_view_resolution", org_slug=org.slug, res_id=res_id))"""
text = text.replace(c_old, c_new)


t_old = """        db.session.add(new_vote)
        flash("Your vote has been recorded successfully.", "success")
        
    db.session.commit()
    return redirect(url_for("uip_bp.treasurer_view_resolution", org_slug=org.slug, res_id=res_id))"""
t_new = """        db.session.add(new_vote)
        flash("Your vote has been recorded successfully.", "success")
        
    db.session.commit()
    
    close_msg = _check_auto_close(org, res, db)
    if close_msg:
        flash(close_msg, "success")
        
    return redirect(url_for("uip_bp.treasurer_view_resolution", org_slug=org.slug, res_id=res_id))"""
text = text.replace(t_old, t_new)


# 4. Fix Tile 5 Dashboard variables for Chairman & Treasurer
ch_dash_old = """    tabled_res = UipResolution.query.filter_by(organization_id=org.id, status="PROPOSED").count()
    proposed_res = UipResolution.query.filter_by(organization_id=org.id, status="DRAFT").count()
    
    if tabled_res > 0:
        switch_res = 'red'
    elif proposed_res > 0:
        switch_res = 'amber'
    else:
        switch_res = 'clear'
        
    return render_template(
        "program_uip/dashboards/chairman_workspace.html",
        org=org,
        open_claims=enriched_claims,
        switch_gate=switch_gate,
        switch_res=switch_res,
        tabled_res=tabled_res,
        proposed_res=proposed_res
    )"""
ch_dash_new = """    first_tabled_res = UipResolution.query.filter_by(organization_id=org.id, status="TABLED").first()
    tabled_res = UipResolution.query.filter_by(organization_id=org.id, status="TABLED").count()
    proposed_res = UipResolution.query.filter_by(organization_id=org.id, status="PROPOSED").count()
    
    if tabled_res > 0:
        switch_res = 'red'
    elif proposed_res > 0:
        switch_res = 'amber'
    else:
        switch_res = 'clear'
        
    return render_template(
        "program_uip/dashboards/chairman_workspace.html",
        org=org,
        open_claims=enriched_claims,
        switch_gate=switch_gate,
        switch_res=switch_res,
        tabled_res=tabled_res,
        proposed_res=proposed_res,
        first_tabled_res=first_tabled_res
    )"""
text = text.replace(ch_dash_old, ch_dash_new)


tr_dash_old = """    tabled_res = UipResolution.query.filter_by(organization_id=org.id, status="PROPOSED").count()
    proposed_res = UipResolution.query.filter_by(organization_id=org.id, status="DRAFT").count()
    
    if tabled_res > 0:
        switch_res = 'red'
    elif proposed_res > 0:
        switch_res = 'amber'
    else:
        switch_res = 'clear'
        
    return render_template(
        "program_uip/dashboards/treasurer_workspace.html",
        org=org,
        open_claims=enriched_claims,
        switch_gate=switch_gate,
        switch_res=switch_res,
        tabled_res=tabled_res,
        proposed_res=proposed_res
    )"""
tr_dash_new = """    first_tabled_res = UipResolution.query.filter_by(organization_id=org.id, status="TABLED").first()
    tabled_res = UipResolution.query.filter_by(organization_id=org.id, status="TABLED").count()
    proposed_res = UipResolution.query.filter_by(organization_id=org.id, status="PROPOSED").count()
    
    if tabled_res > 0:
        switch_res = 'red'
    elif proposed_res > 0:
        switch_res = 'amber'
    else:
        switch_res = 'clear'
        
    return render_template(
        "program_uip/dashboards/treasurer_workspace.html",
        org=org,
        open_claims=enriched_claims,
        switch_gate=switch_gate,
        switch_res=switch_res,
        tabled_res=tabled_res,
        proposed_res=proposed_res,
        first_tabled_res=first_tabled_res
    )"""
text = text.replace(tr_dash_old, tr_dash_new)


with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)

print("Re-applied all fixes cleanly.")
