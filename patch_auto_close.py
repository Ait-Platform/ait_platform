with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

import re

helper_fn = """
def _check_auto_close(org, res, db):
    from app.models.uip_governance import UipCommitteeMember
    from app.program_uip.secretary_routes import execute_resolution_adoption
    
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
        yea_count = len([v for v in votes if v.vote == 'YEA'])
        nay_count = len([v for v in votes if v.vote == 'NAY'])
        
        if yea_count > nay_count:
            execute_resolution_adoption(org, res, db)
            return f"Voting concluded automatically! 100% participation reached. Resolution ADOPTED."
        else:
            res.status = "REJECTED"
            db.session.commit()
            return f"Voting concluded automatically! 100% participation reached. Resolution REJECTED."
    return None
"""

if "def _check_auto_close" not in text:
    text = helper_fn + text

# Now replace the inline auto close block in vote_resolution with the helper
auto_close_block = r'# --- AUTO-CLOSE LOGIC ---.*?flash\(f"Voting concluded automatically! 100% participation reached. Resolution REJECTED.", "success"\)'

replacement = """# --- AUTO-CLOSE LOGIC ---
    close_msg = _check_auto_close(org, res, db)
    if close_msg:
        flash(close_msg, "success")"""

text = re.sub(auto_close_block, replacement, text, flags=re.DOTALL)

# Add the helper check to chairman_vote_resolution and treasurer_vote_resolution
# Locate db.session.commit() before the return redirect
c_commit = """        db.session.add(new_vote)
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
text = text.replace(c_commit, c_new)


t_commit = """        db.session.add(new_vote)
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
text = text.replace(t_commit, t_new)


with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)

print("Injected auto-close logic into all vote routes")
