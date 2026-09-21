with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

# 1. Secretary / Standard Vote
old_sec_return = """    if close_msg:
        flash(close_msg, "success")
            
    return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=res.id))"""
    
new_sec_return = """    if close_msg:
        flash(close_msg, "success")
        
    next_res = _get_next_unvoted_resolution(org.id, current_user.id)
    if next_res:
        flash("Auto-advancing to the next unvoted mandate.", "info")
        return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=next_res.id))
    else:
        flash("Inbox Zero! You have successfully cast your vote on all active mandates.", "success")
        return redirect(url_for("uip_bp.committee_dashboard", org_slug=org.slug))"""
        
text = text.replace(old_sec_return, new_sec_return)

# 2. Treasurer Vote
old_tre_return = """    if close_msg:
        flash(close_msg, "success")
        
    return redirect(url_for("uip_bp.treasurer_view_resolution", org_slug=org.slug, res_id=res_id))"""
    
new_tre_return = """    if close_msg:
        flash(close_msg, "success")
        
    next_res = _get_next_unvoted_resolution(org.id, current_user.id)
    if next_res:
        flash("Auto-advancing to the next unvoted mandate.", "info")
        return redirect(url_for("uip_bp.treasurer_view_resolution", org_slug=org.slug, res_id=next_res.id))
    else:
        flash("Inbox Zero! You have successfully cast your vote on all active mandates.", "success")
        return redirect(url_for("uip_bp.treasurer_workspace", org_slug=org.slug))"""

text = text.replace(old_tre_return, new_tre_return)

# 3. Chairman Vote
old_cha_return = """    if close_msg:
        flash(close_msg, "success")
        
    return redirect(url_for("uip_bp.chairman_view_resolution", org_slug=org.slug, res_id=res_id))"""
    
new_cha_return = """    if close_msg:
        flash(close_msg, "success")
        
    next_res = _get_next_unvoted_resolution(org.id, current_user.id)
    if next_res:
        flash("Auto-advancing to the next unvoted mandate.", "info")
        return redirect(url_for("uip_bp.chairman_view_resolution", org_slug=org.slug, res_id=next_res.id))
    else:
        flash("Inbox Zero! You have successfully cast your vote on all active mandates.", "success")
        return redirect(url_for("uip_bp.chairman_workspace", org_slug=org.slug))"""

text = text.replace(old_cha_return, new_cha_return)

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated all 3 return flows")
