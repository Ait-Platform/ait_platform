with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

# Fix the audit log traceback first
old_audit = """audit.record(org.id, current_user.id, "secretary.resolution_drafted", None)"""
new_audit = """audit.record(org.id, current_user.id, "decision.recorded", None)"""
text = text.replace(old_audit, new_audit)

# Refactor the target logic to support instant verification for ratepayers
old_logic = """    if target == "founding":
        # Find the founding resolution
        founding_res = UipResolution.query.filter_by(organization_id=org.id).filter(UipResolution.title.ilike("%Founding%")).first()
        if founding_res:
            additions = f"\\n\\n-- Added via Inaugural Roster (Term: {term_start} for {term_duration} months) --\\n"
            for claim in claims:"""

new_logic = """    if target == "new" and all(c.interaction_type == "ratepayer_claim" for c in claims):
        target = "instant"

    if target in ["founding", "instant"]:
        founding_res = None
        if target == "founding":
            founding_res = UipResolution.query.filter_by(organization_id=org.id).filter(UipResolution.title.ilike("%Founding%")).first()
            
        if founding_res or target == "instant":
            additions = f"\\n\\n-- Added via Inaugural Roster (Term: {term_start} for {term_duration} months) --\\n"
            for claim in claims:"""

text = text.replace(old_logic, new_logic)

old_commit = """            founding_res.description += additions
            db.session.commit()
            flash("Members successfully officially logged into the Founding Resolution!", "success")
            return redirect(url_for("uip_bp.committee_dashboard", org_slug=org.slug))
            
    # Fallback or "new" resolution logic"""

new_commit = """            if founding_res:
                founding_res.description += additions
            db.session.commit()
            if target == "instant":
                flash("Ratepayers successfully verified and granted access (no resolution required)!", "success")
                return redirect(url_for("uip_bp.secretary_workspace", org_slug=org.slug))
            else:
                flash("Members successfully officially logged into the Founding Resolution!", "success")
                return redirect(url_for("uip_bp.committee_dashboard", org_slug=org.slug))
            
    # Fallback or "new" resolution logic"""

text = text.replace(old_commit, new_commit)

with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Patched secretary_routes.py")
