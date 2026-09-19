with open("app/program_uip/finance_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_logic = """    return render("overview", overview=overview,
        major_categories=sorted(overview["budget"], key=lambda b: b["actual"], reverse=True)[:6],
        recent=[f.detail(org, actor, r) for r in overview["transactions"][:10]],
        commitments=[{**f.detail(org, actor, c["row"]), "outstanding": c["outstanding"]} for c in overview["commitments"] if c["outstanding"]])"""

new_logic = """    from app.models.uip_governance import UipCommitteeMember
    from sqlalchemy import func
    mem = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org,
        UipCommitteeMember.status == "CURRENT",
        func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
    ).first()
    
    pos = mem.position.strip().lower() if mem else ""
    recent_tx = [f.detail(org, actor, r) for r in overview["transactions"][:10]]
    if pos == "treasurer":
        return render_template("program_uip/dashboards/treasurer.html", org=g.organization, overview=overview, recent=recent_tx)
        
    return render("overview", overview=overview,
        major_categories=sorted(overview["budget"], key=lambda b: b["actual"], reverse=True)[:6],
        recent=recent_tx,
        commitments=[{**f.detail(org, actor, c["row"]), "outstanding": c["outstanding"]} for c in overview["commitments"] if c["outstanding"]])"""

text = text.replace(old_logic, new_logic)
with open("app/program_uip/finance_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Patched finance_routes.py")
