with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_block = """        if current_quorum_pct < quorum_target:
            flash(f"Cannot adopt: Quorum not met ({current_quorum_pct}% of {quorum_target}% required).", "danger")
            return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=res.id))"""

new_block = """        if current_quorum_pct < quorum_target:
            flash(f"Digital quorum was not met ({current_quorum_pct}% of {quorum_target}%). Proceeding anyway, as final ratification occurs at the live meeting.", "warning")"""

text = text.replace(old_block, new_block)

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Removed strict quorum blocker")
