with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

import re

# Update chairman_view_resolution
c_old = """return render_template(
        "program_uip/dashboards/chairman_resolution_view.html",
        org=org,
        resolution=res,
        votes=votes,
        has_voted=has_voted,
        yea_count=yea_count,
        nay_count=nay_count,
        abstain_count=abstain_count
    )"""
c_new = """return render_template(
        "program_uip/dashboards/resolution_view.html",
        org=org,
        resolution=res,
        votes=votes,
        has_voted=has_voted,
        yea_count=yea_count,
        nay_count=nay_count,
        abstain_count=abstain_count,
        back_url=url_for('uip_bp.chairman_voting_room', org_slug=org.slug),
        back_text='Back to Voting & Mandates'
    )"""
text = text.replace(c_old, c_new)

# Update treasurer_view_resolution
t_old = """return render_template(
        "program_uip/dashboards/treasurer_resolution_view.html",
        org=org,
        resolution=res,
        votes=votes,
        has_voted=has_voted,
        yea_count=yea_count,
        nay_count=nay_count,
        abstain_count=abstain_count
    )"""
t_new = """return render_template(
        "program_uip/dashboards/resolution_view.html",
        org=org,
        resolution=res,
        votes=votes,
        has_voted=has_voted,
        yea_count=yea_count,
        nay_count=nay_count,
        abstain_count=abstain_count,
        back_url=url_for('uip_bp.treasurer_voting_room', org_slug=org.slug),
        back_text='Back to Voting & Mandates'
    )"""
text = text.replace(t_old, t_new)

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)

print("Updated committee_routes.py to use universal resolution_view.html")
