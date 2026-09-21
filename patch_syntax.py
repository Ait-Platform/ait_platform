with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

import re

# Fix chairman
text = re.sub(r"vote_url=url_for\('uip_bp.chairman_vote_resolution', org_slug=org.slug, res_id=res.id\),\s*vote_url=url_for\('uip_bp.chairman_vote_resolution', org_slug=org.slug, res_id=res.id\)",
              r"vote_url=url_for('uip_bp.chairman_vote_resolution', org_slug=org.slug, res_id=res.id)", text)

# Fix treasurer
text = re.sub(r"vote_url=url_for\('uip_bp.treasurer_vote_resolution', org_slug=org.slug, res_id=res.id\),\s*vote_url=url_for\('uip_bp.treasurer_vote_resolution', org_slug=org.slug, res_id=res.id\)",
              r"vote_url=url_for('uip_bp.treasurer_vote_resolution', org_slug=org.slug, res_id=res.id)", text)

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)

print("Fixed syntax error")
