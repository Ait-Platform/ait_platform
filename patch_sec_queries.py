import re

filepath = 'app/program_uip/secretary_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

old_return = '''    return render_template(
        "program_uip/dashboards/secretary_workspace.html",
        org=org,
        open_claims=enriched_claims,
        switch_gate=switch_gate,
        switch_res=switch_res,
        tabled_res=tabled_res,
        proposed_res=proposed_res,
        first_tabled_res=first_tabled_res
    )'''

new_return = '''    open_queries = CoreInteraction.query.filter_by(
        organization_id=org.id,
        interaction_type="fault_report",
        status="OPEN"
    ).count()

    return render_template(
        "program_uip/dashboards/secretary_workspace.html",
        org=org,
        open_claims=enriched_claims,
        switch_gate=switch_gate,
        switch_res=switch_res,
        tabled_res=tabled_res,
        proposed_res=proposed_res,
        first_tabled_res=first_tabled_res,
        open_queries=open_queries
    )'''

content = content.replace(old_return, new_return)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated secretary_routes.py with open_queries")
