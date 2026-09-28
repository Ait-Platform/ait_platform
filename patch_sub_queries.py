import re

filepath = 'app/program_uip/subcomm_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

old_return = '''    return render_template("program_uip/subcomm_tools/board.html", 
        org=org, subcommittee=sub, resolution=resolution, 
        resp_seat=resp_seat, report_seat=report_seat, resp_mem=resp_mem)'''

new_return = '''    from app.models.core import CoreInteraction
    open_queries = CoreInteraction.query.filter_by(
        organization_id=org.id,
        interaction_type="fault_report",
        status="OPEN"
    ).count()

    return render_template("program_uip/subcomm_tools/board.html", 
        org=org, subcommittee=sub, resolution=resolution, 
        resp_seat=resp_seat, report_seat=report_seat, resp_mem=resp_mem,
        open_queries=open_queries)'''

content = content.replace(old_return, new_return)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated subcomm_routes.py with open_queries")
