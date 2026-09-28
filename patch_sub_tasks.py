import re

filepath = 'app/program_uip/subcomm_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

old_tasks = '''@uip_bp.route("/<org_slug>/subcommittee/<int:sub_id>/tasks")
@login_required
def sub_comm_tasks(org_slug, sub_id):
    org = g.organization
    sub = sub_service.require_subcommittee_membership(org.id, current_user.id, sub_id)
    return render_template("program_uip/subcomm_tools/shell.html", org=org, subcommittee=sub, title="Tasks & Actions", message="Existing task architecture lacks subcommittee assignment relationship.")'''

new_tasks = '''@uip_bp.route("/<org_slug>/subcommittee/<int:sub_id>/tasks")
@login_required
def sub_comm_tasks(org_slug, sub_id):
    org = g.organization
    sub = sub_service.require_subcommittee_membership(org.id, current_user.id, sub_id)
    
    from app.models.core import CoreTask, CoreInteraction
    from app.program_uip.services.subcommittees import resolve_responsible_member
    resp_mem = resolve_responsible_member(sub)
    
    tasks = []
    if resp_mem and resp_mem.user_id:
        tasks = CoreTask.query.join(CoreInteraction).filter(
            CoreInteraction.organization_id == org.id,
            CoreTask.assignee_id == resp_mem.user_id,
            CoreTask.status != "COMPLETED"
        ).all()
        
    return render_template("program_uip/subcomm_tools/tasks.html", org=org, subcommittee=sub, tasks=tasks)'''

content = content.replace(old_tasks, new_tasks)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated subcomm tasks route")
