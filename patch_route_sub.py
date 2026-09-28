import re

filepath = 'app/program_uip/operational_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. In reception_issue POST handling:
old_post = '''        elif action == "task":
            task = operations.add_task(org, actor, issue_id, request.form.get("title"), request.form.get("description"))
            if request.form.get("due"):
                task.due_date = reception.timestamp(request.form["due"], True).replace(tzinfo=None)'''

new_post = '''        elif action == "task":
            task = operations.add_task(org, actor, issue_id, request.form.get("title"), request.form.get("description"))
            if request.form.get("due"):
                task.due_date = reception.timestamp(request.form["due"], True).replace(tzinfo=None)
        elif action == "route_subcom":
            sub_id = request.form.get("subcommittee_id")
            if sub_id:
                from app.models.uip_governance import UipSubcommittee
                sub = UipSubcommittee.query.filter_by(id=sub_id, organization_id=org).first()
                if sub:
                    # Assign a task to the subcom to review this issue
                    from app.program_uip.services.subcommittees import resolve_responsible_member
                    resp_mem = resolve_responsible_member(sub)
                    task = operations.add_task(org, actor, issue_id, f"Subcommittee Review: {sub.name}", "Please review and manage this routed query.")
                    if resp_mem:
                        task.assignee_id = resp_mem.user_id
                    issue.status = "IN_PROGRESS"
                    from app.models.uip import UipCommunicationLog
                    comm = UipCommunicationLog(organization_id=org, interaction_id=issue_id, channel="WEB", party_classification="STAFF", purpose="DISPATCH", status="RECORDED", summary=f"Routed to {sub.name} Subcommittee.")
                    db.session.add(comm)'''
content = content.replace(old_post, new_post)

# 2. Add the form to the GET request
old_forms = '''        form("Create internal task", "task", [field("title", "Title"), field("description", "Description", kind="textarea", required=False),
            field("due", "Due date/time with UTC offset", required=False)]),'''

new_forms = '''        form("Create internal task", "task", [field("title", "Title"), field("description", "Description", kind="textarea", required=False),
            field("due", "Due date/time with UTC offset", required=False)]),
        form("Hand off to Subcommittee", "route_subcom", [
            field("subcommittee_id", "Select Subcommittee", [(s.id, s.name) for s in __import__("app").models.uip_governance.UipSubcommittee.query.filter_by(organization_id=org, status="ACTIVE").all()])
        ]),'''
content = content.replace(old_forms, new_forms)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated operational_routes for routing")
