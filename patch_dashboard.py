import sys

with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    c = f.read()

c = c.replace(
    'return redirect(url_for("uip_bp.work_order_list" if providers.has_workspace(org.id, current_user.id) else "uip_bp.my_access", org_slug=org_slug))',
    'return redirect(url_for("uip_bp.provider_dashboard", org_slug=org_slug))'
)

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(c)
