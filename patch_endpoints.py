with open("app/program_uip/__init__.py", "r", encoding="utf-8") as f:
    text = f.read()

old_endpoints = """    public_endpoints = {
        "uip_bp.router_page", "uip_bp.my_access", 
        "uip_bp.verify_public", 
        "uip_bp.public_dashboard",
        "uip_bp.verify_ratepayer",
        "uip_bp.verify_committee", "uip_bp.verify_secretary",
        "uip_bp.verify_mo",
        "uip_bp.verify_staff",
        "uip_bp.verify_subcommittee",
        "uip_bp.mo_dashboard",
        "uip_bp.subcommittee_dashboard",
        "uip_bp.waiting_lounge",
        "uip_bp.waiting_lounge_dispute",
        "uip_bp.service_status",
        "uip_bp.provisioning", "uip_bp.reset_genesis", "uip_bp.remove_trigger"
    }"""

new_endpoints = """    public_endpoints = {
        "uip_bp.router_page", "uip_bp.my_access", 
        "uip_bp.verify_public", 
        "uip_bp.public_dashboard",
        "uip_bp.verify_ratepayer",
        "uip_bp.verify_committee", "uip_bp.verify_secretary",
        "uip_bp.verify_mo",
        "uip_bp.verify_staff",
        "uip_bp.verify_subcommittee",
        "uip_bp.dashboard",
        "uip_bp.mo_dashboard",
        "uip_bp.subcommittee_dashboard",
        "uip_bp.waiting_lounge",
        "uip_bp.waiting_lounge_dispute",
        "uip_bp.service_status",
        "uip_bp.provisioning", "uip_bp.reset_genesis", "uip_bp.remove_trigger"
    }"""

text = text.replace(old_endpoints, new_endpoints)

with open("app/program_uip/__init__.py", "w", encoding="utf-8") as f:
    f.write(text)

print("Added dashboard to public_endpoints")
