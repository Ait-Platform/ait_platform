import re

with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_return = """    # 2. Strangers / Unverified Users
    # We now always show the 7 tiles. The verify routes will handle routing to provisioning vs waiting lounge based on founding_exists.
    return render_template("program_uip/router.html", org=org)"""

new_return = """    # 2. Strangers / Unverified Users
    # We now always show the 7 tiles. The verify routes will handle routing to provisioning vs waiting lounge based on founding_exists.
    
    # Calculate occupied singular seats so the UI can grey them out
    from app.models.uip_governance import UipCommitteeMember
    from sqlalchemy import func
    occupied = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        func.lower(UipCommitteeMember.position).in_(["chairman", "vice chairman", "secretary", "treasurer"])
    ).all()
    
    occupied_seats = [m.position.lower() for m in occupied]
    
    return render_template("program_uip/router.html", org=org, occupied_seats=occupied_seats)"""

if old_return in text:
    text = text.replace(old_return, new_return)
    with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
        f.write(text)
    print("Updated router_page logic successfully")
else:
    print("Failed to find old_return")
