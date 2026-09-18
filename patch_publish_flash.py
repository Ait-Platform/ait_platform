import re
with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_publish = """    resolution.status = "PROPOSED"
    db.session.commit()
    
    flash("Resolution published to the committee for voting.", "success")"""

new_publish = """    resolution.status = "PROPOSED"
    db.session.commit()
    
    flash("Resolution published! The system has automatically pinged all eligible members to cast their vote.", "success")"""
text = text.replace(old_publish, new_publish)

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated publish flash message")
