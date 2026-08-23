import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

inject_route = '''@mechanic_bp.route("/mechanic/debug_graham", methods=["GET"])
@login_required
def debug_graham():
    from app.models.mechanic import MechClient, MechJobCard
    clients = MechClient.query.all()
    output = "CLIENTS:<br>"
    for c in clients:
        output += f"ID: {c.id}, Name: {c.name}, UserID: {c.user_id}<br>"
    
    jobs = MechJobCard.query.all()
    output += "<br>JOBS:<br>"
    for j in jobs:
        output += f"ID: {j.id}, Job#: {j.job_number}, VehicleID: {j.vehicle_id}, Status: {j.status}<br>"
        
    return output
'''

# Find the end of the imports/setup, insert the route
content = content.replace("from . import mechanic_bp", "from . import mechanic_bp\n\n" + inject_route)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
