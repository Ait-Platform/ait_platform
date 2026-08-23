import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# We need to add logic right after flash(f"Payment of R {amount/100.0:.2f} recorded successfully.", "success")
# We will find all job cards for this client that are in 'Awaiting Deposit' or 'Approved' and mark them as 'Billed'

auto_upgrade_logic = '''
            # Auto-upgrade any active job cards for this client to 'Billed'
            from app.models.mechanic import MechJobCard, MechVehicle, MechClient
            active_jobs = MechJobCard.query.join(MechVehicle).join(MechClient).filter(
                MechClient.name == debtor.name,
                MechClient.user_id == current_user.id,
                MechJobCard.status.in_(['Awaiting Deposit', 'Approved'])
            ).all()
            
            for j in active_jobs:
                j.status = 'Billed'
'''

regex = r'(flash\(f"Payment of R \{amount/100\.0:\.2f\} recorded successfully\.", "success"\))'
replacement = r'\1' + auto_upgrade_logic

content = re.sub(regex, replacement, content)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
