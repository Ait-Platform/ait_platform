import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# I will add a script to job_cards_list to ensure ALL MechClients have a Debtor profile
regex = r'total_debtors_count = len\(all_debtors\) if "all_debtors" in locals\(\) else 0'
new_logic = '''
    # FIX: Ensure every MechClient has a matching Debtor profile (backfill for old clients)
    from app.models.mechanic import MechClient
    from app.models.debtors import SenderProfile
    
    clients = MechClient.query.filter_by(user_id=current_user.id).all()
    debtor_names = {d.name for d in all_debtors} if "all_debtors" in locals() else set()
    
    new_debtors_added = False
    for c in clients:
        if c.name not in debtor_names:
            sender_profile = SenderProfile.query.filter_by(user_id=current_user.id, is_default=True).first()
            new_d = Debtor(
                user_id=current_user.id,
                name=c.name,
                phone=c.phone,
                email=c.email,
                slug_reference='mechanic',
                sender_profile_id=sender_profile.id if sender_profile else None
            )
            db.session.add(new_d)
            new_debtors_added = True
            
    if new_debtors_added:
        db.session.commit()
        # Re-fetch all debtors
        all_debtors = Debtor.query.filter_by(user_id=current_user.id).all()
        
    total_debtors_count = len(all_debtors) if "all_debtors" in locals() else 0
'''

content = re.sub(regex, new_logic, content)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
