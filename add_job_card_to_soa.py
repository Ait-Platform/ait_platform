import re

with open('app/program_debtors/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''    latest_job_card = None
    if debtor.slug_reference == 'mechanic':
        from app.models.mechanic import MechShop, MechVehicle, MechJobCard
        shop = MechShop.query.filter_by(
            user_id=current_user.id, onboarding_status='active').first()
            
        # Get latest job card
        client = debtor.mechanic_client
        if client:
            latest_vehicle = MechVehicle.query.filter_by(client_id=client.id).order_by(MechVehicle.id.desc()).first()
            if latest_vehicle:
                latest_job_card = MechJobCard.query.filter_by(vehicle_id=latest_vehicle.id).order_by(MechJobCard.id.desc()).first()

        if shop:'''

content = re.sub(
    r"    # Fallback/Override for Mechanic Integration\s*if debtor\.slug_reference == 'mechanic':\s*from app\.models\.mechanic import MechShop\s*shop = MechShop\.query\.filter_by\(\s*user_id=current_user\.id, onboarding_status='active'\)\.first\(\)\s*if shop:",
    replacement,
    content,
    flags=re.DOTALL
)

content = content.replace(
    '''period_opening_balance=period_opening_balance,''',
    '''period_opening_balance=period_opening_balance,
                                   latest_job_card=latest_job_card,'''
)

with open('app/program_debtors/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
