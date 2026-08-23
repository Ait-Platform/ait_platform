import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    '''        comm = MechCommunication(
            job_card_id=job_card.id,
            contact_type="Quote Rejected",
            details=f"Reason: {reason}"
        )''',
    '''        comm = MechCommunication(
            job_card_id=job_card.id,
            comm_type="System",
            message=f"Quote Rejected. Reason: {reason}"
        )'''
)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
