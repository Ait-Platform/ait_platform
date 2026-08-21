import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace in email_document
find_email = '''        if success:
            from app.models.mechanic import MechCommunication
            comm = MechCommunication(
                job_card_id=job_card.id,
                comm_type="Email",
                recipient=target_email,
                message=f"Sent {doc_type} #{job_card.job_number}",
                status="Success"
            )
            db.session.add(comm)
            db.session.commit()'''

replace_email = '''        if success:
            from app.models.mechanic import MechCommunication
            from app.models.auth import InviteLog
            
            comm = MechCommunication(
                job_card_id=job_card.id,
                comm_type="Email",
                recipient=target_email,
                message=f"Sent {doc_type} #{job_card.job_number}",
                status="Success"
            )
            db.session.add(comm)
            
            phone = "Unknown Client"
            if job_card.vehicle and job_card.vehicle.client:
                phone = job_card.vehicle.client.phone or f"{job_card.vehicle.client.name} (Client)"
            
            ilog = InviteLog(
                sender_id=current_user.id,
                recipient_phone=phone,
                program_slug="mechanic",
                invite_type=f"Email {doc_type} #{job_card.job_number}",
                status="Sent"
            )
            db.session.add(ilog)
            db.session.commit()'''

# Replace in new_quote
find_quote = '''            pline = MechPartLine(
                job_card_id=job_card.id,
                part_number="Custom/Selected",
                description=desc,
                quantity=qty,
                unit_cost=rate,
                markup_price=rate
            )
            db.session.add(pline)
                
        db.session.commit()
            
        flash("Quote created and Job Card generated successfully!", "success")'''

replace_quote = '''            pline = MechPartLine(
                job_card_id=job_card.id,
                part_number="Custom/Selected",
                description=desc,
                quantity=qty,
                unit_cost=rate,
                markup_price=rate
            )
            db.session.add(pline)
            
        from app.models.auth import InviteLog
        phone = "Unknown Client"
        if vehicle.client:
            phone = vehicle.client.phone or f"{vehicle.client.name} (Client)"
            
        ilog = InviteLog(
            sender_id=current_user.id,
            recipient_phone=phone,
            program_slug="mechanic",
            invite_type=f"Created Job Card #{job_card.job_number}",
            status="Logged"
        )
        db.session.add(ilog)
                
        db.session.commit()
            
        flash("Quote created and Job Card generated successfully!", "success")'''

if find_email in content:
    content = content.replace(find_email, replace_email)
    print("Email replace SUCCESS")
else:
    print("Email replace FAILED - string not found")

if find_quote in content:
    content = content.replace(find_quote, replace_quote)
    print("Quote replace SUCCESS")
else:
    print("Quote replace FAILED - string not found")

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
