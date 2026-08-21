import os

with open('app/program_mechanic/routes.py', 'a', encoding='utf-8') as f:
    f.write('''
@mechanic_bp.route("/mechanic/job_card/<int:id>/accept", methods=["POST"])
@login_required
def accept_quote(id):
    job_card = MechJobCard.query.get_or_404(id)
    if job_card.status == 'Quote':
        job_card.status = 'Awaiting Deposit'
        db.session.commit()
        flash("Quote accepted! Waiting for deposit.", "success")
    return redirect(url_for('mechanic_bp.job_card_detail', id=id))

@mechanic_bp.route("/mechanic/job_card/<int:id>/reject", methods=["POST"])
@login_required
def reject_quote(id):
    from app.models.mechanic import MechCommunication
    job_card = MechJobCard.query.get_or_404(id)
    reason = request.form.get("reason", "")
    
    if job_card.status == 'Quote':
        job_card.status = 'Rejected'
        
        # Log communication for the rejection reason
        comm = MechCommunication(
            job_card_id=job_card.id,
            contact_type="Quote Rejected",
            details=f"Reason: {reason}"
        )
        db.session.add(comm)
        db.session.commit()
        flash("Quote marked as rejected.", "info")
    return redirect(url_for('mechanic_bp.job_card_detail', id=id))

@mechanic_bp.route("/mechanic/job_card/<int:id>/record_deposit", methods=["POST"])
@login_required
def record_deposit(id):
    from app.models.debtors import Debtor, SenderProfile, DebtorLedger
    from app.models.mechanic import MechShop
    
    job_card = MechJobCard.query.get_or_404(id)
    deposit_amount = request.form.get("deposit_amount", type=float)
    
    if deposit_amount and deposit_amount > 0:
        job_card.deposit_amount = (job_card.deposit_amount or 0) + deposit_amount
        job_card.status = 'Approved' # Moving from Awaiting Deposit to Approved
        
        # Move to debtors!
        client = job_card.vehicle.client
        if client:
            debtor = Debtor.query.filter_by(user_id=current_user.id, slug_reference='mechanic', name=client.name).first()
            if not debtor:
                sender_profile = SenderProfile.query.filter_by(user_id=current_user.id, is_default=True).first()
                debtor = Debtor(
                    user_id=current_user.id,
                    name=client.name,
                    phone=client.phone,
                    email=client.email,
                    slug_reference='mechanic',
                    sender_profile_id=sender_profile.id if sender_profile else None
                )
                db.session.add(debtor)
                db.session.flush() # get id
                
            # Log deposit as a credit transaction in DebtorLedger
            ledger = DebtorLedger(
                debtor_id=debtor.id,
                date=db.func.current_date(),
                ref=f"DEP-{job_card.job_number}",
                description=f"Deposit for Job #{job_card.job_number}",
                amount=deposit_amount,
                kind="credit"
            )
            db.session.add(ledger)
            
        db.session.commit()
        flash(f"Deposit of R {deposit_amount:.2f} recorded and synced to Debtors!", "success")
        
    return redirect(url_for('mechanic_bp.job_card_detail', id=id))
''')
