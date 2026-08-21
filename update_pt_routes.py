import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Update approve_quote
approve_original = '''@mechanic_bp.route("/mechanic/job/<int:id>/approve", methods=["POST"])
@login_required
def approve_quote(id):
    job_card = MechJobCard.query.get_or_404(id)

    if job_card.status != 'Quote':
        flash("Only Quotes can be approved.", "warning")
        return redirect(url_for("mechanic_bp.job_card_detail", id=id))

    deposit_str = request.form.get("deposit_amount", "")
    try:
        deposit = float(deposit_str) if deposit_str else 0.0
    except ValueError:
        deposit = 0.0

    job_card.status = 'Approved'
    if deposit > 0:
        job_card.deposit_amount = deposit

        from app.models.debtors import Debtor, DebtorLedger
        client = job_card.vehicle.client
        debtor = Debtor.query.filter_by(
            reference_id=client.id, slug_reference='mechanic', user_id=current_user.id).first()

        if not debtor:
            debtor = Debtor(
                user_id=current_user.id,
                name=client.name,
                email=client.email,
                phone=client.phone,
                reference_id=client.id,
                slug_reference='mechanic'
            )
            db.session.add(debtor)
            db.session.flush()

        ledger = DebtorLedger(
            debtor_id=debtor.id,
            kind='credit',
            amount=int(deposit * 100),
            description=f'Deposit for Job #{job_card.job_number}'
        )
        db.session.add(ledger)

    db.session.commit()
    flash("Quote approved and Job started successfully!", "success")
    return redirect(url_for("mechanic_bp.job_card_detail", id=id))'''

approve_new = '''@mechanic_bp.route("/mechanic/job/<int:id>/approve", methods=["POST"])
@login_required
def approve_quote(id):
    from datetime import datetime
    job_card = MechJobCard.query.get_or_404(id)

    if job_card.status != 'Quote':
        flash("Only Quotes can be approved.", "warning")
        return redirect(url_for("mechanic_bp.job_card_detail", id=id))

    pop_amount_str = request.form.get("pop_amount", "0")
    pop_ref = request.form.get("pop_ref", f"POP-{job_card.job_number}")
    pop_date_str = request.form.get("pop_date")
    
    try:
        pop_amount = float(pop_amount_str) if pop_amount_str else 0.0
    except ValueError:
        pop_amount = 0.0
        
    pop_date = datetime.utcnow()
    if pop_date_str:
        try:
            pop_date = datetime.strptime(pop_date_str, '%Y-%m-%d')
        except ValueError:
            pass

    job_card.status = 'Approved'
    if pop_amount > 0:
        job_card.deposit_amount = pop_amount

    # Ensure Debtors account exists
    from app.models.debtors import Debtor, DebtorLedger
    client = job_card.vehicle.client
    debtor = Debtor.query.filter_by(
        reference_id=client.id, slug_reference='mechanic', user_id=current_user.id).first()

    if not debtor:
        debtor = Debtor(
            user_id=current_user.id,
            name=client.name,
            email=client.email,
            phone=client.phone,
            reference_id=client.id,
            slug_reference='mechanic'
        )
        db.session.add(debtor)
        db.session.flush()

    # Step 1: Charge the full quote amount as a Debit (if not already charged)
    existing_charge = DebtorLedger.query.filter_by(
        debtor_id=debtor.id, 
        ref=f"JOB-{job_card.job_number}", 
        kind='debit'
    ).first()
    
    if not existing_charge and job_card.total > 0:
        charge_ledger = DebtorLedger(
            debtor_id=debtor.id,
            txn_date=datetime.utcnow(),
            kind='debit',
            amount=int(job_card.total * 100),
            description=f'Quote/Tax Invoice for Job #{job_card.job_number}',
            ref=f"JOB-{job_card.job_number}"
        )
        db.session.add(charge_ledger)

    # Step 2: Record the POP deposit as a Credit
    if pop_amount > 0:
        payment_ledger = DebtorLedger(
            debtor_id=debtor.id,
            txn_date=pop_date,
            kind='credit',
            amount=int(pop_amount * 100),
            description=f'Proof of Payment Deposit',
            ref=pop_ref
        )
        db.session.add(payment_ledger)

    db.session.commit()
    flash("Proof of Payment captured! Document converted to Tax Invoice.", "success")
    return redirect(url_for("mechanic_bp.job_card_detail", id=id))'''

content = content.replace(approve_original, approve_new)

# 2. Delete generate_invoice entirely to avoid confusion and 500 errors
# Since generate_invoice was huge and broken, let's use regex to remove it
generate_regex = r'@mechanic_bp\.route\("/mechanic/invoice/<int:id>", methods=\["GET", "POST"\]\)\s*@login_required\s*def generate_invoice\(id\):.*?(?=@mechanic_bp\.route|\Z)'
content = re.sub(generate_regex, "", content, flags=re.DOTALL)

# 3. Update client_soa to accept return_url
client_soa_original = '''@mechanic_bp.route('/mechanic/client_soa/<int:client_id>')
@login_required
def client_soa(client_id):
    from app.models.debtors import Debtor
    debtor = Debtor.query.filter_by(
        reference_id=client_id, slug_reference='mechanic', user_id=current_user.id).first()
    if not debtor:
        flash('No Statement of Account exists for this client yet.', 'info')
        return redirect(url_for('mechanic_bp.mechanic_dashboard'))
    return redirect(url_for('debtors_bp.generate_soa', debtor_id=debtor.id))'''

client_soa_new = '''@mechanic_bp.route('/mechanic/client_soa/<int:client_id>')
@login_required
def client_soa(client_id):
    from app.models.debtors import Debtor
    debtor = Debtor.query.filter_by(
        reference_id=client_id, slug_reference='mechanic', user_id=current_user.id).first()
    if not debtor:
        flash('No Statement of Account exists for this client yet.', 'info')
        return redirect(url_for('mechanic_bp.mechanic_dashboard'))
    return_url = request.args.get('return_url')
    if return_url:
        return redirect(url_for('debtors_bp.generate_soa', debtor_id=debtor.id, return_url=return_url))
    return redirect(url_for('debtors_bp.generate_soa', debtor_id=debtor.id))'''

content = content.replace(client_soa_original, client_soa_new)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated routes.py")
