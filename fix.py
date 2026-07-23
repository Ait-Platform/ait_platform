import sys

with open('D:/Users/yeshk/Documents/ait_platform/app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

start_idx = -1
for i, line in enumerate(lines):
    if line.startswith('def job_card_detail'):
        start_idx = i - 2
        break

if start_idx != -1:
    new_content = lines[:start_idx]
    
    new_code = '''@mechanic_bp.route("/mechanic/job/<int:id>", methods=["GET", "POST"])
@login_required
def job_card_detail(id):
    job_card = MechJobCard.query.get_or_404(id)
    return render_template("program_mechanic/job_card.html", job_card=job_card)

@mechanic_bp.route("/mechanic/invoice/<int:id>", methods=["GET", "POST"])
@login_required
def generate_invoice(id):
    active_shop = MechShop.query.filter_by(user_id=current_user.id, onboarding_status='active').first()
    if not active_shop:
        flash("You must complete your shop setup first.", "warning")
        return redirect(url_for("mechanic_bp.mechanic_dashboard"))

    if request.method == "POST":
        setting = db.session.execute(text("SELECT value FROM system_settings WHERE key = 'mechanic_invoice_cents'")).fetchone()
        invoice_cost = int(setting[0]) if setting else 1000

        if active_shop.trial_ends_at and datetime.utcnow() < active_shop.trial_ends_at:
            active_shop.shadow_spent_cents += invoice_cost
            db.session.commit()
            flash(f"Invoice generated successfully! (Shadow Billed R{invoice_cost/100:.2f})", "success")
        else:
            if active_shop.wallet_balance_cents < invoice_cost:
                flash("Insufficient tokens. Please top up or pay your registration fee.", "warning")
                return redirect(url_for("mechanic_bp.mock_bill"))
                
            active_shop.wallet_balance_cents -= invoice_cost
            db.session.commit()
            flash(f"Invoice generated successfully! (R{invoice_cost/100:.2f} deducted)", "success")
            
        job_card = MechJobCard.query.get_or_404(id)
        labor_total = sum(l.hours * l.rate_per_hour for l in job_card.labor_lines)
        parts_total = sum(p.quantity * p.markup_price for p in job_card.part_lines)
        total = labor_total + parts_total
        
        from app.models.mechanic import MechInvoice
        invoice = MechInvoice(job_card_id=job_card.id, subtotal=total, total=total, status='Unpaid')
        db.session.add(invoice)
        job_card.status = 'Billed'
        db.session.flush()
        
        from app.models.debtors import Debtor, DebtorLedger
        client = job_card.vehicle.client
        debtor = Debtor.query.filter_by(reference_id=client.id, slug_reference='mechanic').first()
        
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
            transaction_type='debit',
            amount_cents=int(total * 100),
            description=f'Mechanic Invoice for Job #{job_card.job_number}'
        )
        db.session.add(ledger)
        db.session.commit()
        
        return redirect(url_for("mechanic_bp.job_card_detail", id=id))

    job_card = MechJobCard.query.get_or_404(id)
    return render_template("program_mechanic/invoice_view.html", job_card=job_card, shop=active_shop)

@mechanic_bp.route("/mechanic/quote/new", methods=["GET", "POST"])
@login_required
def new_quote():
    active_shop = MechShop.query.filter_by(user_id=current_user.id, onboarding_status='active').first()
    if not active_shop:
        flash("You must complete your shop setup first.", "warning")
        return redirect(url_for("mechanic_bp.mechanic_dashboard"))
        
    all_parts = MechCatalogPart.query.filter(
        (MechCatalogPart.user_id == None) | (MechCatalogPart.user_id == current_user.id)
    ).all()
    
    part_dict = {}
    for p in all_parts:
        name_lower = p.part_name.lower().strip()
        if name_lower not in part_dict:
            part_dict[name_lower] = p
        else:
            if p.user_id == current_user.id:
                part_dict[name_lower] = p
    
    catalog_parts = list(part_dict.values())
    catalog_parts.sort(key=lambda x: x.part_name)
    
    if request.method == "POST":
        setting = db.session.execute(text("SELECT value FROM system_settings WHERE key = 'mechanic_quote_cents'")).fetchone()
        quote_cost = int(setting[0]) if setting else 500

        if active_shop.trial_ends_at and datetime.utcnow() < active_shop.trial_ends_at:
            active_shop.shadow_spent_cents += quote_cost
        else:
            if active_shop.wallet_balance_cents < quote_cost:
                flash("Insufficient tokens. Please top up or pay your registration fee.", "warning")
                return redirect(url_for("mechanic_bp.mock_bill"))
                
            active_shop.wallet_balance_cents -= quote_cost
        
        customer_name = request.form.get("customer_name")
        vehicle_reg = request.form.get("vehicle_reg")
        new_part_name = request.form.get("new_part_name")
        new_part_price = request.form.get("new_part_price")
        
        if new_part_name and new_part_price:
            learned_part = MechCatalogPart(
                user_id=current_user.id,
                part_name=new_part_name,
                category='Custom',
                default_price=float(new_part_price)
            )
            db.session.add(learned_part)
            db.session.commit()
            flash(f"Learned new part: {new_part_name}", "success")
            
        flash("Quote created successfully!", "success")
        return redirect(url_for("mechanic_bp.mechanic_dashboard"))
        
    return render_template("program_mechanic/quote_form.html", catalog_parts=catalog_parts, shop=active_shop)

from app.models.auth import DirectMessage

@mechanic_bp.route('/mechanic/messages', methods=['GET', 'POST'])
@login_required
def messages():
    if request.method == 'POST':
        message_text = request.form.get('message')
        if message_text:
            new_msg = DirectMessage(user_id=current_user.id, subject='mechanic', message=message_text)
            db.session.add(new_msg)
            db.session.commit()
            flash('Message sent to Admin', 'success')
        return redirect(url_for('mechanic_bp.messages'))
    
    msgs = DirectMessage.query.filter_by(user_id=current_user.id, subject='mechanic').order_by(DirectMessage.created_at.desc()).all()
    return render_template('program_mechanic/messages.html', messages=msgs)

@mechanic_bp.route('/mechanic/catalog', methods=['GET', 'POST'])
@login_required
def catalog_manage():
    active_shop = MechShop.query.filter_by(user_id=current_user.id, onboarding_status='active').first()
    if not active_shop:
        flash('You must complete your shop setup first.', 'warning')
        return redirect(url_for('mechanic_bp.mechanic_dashboard'))

    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'add':
            part_name = request.form.get('part_name')
            category = request.form.get('category', 'Custom')
            price = request.form.get('price', type=float, default=0.0)
            
            if part_name:
                existing = MechCatalogPart.query.filter_by(user_id=current_user.id, part_name=part_name).first()
                if existing:
                    existing.default_price = price
                    existing.category = category
                    flash(f'Updated price for {part_name}', 'success')
                else:
                    new_part = MechCatalogPart(user_id=current_user.id, part_name=part_name, category=category, default_price=price)
                    db.session.add(new_part)
                    flash(f'Added {part_name} to your catalog', 'success')
                db.session.commit()
                
        elif action == 'delete':
            part_id = request.form.get('part_id')
            part = MechCatalogPart.query.filter_by(id=part_id, user_id=current_user.id).first()
            if part:
                db.session.delete(part)
                db.session.commit()
                flash('Part removed from your catalog.', 'success')
                
        return redirect(url_for('mechanic_bp.catalog_manage'))

    all_parts = MechCatalogPart.query.filter(
        (MechCatalogPart.user_id == None) | (MechCatalogPart.user_id == current_user.id)
    ).all()
    
    part_dict = {}
    for p in all_parts:
        name_lower = p.part_name.lower().strip()
        if name_lower not in part_dict:
            part_dict[name_lower] = p
        else:
            if p.user_id == current_user.id:
                part_dict[name_lower] = p
                
    catalog_parts = list(part_dict.values())
    catalog_parts.sort(key=lambda x: x.part_name)

    return render_template('program_mechanic/catalog_manage.html', catalog_parts=catalog_parts, shop=active_shop)
'''
    new_content.append(new_code)
    
    with open('D:/Users/yeshk/Documents/ait_platform/app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
        f.writelines(new_content)
