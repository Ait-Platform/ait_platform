from flask import render_template, redirect, url_for, flash, request, session
from flask_login import login_required, current_user
from sqlalchemy import text
from app.extensions import db
from . import mechanic_bp
from app.models.mechanic import MechClient, MechVehicle, MechJobCard, MechInvoice
from datetime import datetime, timedelta
import os
import time
from werkzeug.utils import secure_filename

@mechanic_bp.context_processor
def inject_currency():
    if current_user.is_authenticated:
        from app.models.auth import AuthSubject, UserEnrollment
        subject = AuthSubject.query.filter_by(slug='mechanic').first()
        if subject:
            enrollment = UserEnrollment.query.filter_by(user_id=current_user.id, subject_id=subject.id).first()
            if enrollment and enrollment.local_currency:
                curr = enrollment.local_currency.upper()
                sym_map = {'ZAR': 'R ', 'USD': '$ ', 'EUR': '€ ', 'GBP': '£ ', 'AUD': '$ ', 'CAD': '$ '}
                sym = sym_map.get(curr, '') if curr else ''
                return dict(currency_sym=sym)
    # Default fallback for unauthenticated or un-enrolled
    return dict(currency_sym='R ')

@mechanic_bp.route("/mechanic/about")
def about():
    return render_template("program_mechanic/about.html")

@mechanic_bp.route("/mechanic/price")
def price_page():
    from app.models.auth import AuthSubject
    from app.enrollment.logic import get_quote_for_subject_country
    
    subject = AuthSubject.query.filter(db.func.lower(AuthSubject.slug) == 'mechanic').first()
    if not subject:
        flash("Subject not found.", "warning")
        return redirect(url_for('public_bp.welcome'))

    country_code = (request.args.get("country") or "").strip().upper()
    if not country_code and current_user.is_authenticated:
        ent = db.session.execute(text("""
            SELECT ue.country_code 
              FROM user_enrollment ue
              JOIN auth_subject s ON s.id = ue.subject_id
             WHERE ue.user_id = :uid AND s.slug = 'mechanic'
        """), {"uid": current_user.id}).mappings().first()
        if ent and ent["country_code"]:
            country_code = ent["country_code"]

    if not country_code:
        country_code = session.get("country_code", "")

    if country_code:
        session["country_code"] = country_code

    price_ctx = {
        "has_quote": False,
        "price_id": None,
        "country_code": None,
        "local_amount": None,
        "local_currency": None,
        "estimated_zar": None,
        "fx_rate": None,
        "is_discount": False,
    }

    if country_code:
        row = get_quote_for_subject_country(subject.id, country_code)
        if row:
            price_ctx.update({
                "price_id": row.id,
                "country_code": row.country_code,
                "local_amount": row.local_amount_cents,
                "local_currency": row.local_currency,
                "estimated_zar": row.zar_amount_cents,
                "fx_rate": getattr(row, "fx_rate", None),
                "is_discount": getattr(row, "is_discount", False),
            })
            price_ctx["has_quote"] = True
        else:
            flash("No pricing found for that country yet.", "warning")

    countries = db.session.execute(
        text("""
            SELECT r.alpha2 AS code, r.name
              FROM ref_country_currency r
        """)
    ).mappings().all()

    val_quote = db.session.execute(text("SELECT value FROM system_settings WHERE key = 'mechanic_quote_cents'")).scalar()
    quote_cents = int(float(val_quote)) if val_quote else 500

    val_invoice = db.session.execute(text("SELECT value FROM system_settings WHERE key = 'mechanic_invoice_cents'")).scalar()
    invoice_cents = int(float(val_invoice)) if val_invoice else 1000

    return render_template("program_mechanic/price.html", price=price_ctx, subject=subject, countries=countries, quote_cents=quote_cents, invoice_cents=invoice_cents)

from app.models.mechanic import MechShop, MechCatalogPart

@mechanic_bp.route("/mechanic/topup")
@login_required
def topup():
    active_shop = MechShop.query.filter_by(user_id=current_user.id, onboarding_status='active').first()
    if not active_shop:
        flash("You need an active shop to top up.", "warning")
        return redirect(url_for('mechanic_bp.mechanic_dashboard'))
    return render_template("program_mechanic/topup.html", active_shop=active_shop)

@mechanic_bp.route("/mechanic/process_topup", methods=["POST"])
@login_required
def process_topup():
    amount_cents = request.form.get("amount_cents")
    if not amount_cents or int(amount_cents) < 5000:
        flash("Invalid top up amount.", "danger")
        return redirect(url_for('mechanic_bp.topup'))
    
    session["mechanic_topup_amount_cents"] = int(amount_cents)
    
    return redirect(url_for('yoco_bp.yoco_start', subject='mechanic_topup', email=current_user.email, next_url=url_for('mechanic_bp.mechanic_dashboard')))


@mechanic_bp.route("/mechanic/mock_bill")
@login_required
def mock_bill():
    active_shop = MechShop.query.filter_by(user_id=current_user.id, onboarding_status='active').first()
    if not active_shop:
        return redirect(url_for('mechanic_bp.mechanic_dashboard'))
    return render_template("program_mechanic/mock_bill.html", shop=active_shop)


@mechanic_bp.route("/mechanic/dashboard")
@login_required
def mechanic_dashboard():
    draft_shop = MechShop.query.filter(
        MechShop.user_id == current_user.id,
        MechShop.onboarding_status.like('draft_%')
    ).first()
    
    active_shop = MechShop.query.filter(
        MechShop.user_id == current_user.id,
        MechShop.onboarding_status == 'active'
    ).first()

    job_cards = MechJobCard.query.order_by(MechJobCard.created_at.desc()).limit(10).all()
    
    # Seed some default parts if none exist
    if MechCatalogPart.query.count() == 0:
        default_parts = [
            MechCatalogPart(part_name='Brake Pads', category='Brakes', default_price=450.0),
            MechCatalogPart(part_name='Oil Filter', category='Engine', default_price=120.0),
            MechCatalogPart(part_name='Spark Plug', category='Engine', default_price=80.0),
            MechCatalogPart(part_name='Air Filter', category='Engine', default_price=150.0),
            MechCatalogPart(part_name='Wiper Blades', category='Exterior', default_price=200.0),
            MechCatalogPart(part_name='Battery', category='Electrical', default_price=1200.0)
        ]
        db.session.bulk_save_objects(default_parts)
        db.session.commit()

    return render_template("program_mechanic/dashboard.html", 
                           job_cards=job_cards, 
                           draft_shop=draft_shop, 
                           active_shop=active_shop)

@mechanic_bp.route("/mechanic/onboarding/start", methods=["POST"])
@login_required
def onboarding_start():
    import time
    time.sleep(2) # Simulate AI processing
    
    draft_shop = MechShop.query.filter(
        MechShop.user_id == current_user.id,
        MechShop.onboarding_status.like('draft_%')
    ).first()
    
    if not draft_shop:
        draft_shop = MechShop(
            user_id=current_user.id,
            business_name="Extracted Business Name",
            address="123 Extracted Street",
            phone="555-1234",
            email="extracted@example.com",
            terms_and_conditions="Payment strictly within 30 days.",
            onboarding_status='draft_review',
            trial_ends_at=datetime.utcnow() + timedelta(days=30)
        )
        db.session.add(draft_shop)
        db.session.commit()
    
    return redirect(url_for("mechanic_bp.mechanic_dashboard", view='review'))

@mechanic_bp.route("/mechanic/onboarding/process", methods=["POST"])
@login_required
def onboarding_process():
    shop = MechShop.query.filter_by(user_id=current_user.id).first()
    if not shop:
        shop = MechShop(
            user_id=current_user.id,
            onboarding_status='active',
            trial_ends_at=datetime.utcnow() + timedelta(days=30)
        )
        db.session.add(shop)
        
    shop.business_name = request.form.get("business_name") or "My Mechanic Shop"
    shop.address = request.form.get("address")
    shop.phone = request.form.get("phone")
    shop.email = request.form.get("email")
    shop.terms_and_conditions = request.form.get("terms_and_conditions")
    shop.onboarding_status = 'active'
    
    logo_file = request.files.get("logo_file")
    if logo_file and logo_file.filename:
        import os
        import time
        from werkzeug.utils import secure_filename
        from flask import current_app
        
        filename = secure_filename(f"mechanic_{current_user.id}_{int(time.time())}_{logo_file.filename}")
        upload_folder = os.path.join(current_app.root_path, "static", "uploads", "mechanic")
        os.makedirs(upload_folder, exist_ok=True)
        logo_file.save(os.path.join(upload_folder, filename))
        shop.logo_url = filename

    db.session.commit()
    flash("Shop profile successfully saved!", "success")
        
    return redirect(url_for("mechanic_bp.mechanic_dashboard"))

@mechanic_bp.route("/mechanic/intake", methods=["GET", "POST"])
@login_required
def mechanic_intake():
    # Placeholder for new vehicle intake
    if request.method == "POST":
        # Handle form submission for new client + vehicle
        flash("Vehicle intake successful (Mock)", "success")
        return redirect(url_for('mechanic_bp.mechanic_dashboard'))
    return render_template("program_mechanic/intake.html")

@mechanic_bp.route("/mechanic/job/<int:id>", methods=["GET", "POST"])
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
        
        selected_part_ids = request.form.getlist('selected_parts')
        
        if new_part_name and new_part_price:
            learned_part = MechCatalogPart(
                user_id=current_user.id,
                part_name=new_part_name,
                category='Custom',
                default_price=float(new_part_price)
            )
            db.session.add(learned_part)
            db.session.flush()
            selected_part_ids.append(str(learned_part.id))
            flash(f"Learned new part: {new_part_name}", "success")
            
        from app.models.mechanic import MechClient, MechVehicle, MechJobCard, MechPartLine, MechLaborLine
        import uuid
        
        # Mock finding or creating client
        client = MechClient.query.filter_by(name=customer_name).first()
        if not client:
            client = MechClient(name=customer_name)
            db.session.add(client)
            db.session.flush()
            
        vehicle = MechVehicle.query.filter_by(registration_number=vehicle_reg, client_id=client.id).first()
        if not vehicle:
            vehicle = MechVehicle(registration_number=vehicle_reg, make="Unknown", client_id=client.id)
            db.session.add(vehicle)
            db.session.flush()
            
        job_card = MechJobCard(
            job_number=f"JOB-{uuid.uuid4().hex[:6].upper()}",
            vehicle_id=vehicle.id,
            status='Ready'
        )
        db.session.add(job_card)
        db.session.flush()
        
        # Add a default labor line
        labor = MechLaborLine(job_card_id=job_card.id, mechanic_name="General", description="General Inspection", hours=1.0, rate_per_hour=350.0)
        db.session.add(labor)
        
        for p_id in selected_part_ids:
            part_def = MechCatalogPart.query.get(p_id)
            if part_def:
                pline = MechPartLine(
                    job_card_id=job_card.id,
                    part_number=part_def.part_name,
                    description=f"{part_def.category} part",
                    quantity=1,
                    unit_cost=part_def.default_price,
                    markup_price=part_def.default_price
                )
                db.session.add(pline)
                
        db.session.commit()
            
        flash("Quote created and Job Card generated successfully!", "success")
        return redirect(url_for("mechanic_bp.job_card_detail", id=job_card.id))
        
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
@mechanic_bp.route('/mechanic/client_soa/<int:client_id>')
@login_required
def client_soa(client_id):
    from app.models.debtors import Debtor
    debtor = Debtor.query.filter_by(reference_id=client_id, slug_reference='mechanic', user_id=current_user.id).first()
    if not debtor:
        flash('No Statement of Account exists for this client yet.', 'info')
        return redirect(url_for('mechanic_bp.mechanic_dashboard'))
    return redirect(url_for('debtors_bp.generate_soa', debtor_id=debtor.id))
