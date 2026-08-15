from app.models.auth import DirectMessage
from app.models.mechanic import MechShop, MechCatalogPart
from flask import render_template, redirect, url_for, flash, request, session, current_app
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
            enrollment = UserEnrollment.query.filter_by(
                user_id=current_user.id, subject_id=subject.id).first()
            if enrollment and enrollment.local_currency:
                curr = enrollment.local_currency.upper()
                sym_map = {'ZAR': 'R ', 'USD': '$ ', 'EUR': '€ ', 'GBP': '£ ',
                           'AUD': '$ ', 'CAD': '$ ', 'NGN': '₦ ', 'KES': 'KSh ', 'GHS': 'GH₵ '}
                sym = sym_map.get(curr, curr + ' ') if curr else ''
                return dict(currency_sym=sym)
    # Default fallback for unauthenticated or un-enrolled
    return dict(currency_sym='R ')


@mechanic_bp.route("/mechanic/about")
def about():
    return render_template("program_mechanic/about.html")


@mechanic_bp.route("/mechanic/communication-logs")
@login_required
def communication_logs():
    from app.models.auth import InviteLog
    logs = InviteLog.query.filter_by(
        sender_id=current_user.id, program_slug="mechanic").order_by(InviteLog.sent_at.desc()).all()
    return render_template("shared/invite_logs_page.html", logs=logs, is_admin_view=False, back_url=url_for("mechanic_bp.mechanic_dashboard"))


@mechanic_bp.route("/mechanic/price")
def price_page():
    from app.models.auth import AuthSubject
    from app.enrollment.logic import get_quote_for_subject_country

    subject = AuthSubject.query.filter(
        db.func.lower(AuthSubject.slug) == 'mechanic').first()
    if not subject:
        flash("Subject not found.", "warning")
        return redirect(url_for('public_bp.welcome'))

    is_enrolled = False
    country_code = (request.args.get("country") or "").strip().upper()

    if current_user.is_authenticated:
        ent = db.session.execute(text("""
            SELECT ue.country_code 
              FROM user_enrollment ue
              JOIN auth_subject s ON s.id = ue.subject_id
             WHERE ue.user_id = :uid AND lower(s.slug) = 'mechanic'
        """), {"uid": current_user.id}).mappings().first()
        if ent:
            is_enrolled = True
            if not country_code and ent["country_code"]:
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

    from app.utils.country import get_active_countries
    countries = get_active_countries()

    val_quote = db.session.execute(text(
        "SELECT value FROM system_settings WHERE key = 'mechanic_quote_cents'")).scalar()
    quote_cents = int(float(val_quote)) if val_quote else 500

    val_invoice = db.session.execute(text(
        "SELECT value FROM system_settings WHERE key = 'mechanic_invoice_cents'")).scalar()
    invoice_cents = int(float(val_invoice)) if val_invoice else 1000

    return render_template("program_mechanic/price.html", price=price_ctx, subject=subject, countries=countries, quote_cents=quote_cents, invoice_cents=invoice_cents, is_enrolled=is_enrolled)


@mechanic_bp.route("/mechanic/mock_bill")
@login_required
def mock_bill():
    active_shop = MechShop.query.filter_by(
        user_id=current_user.id, onboarding_status='active').first()
    if not active_shop:
        return redirect(url_for('mechanic_bp.mechanic_dashboard'))
    return render_template("program_mechanic/mock_bill.html", shop=active_shop)


@mechanic_bp.route("/mechanic/document_preview")
@login_required
def document_preview():
    active_shop = MechShop.query.filter_by(
        user_id=current_user.id, onboarding_status='active').first()
    if not active_shop:
        flash("You must complete your shop setup first.", "warning")
        return redirect(url_for('mechanic_bp.mechanic_dashboard'))

    from datetime import datetime

    mock_job = {
        'id': 0,
        'job_number': 'PREVIEW-001',
        'created_at': datetime.utcnow(),
        'invoices': [{'status': 'Unpaid'}],
        'vehicle': {
            'client': {
                'id': 0,
                'name': 'John Doe',
                'phone': '555-0192',
                'email': 'john@example.com'
            },
            'make': 'Toyota',
            'model': 'Corolla',
            'year': 2019,
            'registration_number': 'ABC 123',
            'vin': 'JT1234567890'
        },
        'part_lines': [
            {'part_name': 'Brake Pads (Front)',
             'quantity': 1, 'markup_price': 850.0}
        ],
        'labor_lines': [
            {'description': 'Replace front brake pads',
                'hours': 1.5, 'rate_per_hour': 450.0}
        ]
    }

    return render_template("program_mechanic/invoice_view.html", job_card=mock_job, shop=active_shop)


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

    job_cards = MechJobCard.query.order_by(
        MechJobCard.created_at.desc()).limit(10).all()

    # Seed some default parts if none exist
    if MechCatalogPart.query.count() == 0:
        default_parts = [
            MechCatalogPart(part_name='Brake Pads',
                            category='Brakes', default_price=450.0),
            MechCatalogPart(part_name='Oil Filter',
                            category='Engine', default_price=120.0),
            MechCatalogPart(part_name='Spark Plug',
                            category='Engine', default_price=80.0),
            MechCatalogPart(part_name='Air Filter',
                            category='Engine', default_price=150.0),
            MechCatalogPart(part_name='Wiper Blades',
                            category='Exterior', default_price=200.0),
            MechCatalogPart(part_name='Battery',
                            category='Electrical', default_price=1200.0)
        ]
        db.session.bulk_save_objects(default_parts)
        db.session.commit()

    from app.models.auth import AitTokenWallet
    wallet = AitTokenWallet.query.filter_by(user_id=current_user.id).first()

    return render_template("program_mechanic/dashboard.html",
                           job_cards=job_cards,
                           draft_shop=draft_shop,
                           active_shop=active_shop,
                           wallet=wallet)


@mechanic_bp.route("/mechanic/onboarding/start", methods=["POST"])
@login_required
def onboarding_start():
    import time
    time.sleep(2)  # Simulate AI processing

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
            onboarding_status='draft_review'
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
            onboarding_status='active'
        )
        db.session.add(shop)

        shop.business_name = request.form.get(
            "business_name") or "My Mechanic Shop"
    shop.address = request.form.get("address")
    shop.phone = request.form.get("phone")
    shop.email = request.form.get("email")
    shop.tax_number = request.form.get("tax_number")

    try:
        vat_rate = float(request.form.get("vat_rate", 0))
    except ValueError:
        vat_rate = 0.0
    shop.vat_rate = vat_rate

    shop.terms_and_conditions = request.form.get("terms_and_conditions")
    shop.use_custom_letterhead = True if request.form.get(
        "use_custom_letterhead") else False
    shop.onboarding_status = 'active'

    logo_file = request.files.get("logo_file")
    if logo_file and logo_file.filename:
        import os
        import time
        from werkzeug.utils import secure_filename
        from flask import current_app

        filename = secure_filename(
            f"mechanic_{current_user.id}_{int(time.time())}_{logo_file.filename}")
        upload_folder = os.path.join(
            current_app.root_path, "static", "uploads", "mechanic")
        os.makedirs(upload_folder, exist_ok=True)
        logo_file.save(os.path.join(upload_folder, filename))
        shop.logo_url = filename

    letterhead_file = request.files.get("letterhead_file")
    if letterhead_file and letterhead_file.filename:
        import os
        import time
        from werkzeug.utils import secure_filename
        from flask import current_app

        lh_filename = secure_filename(
            f"mechanic_lh_{current_user.id}_{int(time.time())}_{letterhead_file.filename}")
        upload_folder = os.path.join(
            current_app.root_path, "static", "uploads", "mechanic")
        os.makedirs(upload_folder, exist_ok=True)
        letterhead_file.save(os.path.join(upload_folder, lh_filename))
        shop.letterhead_url = lh_filename

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
    from datetime import datetime
    job_card = MechJobCard.query.get_or_404(id)
    today_date = datetime.utcnow().strftime('%Y-%m-%d')
    return render_template("program_mechanic/job_card.html", job_card=job_card, today_date=today_date)


@mechanic_bp.route("/mechanic/email/<int:id>", methods=["GET", "POST"])
@login_required
def email_document(id):
    from app.utils.mailer import send_email
    job_card = MechJobCard.query.get_or_404(id)

    doc_type = "Invoice" if job_card.status == 'Billed' else "Quote"
    default_email = ""
    if job_card.vehicle and job_card.vehicle.client and job_card.vehicle.client.email:
        default_email = job_card.vehicle.client.email

    if request.method == "POST":
        target_email = request.form.get("email")
        if not target_email:
            flash("Please provide an email address.", "warning")
            return redirect(url_for('mechanic_bp.email_document', id=id))

        subject = f"Your {doc_type} #{job_card.job_number} from AIT ProTrade"
        doc_url = url_for('mechanic_bp.job_card_detail', id=id, _external=True)

        body = f"Hello,\n\nYour {doc_type} #{job_card.job_number} is ready. You can view it here: {doc_url}\n\nThank you for choosing us!"
        html = f"<p>Hello,</p><p>Your {doc_type} <strong>#{job_card.job_number}</strong> is ready. You can view it here: <a href='{doc_url}'>{doc_url}</a></p><p>Thank you for choosing us!</p>"

        success = send_email(subject=subject, recipients=[
                             target_email], body=body, html=html)

        if success:
            flash(f"{doc_type} successfully emailed to {target_email}", "success")
        else:
            flash("Failed to send email. Please check server logs.", "danger")

        return redirect(url_for('mechanic_bp.job_card_detail', id=id))

    return render_template("program_mechanic/email_preview.html", job_card=job_card, doc_type=doc_type, default_email=default_email)


@mechanic_bp.route("/mechanic/job/<int:id>/approve", methods=["POST"])
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
    return redirect(url_for("mechanic_bp.job_card_detail", id=id))


@mechanic_bp.route('/mechanic/messages', methods=['GET', 'POST'])
@login_required
def messages():
    if request.method == 'POST':
        message_text = request.form.get('message')
        if message_text:
            new_msg = DirectMessage(
                user_id=current_user.id, subject='mechanic', message=message_text)
            db.session.add(new_msg)
            db.session.commit()
            flash('Message sent to Admin', 'success')
        return redirect(url_for('mechanic_bp.messages'))

    msgs = DirectMessage.query.filter_by(user_id=current_user.id, subject='mechanic').order_by(
        DirectMessage.created_at.desc()).all()
    return render_template('program_mechanic/messages.html', messages=msgs)


@mechanic_bp.route('/mechanic/catalog', methods=['GET', 'POST'])
@login_required
def catalog_manage():
    active_shop = MechShop.query.filter_by(
        user_id=current_user.id, onboarding_status='active').first()
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
                existing = MechCatalogPart.query.filter_by(
                    user_id=current_user.id, part_name=part_name).first()
                if existing:
                    existing.default_price = price
                    existing.category = category
                    flash(f'Updated price for {part_name}', 'success')
                else:
                    new_part = MechCatalogPart(
                        user_id=current_user.id, part_name=part_name, category=category, default_price=price)
                    db.session.add(new_part)
                    flash(f'Added {part_name} to your catalog', 'success')
                db.session.commit()

        elif action == 'delete':
            part_id = request.form.get('part_id')
            part = MechCatalogPart.query.filter_by(
                id=part_id, user_id=current_user.id).first()
            if part:
                db.session.delete(part)
                db.session.commit()
                flash('Part removed from your catalog.', 'success')

        return redirect(url_for('mechanic_bp.catalog_manage'))

    all_parts = MechCatalogPart.query.filter(
        (MechCatalogPart.user_id == None) | (
            MechCatalogPart.user_id == current_user.id)
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
    debtor = Debtor.query.filter_by(
        reference_id=client_id, slug_reference='mechanic', user_id=current_user.id).first()
    if not debtor:
        flash('No Statement of Account exists for this client yet.', 'info')
        return redirect(url_for('mechanic_bp.mechanic_dashboard'))
    return_url = request.args.get('return_url')
    if return_url:
        return redirect(url_for('debtors_bp.generate_soa', debtor_id=debtor.id, return_url=return_url))
    return redirect(url_for('debtors_bp.generate_soa', debtor_id=debtor.id))


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
        token_cost = quote_cost // 100

        from app.models.auth import AitTokenWallet, AitTokenTransaction
        wallet = AitTokenWallet.query.filter_by(user_id=current_user.id).first()

        if not wallet or wallet.balance < token_cost:
            flash("Insufficient tokens. Please top up your wallet.", "warning")
            return redirect(url_for("mechanic_bp.mock_bill"))
            
        wallet.balance -= token_cost
        txn = AitTokenTransaction(
            wallet_id=wallet.id,
            amount=-token_cost,
            description=f"Generated quote for shop {active_shop.id}"
        )
        db.session.add(txn)
        
        customer_name = request.form.get("customer_name")
        vehicle_reg = request.form.get("vehicle_reg")
        
        if not customer_name or not customer_name.strip():
            flash("Customer Name is required.", "danger")
            return redirect(url_for('mechanic_bp.new_quote'))
            
        if not vehicle_reg or not vehicle_reg.strip():
            flash("Vehicle Registration is required.", "danger")
            return redirect(url_for('mechanic_bp.new_quote'))
        vin_number = request.form.get("vin_number")
        make = request.form.get("make")
        model = request.form.get("model")
        year_str = request.form.get("year")
        year = int(year_str) if year_str and year_str.isdigit() else None
        # Process dynamic labor and parts arrays
        labor_descs = request.form.getlist('labor_desc[]')
        labor_ins = request.form.getlist('labor_in[]')
        labor_outs = request.form.getlist('labor_out[]')
        labor_rates = request.form.getlist('labor_rate[]')

        part_qtys = request.form.getlist('part_qty[]')
        part_descs = request.form.getlist('part_desc[]')
        part_rates = request.form.getlist('part_rate[]')
        
        from app.models.mechanic import MechClient, MechVehicle, MechJobCard, MechPartLine, MechLaborLine
        import uuid
        
        # Mock finding or creating client
        client = MechClient.query.filter_by(name=customer_name).first()
        if not client:
            client = MechClient(name=customer_name)
            db.session.add(client)
            db.session.flush()
            
        vehicle = MechVehicle.query.filter_by(license_plate=vehicle_reg, client_id=client.id).first()
        if not vehicle:
            vehicle = MechVehicle(license_plate=vehicle_reg, make="Unknown", client_id=client.id)
            db.session.add(vehicle)
            db.session.flush()

        import os
        from werkzeug.utils import secure_filename
        
        license_disk_image = request.files.get("license_disk_image")
        filename = None
        if license_disk_image and license_disk_image.filename:
            upload_folder = os.path.join(current_app.root_path, "static", "uploads", "mechanic")
            os.makedirs(upload_folder, exist_ok=True)
            filename = secure_filename(license_disk_image.filename)
            import time
            filename = f"{int(time.time())}_{filename}"
            license_disk_image.save(os.path.join(upload_folder, filename))
            
        if vin_number: vehicle.vin = vin_number
        if make: vehicle.make = make
        if model: vehicle.model = model
        if year: vehicle.year = year
        # Check if an AJAX-uploaded filename was passed as hidden input
        hidden_filename = request.form.get("uploaded_disk_filename")
        if hidden_filename:
            vehicle.license_disk_url = hidden_filename
        elif filename: 
            vehicle.license_disk_url = filename

            
        job_card = MechJobCard(
            job_number=f"JOB-{uuid.uuid4().hex[:6].upper()}",
            vehicle_id=vehicle.id,
            status='Quote',
            vat_rate=active_shop.vat_rate
        )
        db.session.add(job_card)
        db.session.flush()
        
        # Process Labor Lines
        for i in range(len(labor_descs)):
            desc = labor_descs[i].strip()
            if not desc:
                continue
            t_in = labor_ins[i] if i < len(labor_ins) else ""
            t_out = labor_outs[i] if i < len(labor_outs) else ""
            rate_str = labor_rates[i] if i < len(labor_rates) else "0"
            rate = float(rate_str) if rate_str else 0.0
            
            hours = 0.0
            if t_in and t_out:
                try:
                    h1, m1 = map(int, t_in.split(':'))
                    h2, m2 = map(int, t_out.split(':'))
                    diff = (h2 + m2/60.0) - (h1 + m1/60.0)
                    if diff < 0:
                        diff += 24.0
                    hours = round(diff, 2)
                except Exception:
                    pass

            labor = MechLaborLine(
                job_card_id=job_card.id,
                mechanic_name="Shop Tech",
                description=desc,
                time_in=t_in,
                time_out=t_out,
                hours=hours,
                rate_per_hour=rate
            )
            db.session.add(labor)
        
        # Process Part Lines
        for i in range(len(part_descs)):
            desc = part_descs[i].strip()
            if not desc:
                continue
            qty_str = part_qtys[i] if i < len(part_qtys) else "1"
            qty = int(qty_str) if qty_str else 1
            rate_str = part_rates[i] if i < len(part_rates) else "0"
            rate = float(rate_str) if rate_str else 0.0
            
            pline = MechPartLine(
                job_card_id=job_card.id,
                part_number="Custom/Selected",
                description=desc,
                quantity=qty,
                unit_cost=rate,
                markup_price=rate
            )
            db.session.add(pline)
                
        db.session.commit()
            
        flash("Quote created and Job Card generated successfully!", "success")
        return redirect(url_for("mechanic_bp.job_card_detail", id=job_card.id))
        
    return render_template("program_mechanic/quote_form.html", catalog_parts=catalog_parts, shop=active_shop)

from app.models.auth import DirectMessage

@mechanic_bp.route("/mechanic/help")
@login_required
def help_center():
    flash("Help Center is coming soon!", "info")
    return redirect(url_for('mechanic_bp.mechanic_dashboard'))


@mechanic_bp.route("/mechanic/jobs")
@login_required
def job_cards_list():
    active_shop = MechShop.query.filter_by(user_id=current_user.id, onboarding_status='active').first()
    job_cards = []
    if active_shop:
        job_cards = MechJobCard.query.join(MechVehicle).join(MechClient).filter(
            MechClient.user_id == current_user.id
        ).order_by(MechJobCard.created_at.desc()).all()
        # Fallback if clients weren't created with user_id
        if not job_cards:
            job_cards = MechJobCard.query.order_by(MechJobCard.created_at.desc()).limit(50).all()
    return render_template("program_mechanic/job_cards_list.html", job_cards=job_cards)

@mechanic_bp.route("/mechanic/api/upload_disk", methods=["POST"])
@login_required
def upload_disk():
    import os
    import time
    from werkzeug.utils import secure_filename
    from flask import jsonify

    if "license_disk_image" not in request.files:
        return jsonify({"error": "No file part"}), 400

    file = request.files["license_disk_image"]
    if file.filename == "":
        return jsonify({"error": "No selected file"}), 400

    if file:
        upload_folder = os.path.join(current_app.root_path, "static", "uploads", "mechanic")
        os.makedirs(upload_folder, exist_ok=True)
        filename = secure_filename(file.filename)
        filename = f"{int(time.time())}_{filename}"
        file.save(os.path.join(upload_folder, filename))
        
        # Return URL for preview and the filename for saving
        file_url = url_for('static', filename=f'uploads/mechanic/{filename}')
        return jsonify({"url": file_url, "filename": filename})

    return jsonify({"error": "Upload failed"}), 500
