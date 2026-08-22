from app.models.auth import DirectMessage
from app.models.mechanic import MechShop, MechCatalogPart


from flask import render_template, redirect, url_for, flash, request, session, current_app
from flask_login import login_required, current_user
from sqlalchemy import text
from app.extensions import db
from . import mechanic_bp

@mechanic_bp.route("/mechanic/debug_graham", methods=["GET"])
@login_required
def debug_graham():
    from app.models.mechanic import MechClient, MechJobCard
    clients = MechClient.query.all()
    output = "CLIENTS:<br>"
    for c in clients:
        output += f"ID: {c.id}, Name: {c.name}, UserID: {c.user_id}<br>"
    
    jobs = MechJobCard.query.all()
    output += "<br>JOBS:<br>"
    for j in jobs:
        output += f"ID: {j.id}, Job#: {j.job_number}, VehicleID: {j.vehicle_id}, Status: {j.status}<br>"
        
    return output


@mechanic_bp.route("/mechanic/fix_quotes", methods=["GET"])
@login_required
def fix_quotes():
    from app.models.mechanic import MechClient
    clients = MechClient.query.all()
    count = 0
    for c in clients:
        c.user_id = current_user.id
        count += 1
    db.session.commit()
    flash(f"Successfully recovered and linked {count} clients/quotes to your account!", "success")
    return redirect(url_for('mechanic_bp.mechanic_dashboard'))

@mechanic_bp.route("/mechanic/fix_wallet", methods=["GET"])
@login_required
def fix_wallet():
    from app.models.auth import AitTokenWallet, AitTokenTransaction
    wallet = AitTokenWallet.query.filter_by(user_id=current_user.id).first()
    if not wallet:
        wallet = AitTokenWallet(user_id=current_user.id, balance=0)
        db.session.add(wallet)
        db.session.flush()
    wallet.balance += 200
    tx = AitTokenTransaction(wallet_id=wallet.id, amount=200, description="Manual fix for missing Render tokens")
    db.session.add(tx)
    db.session.commit()
    flash("200 Tokens injected successfully into Render DB!", "success")
    return redirect(url_for('mechanic_bp.mechanic_dashboard'))
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
    
    from app.models.billing import TokenTariff
    schedule_tariff = TokenTariff.query.filter_by(program_slug='mechanic', action_name='generate_schedule').first()
    schedule_tokens = schedule_tariff.base_token_cost if schedule_tariff else 10

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
    # Attempt to auto-upgrade the table if missing
    try:
        db.session.execute(text('ALTER TABLE mech_job_cards ADD COLUMN next_service_due VARCHAR(100);'))
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        
    try:
        db.session.execute(text('ALTER TABLE mech_job_cards ADD COLUMN IF NOT EXISTS next_service_due VARCHAR(100);'))
        db.session.commit()
    except Exception as e:
        db.session.rollback()
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

    
    shop.business_name = request.form.get("business_name") or shop.business_name or "My Mechanic Shop"
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
    shop.bank_details = request.form.get("bank_details")
    shop.onboarding_status = 'active'
    # Default to true if a checkbox is checked, but we'll override it later if they actually have a letterhead url
    shop.use_custom_letterhead = True if request.form.get("use_custom_letterhead") else False

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
        
    if shop.letterhead_url:
        shop.use_custom_letterhead = True
    else:
        shop.use_custom_letterhead = False

    db.session.commit()
    
    # Sync with SenderProfile for Debtors module
    from app.models.debtors import SenderProfile
    sender_profile = SenderProfile.query.filter_by(user_id=current_user.id, is_default=True).first()
    if not sender_profile:
        sender_profile = SenderProfile(user_id=current_user.id, is_default=True)
        db.session.add(sender_profile)
    sender_profile.business_name = shop.business_name
    sender_profile.address = shop.address
    sender_profile.phone = shop.phone
    sender_profile.email = shop.email
    sender_profile.logo_url = shop.logo_url
    sender_profile.letterhead_url = shop.letterhead_url
    sender_profile.use_custom_letterhead = shop.use_custom_letterhead
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



@mechanic_bp.route("/mechanic/client/<int:client_id>/update", methods=["POST"])
@login_required
def update_client(client_id):
    from app.models.mechanic import MechClient
    from app.models.debtors import Debtor
    client = MechClient.query.get_or_404(client_id)
    job_id = request.args.get('job_id')
    
    name = request.form.get("name")
    phone = request.form.get("phone")
    email = request.form.get("email")
    
    if name:
        client.name = name
    client.phone = phone
    client.email = email
    
    vin = request.form.get("vin")
    if job_id and vin:
        job_card = MechJobCard.query.get(job_id)
        if job_card and job_card.vehicle:
            job_card.vehicle.vin = vin
    
    # Sync with Debtors profile if it exists
    debtor = Debtor.query.filter_by(
        reference_id=client.id, slug_reference='mechanic', user_id=current_user.id).first()
    if debtor:
        if name:
            debtor.name = name
        debtor.phone = phone
        debtor.email = email
        
    db.session.commit()
    flash("Client details updated successfully.", "success")
    
    return_url = request.args.get('return_url')
    if return_url:
        return redirect(return_url)
    
    if job_id:
        return redirect(url_for('mechanic_bp.job_card_detail', id=job_id))
    return redirect(url_for('mechanic_bp.mechanic_dashboard'))

@mechanic_bp.route("/mechanic/job_card/<int:id>")
@login_required
def job_card_detail(id):
    from datetime import datetime
    from app.models.mechanic import MechCommunication
    from app.models.debtors import Debtor
    job_card = MechJobCard.query.get_or_404(id)
    today_date = datetime.utcnow().strftime('%Y-%m-%d')
    communications = MechCommunication.query.filter_by(job_card_id=id).order_by(MechCommunication.created_at.desc()).all()
    
    client_debtor = None
    if job_card.vehicle and job_card.vehicle.client:
        client_debtor = Debtor.query.filter(
            Debtor.user_id == current_user.id,
            Debtor.slug_reference == 'mechanic',
            Debtor.name == job_card.vehicle.client.name
        ).first()

    from app.models.mechanic import MechShop
    active_shop = MechShop.query.filter_by(user_id=current_user.id, onboarding_status='active').first()
    return render_template("program_mechanic/job_card.html", job_card=job_card, today_date=today_date, communications=communications, client_debtor=client_debtor, shop=active_shop)



@mechanic_bp.route("/mechanic/download/<int:id>", methods=["GET"])
@login_required
def download_document(id):
    from app.utils.pdf_render import html_to_pdf_bytes
    from datetime import datetime
    import io
    from flask import send_file
    
    job_card = MechJobCard.query.get_or_404(id)
    
    client = job_card.vehicle.client if job_card.vehicle else None
    if (client and (not client.email or not client.phone)) or (job_card.vehicle and not job_card.vehicle.vin):
        flash("Please fill in the client's email, phone, and vehicle VIN before generating documents.", "warning")
        return redirect(url_for('mechanic_bp.job_card_detail', id=job_card.id))
        
    active_shop = MechShop.query.filter_by(user_id=current_user.id, onboarding_status='active').first()
    from app.models.debtors import BusinessBankAccount
    bank_account = BusinessBankAccount.query.filter_by(user_id=job_card.vehicle.client.shop.user_id, is_default=True).first()
    
    today_date = datetime.utcnow().strftime('%Y-%m-%d')
    pdf_html_content = render_template("program_mechanic/public_job_card.html", job_card=job_card, shop=active_shop, today_date=today_date, bank_account=bank_account)
    
    try:
        pdf_bytes = html_to_pdf_bytes(pdf_html_content, base_url=request.host_url)
        doc_type = "Invoice" if job_card.status == 'Billed' else "Quote"
        file_name = f"{doc_type}_{job_card.job_number}.pdf"
        
        return send_file(
            io.BytesIO(pdf_bytes),
            mimetype='application/pdf',
            as_attachment=True,
            download_name=file_name
        )
    except Exception as e:
        current_app.logger.error(f"Failed to generate PDF: {e}")
        flash("Failed to generate PDF. Please try again.", "danger")
        return redirect(url_for('mechanic_bp.job_card_detail', id=id))

@mechanic_bp.route("/mechanic/email/<int:id>", methods=["GET", "POST"])
@login_required
def email_document(id):
    from app.utils.mailer import send_email, send_pdf_email
    from app.utils.pdf_render import html_to_pdf_bytes
    from datetime import datetime
    from flask_mail import Message
    from app.extensions import mail
    
    job_card = MechJobCard.query.get_or_404(id)
    
    client = job_card.vehicle.client if job_card.vehicle else None
    if (client and (not client.email or not client.phone)) or (job_card.vehicle and not job_card.vehicle.vin):
        flash("Please fill in the client's email, phone, and vehicle VIN before generating documents.", "warning")
        return redirect(url_for('mechanic_bp.job_card_detail', id=job_card.id))

    doc_type = "SOA" if job_card.status in ['Approved', 'Billed'] else "Quote"
    default_email = ""
    if job_card.vehicle and job_card.vehicle.client and job_card.vehicle.client.email:
        default_email = job_card.vehicle.client.email

    if request.method == "POST":
        target_email = request.form.get("email")
        if not target_email:
            flash("Please provide an email address.", "warning")
            return redirect(url_for('mechanic_bp.email_document', id=id))

        active_shop = MechShop.query.filter_by(user_id=current_user.id, onboarding_status='active').first()
        subject = f"Your {doc_type} #{job_card.job_number} from {active_shop.business_name if active_shop else 'AIT ProTrade'}"
        
        # VERY IMPORTANT: doc_url must be the public job card URL
        doc_url = url_for('mechanic_bp.public_job_card', job_number=job_card.job_number, _external=True)

        body = f"Hello,\n\nYour {doc_type} #{job_card.job_number} is ready. We have attached a PDF copy for your records.\n\nThank you for choosing us!"
        
        # Prepare HTML Email Body
        letterhead_html = ""
        if active_shop and active_shop.use_custom_letterhead and active_shop.letterhead_url:
            lh_url = url_for('static', filename=f'uploads/mechanic/{active_shop.letterhead_url}', _external=True)
            letterhead_html = f'<div style="text-align: center; margin-bottom: 20px;"><img src="{lh_url}" alt="Shop Letterhead" style="max-width: 100%; height: auto; max-height: 150px; border-radius: 8px;"></div><hr style="border: 0; border-top: 1px solid #e2e8f0; margin-bottom: 20px;">'

        html = f"""{letterhead_html}
        <div style="font-family: sans-serif; color: #334155; max-width: 600px; margin: 0 auto;">
            <p>Hello,</p>
            <p>Your {doc_type} <strong>#{job_card.job_number}</strong> is ready. We have attached a PDF copy for your records.</p>
            <br>
            <p>Thank you for choosing us!</p>
        </div>"""

        # Generate PDF Attachment
        today_date = datetime.utcnow().strftime('%Y-%m-%d')
        from app.models.debtors import BusinessBankAccount
        bank_account = BusinessBankAccount.query.filter_by(user_id=job_card.vehicle.client.shop.user_id, is_default=True).first()
        pdf_html_content = render_template("program_mechanic/public_job_card.html", job_card=job_card, shop=active_shop, today_date=today_date, bank_account=bank_account)
        
        success = False
        try:
            pdf_bytes = html_to_pdf_bytes(pdf_html_content, base_url=request.host_url)
            file_name = f"{'Invoice' if job_card.status == 'Billed' else 'Quote'}_{job_card.job_number}.pdf"
            
            msg = Message(subject=subject, recipients=[target_email], body=body, html=html)
            msg.sender = current_app.config.get("MAIL_DEFAULT_SENDER")
            msg.attach(file_name, "application/pdf", pdf_bytes)
            mail.send(msg)
            success = True
        except Exception as e:
            current_app.logger.error(f"Failed to generate/send PDF: {e}")
            # Fallback to standard email without attachment
            success = send_email(subject=subject, recipients=[target_email], body=body, html=html)

        if success:
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
            db.session.commit()
            
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
    
    # Calculate total
    labor_total = sum(l.hours * l.rate_per_hour for l in job_card.labor_lines)
    parts_total = sum(p.quantity * p.markup_price for p in job_card.part_lines)
    subtotal = labor_total + parts_total
    vat_amount = subtotal * (job_card.vat_rate / 100.0)
    job_card_total = subtotal + vat_amount

    if not existing_charge and job_card_total > 0:
        charge_ledger = DebtorLedger(
            debtor_id=debtor.id,
            txn_date=datetime.utcnow(),
            kind='debit',
            amount=int(job_card_total * 100),
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
    return redirect(url_for("mechanic_bp.job_cards_list"))


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
    return redirect(url_for('mechanic_bp.client_ledger', debtor_id=debtor.id))


@mechanic_bp.route("/mechanic/quote/<int:id>/edit", methods=["GET", "POST"])
@login_required
def edit_quote(id):
    from app.models.mechanic import MechJobCard, MechLaborLine, MechPartLine, MechCatalogPart, MechShop
    
    job_card = MechJobCard.query.get_or_404(id)
    if job_card.status != 'Quote':
        flash("Only Quotes can be edited. This job card has already progressed.", "warning")
        return redirect(url_for('mechanic_bp.job_card_detail', id=id))
        
    active_shop = MechShop.query.filter_by(user_id=current_user.id, onboarding_status='active').first()
    
    if request.method == "POST":
        # Process dynamic labor and parts arrays
        labor_descs = request.form.getlist('labor_desc[]')
        labor_ins = request.form.getlist('labor_in[]')
        labor_outs = request.form.getlist('labor_out[]')
        labor_rates = request.form.getlist('labor_rate[]')

        part_qtys = request.form.getlist('part_qty[]')
        part_descs = request.form.getlist('part_desc[]')
        part_rates = request.form.getlist('part_rate[]')
        
        # Clear existing lines
        for l in job_card.labor_lines:
            db.session.delete(l)
        for p in job_card.part_lines:
            db.session.delete(p)
            
        # Add new lines
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
            
        # Update vehicle details
        vehicle = job_card.vehicle
        if vehicle:
            vin_number = request.form.get("vin_number")
            make = request.form.get("make")
            model = request.form.get("model")
            year_str = request.form.get("year")
            year = int(year_str) if year_str and year_str.isdigit() else None
            mileage = request.form.get("mileage")
            
            if vin_number: vehicle.vin = vin_number
            if make: vehicle.make = make
            if model: vehicle.model = model
            if year: vehicle.year = year
            if mileage and mileage.isdigit(): vehicle.mileage = int(mileage)
            
            # Client details
            customer_name = request.form.get("customer_name")
            if customer_name and vehicle.client:
                vehicle.client.name = customer_name
                
        try:
            db.session.commit()
            flash("Quote updated successfully!", "success")
            return redirect(url_for('mechanic_bp.job_card_detail', id=job_card.id))
        except Exception as e:
            db.session.rollback()
            flash(f"An error occurred while updating the quote. Error: {str(e)[:100]}", "danger")
            
    # GET Request: render form
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
    
    return render_template("program_mechanic/quote_form.html", catalog_parts=catalog_parts, shop=active_shop, edit_card=job_card)



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
            
        vehicle_reg = request.form.get("vehicle_reg", "Unknown")
        import uuid
        job_number = f"JOB-{uuid.uuid4().hex[:6].upper()}"
        
        wallet.balance -= token_cost
        txn = AitTokenTransaction(
            wallet_id=wallet.id,
            amount=-token_cost,
            description=f"Generated quote {job_number} for {vehicle_reg}"
        )
        db.session.add(txn)
        
        customer_name = request.form.get("customer_name")
        
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
        mileage = request.form.get("mileage")
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
        client = MechClient.query.filter_by(name=customer_name, user_id=current_user.id).first()
        if not client:
            client = MechClient(name=customer_name, user_id=current_user.id)
            db.session.add(client)
            db.session.flush()
        elif not client.user_id:
            client.user_id = current_user.id
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
        if mileage and mileage.isdigit(): vehicle.mileage = int(mileage)
        # Check if an AJAX-uploaded filename was passed as hidden input
        hidden_filename = request.form.get("uploaded_disk_filename")
        if hidden_filename:
            vehicle.license_disk_url = hidden_filename
        elif filename: 
            vehicle.license_disk_url = filename

            
        job_card = MechJobCard(
            job_number=job_number,
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
                
        try:
            db.session.commit()
            flash("Quote created and Job Card generated successfully!", "success")
            return redirect(url_for("mechanic_bp.job_card_detail", id=job_card.id))
        except Exception as e:
            db.session.rollback()
            flash(f"An error occurred while saving the quote. Your tokens were refunded. Error: {str(e).split('DETAIL:')[0].strip()[:100]}", "danger")
            return redirect(url_for('mechanic_bp.new_quote'))
        
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
            
    # Get debtors with balances
    from app.models.debtors import Debtor
    debtors_with_balances = []
    try:
        all_debtors = Debtor.query.filter_by(user_id=current_user.id).all()
        for d in all_debtors:
            total_debits = sum(l.amount for l in d.ledgers if l.kind == 'debit')
            total_credits = sum(l.amount for l in d.ledgers if l.kind == 'credit')
            bal = total_debits - total_credits
            if bal > 0:
                d.current_balance = bal / 100.0
                debtors_with_balances.append(d)
    except Exception as e:
        current_app.logger.error(f"Error loading debtors: {e}")
            
    return render_template("program_mechanic/job_cards_list.html", job_cards=job_cards, debtors_with_balances=debtors_with_balances)


@mechanic_bp.route("/upload_business_card", methods=["POST"])
@login_required
def upload_business_card():
    import os
    import json
    from werkzeug.utils import secure_filename
    import google.genai as genai
    from google.genai import types
    from flask import current_app, request, jsonify

    image_file = request.files.get("business_card_image")
    if not image_file or not image_file.filename:
        return jsonify({"error": "No image uploaded"}), 400

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        return jsonify({"error": "Gemini AI API key not configured on server"}), 500

    try:
        # Save temp file
        temp_dir = os.path.join(current_app.root_path, "static", "uploads", "temp")
        os.makedirs(temp_dir, exist_ok=True)
        filename = secure_filename(image_file.filename)
        filepath = os.path.join(temp_dir, filename)
        image_file.save(filepath)

        client = genai.Client(api_key=api_key)
        
        prompt = """
        Analyze this business card, letterhead, or storefront sign. Extract the following details for the business:
        - "business_name": The Name of the business.
        - "address": The physical address of the business.
        - "phone": The primary contact phone number.
        - "email": The primary contact email address.
        
        Return the result strictly as a valid JSON object with the exact keys: "business_name", "address", "phone", "email".
        If a detail cannot be clearly read or found, return an empty string "" for that key. Do not include markdown formatting like `json.
        """

        uploaded_file = client.files.upload(file=filepath)
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=[uploaded_file, prompt],
            config=types.GenerateContentConfig(response_mime_type="application/json")
        )
        
        try:
            os.remove(filepath)
        except Exception:
            pass

        parsed_data = json.loads(response.text.strip())
        
        return jsonify({"ai_data": parsed_data})

    except Exception as e:
        print(f"Gemini AI Error: {e}")
        print(f"Gemini AI Error: {e}")
        return jsonify({"error": str(e)}), 500

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
        file_path = os.path.join(upload_folder, filename)
        file.save(file_path)
        
        # AI Extraction
        ai_data = None
        try:
            from google import genai
            from google.genai import types
            from dotenv import load_dotenv
            import json
            
            dotenv_path = os.path.join(current_app.root_path, '..', '.env')
            load_dotenv(dotenv_path, override=True)
            
            api_key = os.environ.get("GEMINI_API_KEY") or current_app.config.get("GEMINI_API_KEY")
            if not api_key:
                try:
                    with open(dotenv_path, 'r', encoding='utf-8') as ef:
                        for line in ef:
                            if line.startswith('GEMINI_API_KEY='):
                                api_key = line.split('=', 1)[1].strip().strip('"').strip("'")
                                break
                except Exception:
                    pass
            
            if api_key:
                client = genai.Client(api_key=api_key)
                
                with open(file_path, "rb") as f_img:
                    file_bytes = f_img.read()
                    
                mime_type = file.mimetype
                if mime_type not in ['image/jpeg', 'image/png']:
                    mime_type = 'image/jpeg' # Fallback
                
                prompt = """
                Analyze this South African vehicle license disk. Extract the following details:
                - "vin": The 17-character VIN Number
                - "reg": The Vehicle Registration Number (License Plate)
                - "make": The Make of the vehicle (e.g., NISSAN)
                - "model": The Model or Description (e.g., Pick-up / Bakkie, or specific model if found)
                - "year": The year of the vehicle. You can often infer this from the "Date of test", "Date of liability", or the expiry date minus 1 year.
                - "engine_no": The Engine Number (Enjinnr.)
                - "gvm": The GVM / BVM value
                - "tare": The Tare / Tarra value
                - "disk_license_no": The printed License number (Lisensienr.) usually at the top or near the VIN.
                
                Return the result strictly as a valid JSON object with the exact keys: "vin", "reg", "make", "model", "year", "engine_no", "gvm", "tare", "disk_license_no".
                If a detail cannot be clearly read or found, return an empty string "" for that key. Do not include markdown formatting like `json.
                """
                
                response = client.models.generate_content(
                    model='gemini-2.5-flash',
                    contents=[types.Part.from_bytes(data=file_bytes, mime_type=mime_type), prompt],
                    config=types.GenerateContentConfig(response_mime_type="application/json")
                )
                
                ai_data = json.loads(response.text.strip())
        except Exception as e:
            current_app.logger.error(f"Failed to extract VIN details via AI: {e}")
        
        # Return URL for preview and the filename for saving
        file_url = url_for('static', filename=f'uploads/mechanic/{filename}')
        return jsonify({"url": file_url, "filename": filename, "ai_data": ai_data})

    return jsonify({"error": "Upload failed"}), 500

@mechanic_bp.route("/mechanic/log_call/<int:job_id>", methods=["POST"])
@login_required
def log_call(job_id):
    from app.models.mechanic import MechJobCard, MechCommunication
    job_card = MechJobCard.query.get_or_404(job_id)
    
    # Ensure they own it
    active_shop = MechShop.query.filter_by(user_id=current_user.id, onboarding_status='active').first()
    if not active_shop or job_card.vehicle.client.user_id != current_user.id:
        return {"error": "Unauthorized"}, 403
        
    phone = job_card.vehicle.client.phone or "Unknown"
    
    from app.models.auth import InviteLog
    ilog = InviteLog(
        sender_id=current_user.id,
        recipient_phone=phone,
        program_slug="mechanic",
        invite_type="Phone Call",
        status="Logged"
    )
    db.session.add(ilog)
    
    comm = MechCommunication(
        job_card_id=job_card.id,
        comm_type="Phone Call",
        recipient=phone,
        message="Initiated phone call",
        status="Logged"
    )
    db.session.add(comm)
    db.session.commit()
    
    return {"status": "success"}










@mechanic_bp.route('/mechanic/public/job_card/<job_number>')
def public_job_card(job_number):
    from app.models.mechanic import MechJobCard, MechShop
    from datetime import datetime
    
    job_card = MechJobCard.query.filter_by(job_number=job_number).first_or_404()
    shop = None
    if job_card.vehicle and job_card.vehicle.client and job_card.vehicle.client.user_id:
        shop = MechShop.query.filter_by(user_id=job_card.vehicle.client.user_id).first()
        
    today_date = datetime.utcnow().strftime('%Y-%m-%d')
    from app.models.debtors import BusinessBankAccount
    bank_account = BusinessBankAccount.query.filter_by(user_id=job_card.vehicle.client.shop.user_id, is_default=True).first()
    return render_template('program_mechanic/public_job_card.html', job_card=job_card, shop=shop, today_date=today_date, bank_account=bank_account)

@mechanic_bp.route("/mechanic/job_card/<int:id>/accept", methods=["POST"])
@login_required
def accept_quote(id):
    from app.models.debtors import Debtor, SenderProfile, DebtorLedger
    from datetime import datetime
    
    job_card = MechJobCard.query.get_or_404(id)
    if job_card.status == 'Quote':
        job_card.status = 'Awaiting Deposit'
        
        # Ensure Debtor profile exists
        client = job_card.vehicle.client
        debtor = None
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
                
            # Log the full quote amount as a Debit
            existing_charge = DebtorLedger.query.filter_by(
                debtor_id=debtor.id, 
                ref=f"JOB-{job_card.job_number}", 
                kind='debit'
            ).first()
            
            if not existing_charge:
                labor_total = sum(l.hours * l.rate_per_hour for l in job_card.labor_lines)
                parts_total = sum(p.quantity * p.markup_price for p in job_card.part_lines)
                subtotal = labor_total + parts_total
                vat_amount = subtotal * (job_card.vat_rate / 100.0)
                job_card_total = subtotal + vat_amount
                
                if job_card_total > 0:
                    charge_ledger = DebtorLedger(
                        debtor_id=debtor.id,
                        txn_date=db.func.current_date(),
                        kind='debit',
                        amount=int(job_card_total * 100),
                        description=f"Quote/Tax Invoice for Job #{job_card.job_number}",
                        ref=f"JOB-{job_card.job_number}"
                    )
                    db.session.add(charge_ledger)
        
        db.session.commit()
        flash("Quote accepted! Invoice posted to ledger.", "success")
        
        if debtor:
            return redirect(url_for('mechanic_bp.client_ledger', debtor_id=debtor.id))
            
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
            comm_type="System",
            message=f"Quote Rejected. Reason: {reason}"
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
        if job_card.status == 'Awaiting Deposit':
            job_card.status = 'Approved' # Moving forward if it was awaiting
        
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
                

            # Step 1: Charge the full quote amount as a Debit (if not already charged)
            existing_charge = DebtorLedger.query.filter_by(
                debtor_id=debtor.id, 
                ref=f"JOB-{job_card.job_number}", 
                kind='debit'
            ).first()
            
            # Calculate total
            labor_total = sum(l.hours * l.rate_per_hour for l in job_card.labor_lines)
            parts_total = sum(p.quantity * p.markup_price for p in job_card.part_lines)
            subtotal = labor_total + parts_total
            vat_amount = subtotal * (job_card.vat_rate / 100.0)
            job_card_total = subtotal + vat_amount
        
            if not existing_charge and job_card_total > 0:
                charge_ledger = DebtorLedger(
                    debtor_id=debtor.id,
                    txn_date=db.func.current_date(),
                    kind='debit',
                    amount=int(job_card_total * 100),
                    description=f"Quote/Tax Invoice for Job #{job_card.job_number}",
                    ref=f"JOB-{job_card.job_number}"
                )
                db.session.add(charge_ledger)
            
            # Step 2: Log payment as a credit transaction in DebtorLedger

            ledger = DebtorLedger(
                debtor_id=debtor.id,
                txn_date=db.func.current_date(),
                ref=f"DEP-{job_card.job_number}",
                description=f"Deposit for Job #{job_card.job_number}",
                amount=int(deposit_amount * 100),
                kind="credit"
            )
            db.session.add(ledger)
            
        db.session.commit()
        flash(f"Deposit of R {deposit_amount:.2f} recorded and synced to Debtors!", "success")
        
    return redirect(url_for('mechanic_bp.job_card_detail', id=id))


@mechanic_bp.route("/mechanic/client_accounts")
@login_required
def client_accounts():
    from app.models.debtors import Debtor
    
    try:
        debtors = Debtor.query.filter_by(user_id=current_user.id, slug_reference='mechanic').all()
        for d in debtors:
            total_debits = sum(l.amount for l in d.ledgers if l.kind == 'debit')
            total_credits = sum(l.amount for l in d.ledgers if l.kind == 'credit')
            d.current_balance = (total_debits - total_credits) / 100.0
    except Exception as e:
        debtors = []
        
    return render_template("program_mechanic/client_accounts.html", debtors=debtors)

@mechanic_bp.route("/mechanic/generate_debtors_schedule", methods=["POST"])
@login_required
def generate_debtors_schedule():
    from app.models.debtors import Debtor
    from app.models.mechanic import MechShop
    from app.models.auth import AitTokenWallet, AitTokenTransaction
    from app.models.billing import TokenTariff
    from datetime import datetime
    
    wallet = AitTokenWallet.query.filter_by(user_id=current_user.id).first()
    
    tariff = TokenTariff.query.filter_by(program_slug='mechanic', action_name='generate_schedule').first()
    if not tariff:
        tariff = TokenTariff(program_slug='mechanic', action_name='generate_schedule', base_token_cost=10)
        db.session.add(tariff)
        db.session.commit()
        
    token_cost = tariff.base_token_cost
    
    if not wallet or wallet.balance < token_cost:
        flash(f"You need {token_cost} tokens to generate a Debtors Schedule.", "error")
        return redirect(url_for("payment_bp.wallet_topup", subject_slug="mechanic"))
        
    # Deduct tokens
    wallet.balance -= token_cost
    txn = AitTokenTransaction(
        wallet_id=wallet.id,
        amount=-token_cost,
        description="Generated Debtors Schedule (Balance Sheet)"
    )
    db.session.add(txn)
    db.session.commit()
    
    start_date_str = request.form.get('start_date', '')
    end_date_str = request.form.get('end_date', '')
    
    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date() if start_date_str else None
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date() if end_date_str else None
    except ValueError:
        start_date = None
        end_date = None

    shop = MechShop.query.filter_by(user_id=current_user.id).first()

    try:
        debtors = Debtor.query.filter_by(user_id=current_user.id, slug_reference='mechanic').all()
        total_owed = 0
        
        for d in debtors:
            valid_ledgers = d.ledgers
            if start_date:
                valid_ledgers = [l for l in valid_ledgers if l.txn_date >= start_date]
            if end_date:
                valid_ledgers = [l for l in valid_ledgers if l.txn_date <= end_date]
                
            total_debits = sum(l.amount for l in valid_ledgers if l.kind == 'debit')
            total_credits = sum(l.amount for l in valid_ledgers if l.kind == 'credit')
            d.current_balance = (total_debits - total_credits) / 100.0
            
            if d.current_balance > 0:
                total_owed += d.current_balance
                
    except Exception as e:
        debtors = []
        total_owed = 0
        
    return render_template("program_mechanic/debtors_schedule.html", 
                           debtors=debtors, 
                           total_owed=total_owed,
                           start_date=start_date_str,
                           end_date=end_date_str,
                           shop=shop)



@mechanic_bp.route("/mechanic/client_ledger/<int:debtor_id>")
@login_required
def client_ledger(debtor_id):
    from app.models.debtors import Debtor
    from datetime import datetime
    
    debtor = Debtor.query.filter_by(id=debtor_id, user_id=current_user.id).first_or_404()
    
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')

    start_date = None
    end_date = None
    if start_date_str:
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        except ValueError:
            pass
    if end_date_str:
        try:
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
        except ValueError:
            pass

    ledgers_query = debtor.ledgers
    if start_date:
        ledgers_query = [l for l in ledgers_query if l.txn_date and l.txn_date >= start_date]
    if end_date:
        ledgers_query = [l for l in ledgers_query if l.txn_date and l.txn_date <= end_date]

    ledgers = sorted(ledgers_query, key=lambda l: (l.txn_date or datetime.min.date(), l.id))
    
    running_balance = 0
    for l in ledgers:
        if l.kind == 'debit':
            running_balance += l.amount
        else:
            running_balance -= l.amount
        l.running_balance = running_balance

    # Fetch job cards for this client
    from app.models.mechanic import MechJobCard, MechVehicle, MechClient
    job_cards = MechJobCard.query.join(MechVehicle).join(MechClient).filter(MechClient.name == debtor.name, MechClient.user_id == current_user.id).order_by(MechJobCard.created_at.desc()).all()

    return render_template("program_mechanic/client_ledger.html", 
                           debtor=debtor, 
                           ledgers=ledgers, 
                           job_cards=job_cards,
                           start_date=start_date_str, 
                           end_date=end_date_str)


@mechanic_bp.route("/mechanic/client_ledger/<int:debtor_id>/add_payment", methods=["POST"])
@login_required
def client_ledger_add_payment(debtor_id):
    from app.models.debtors import Debtor, DebtorLedger
    from datetime import datetime
    
    debtor = Debtor.query.filter_by(id=debtor_id, user_id=current_user.id).first_or_404()
    
    try:
        amount_str = request.form.get('amount', '0')
        amount = int(float(amount_str) * 100)
        ref = request.form.get('ref', '')
        desc = request.form.get('description', 'Payment')
        kind = request.form.get('kind', 'credit')
        
        if kind not in ['credit', 'debit']:
            kind = 'credit'
            
        date_str = request.form.get('date')
        txn_date = datetime.strptime(date_str, '%Y-%m-%d').date() if date_str else db.func.current_date()
        
        if amount > 0:
            ledger = DebtorLedger(
                debtor_id=debtor.id,
                txn_date=txn_date,
                ref=ref,
                description=desc,
                kind=kind,
                amount=amount
            )
            db.session.add(ledger)
            db.session.commit()
            flash(f"Payment of R {amount/100.0:.2f} recorded successfully.", "success")
        else:
            flash("Amount must be greater than 0.", "warning")
            
    except Exception as e:
        db.session.rollback()
        flash(f"Error recording payment: {e}", "danger")
        
    return redirect(url_for('mechanic_bp.client_ledger', debtor_id=debtor.id))


from app.models.debtors import BusinessBankAccount
from app.program_debtors.forms import BankAccountForm

@mechanic_bp.route("/mechanic/bank_accounts")
@login_required
def bank_accounts():
    bank_accounts = BusinessBankAccount.query.filter_by(user_id=current_user.id).order_by(BusinessBankAccount.created_at.desc()).all()
    bank_form = BankAccountForm()
    return render_template("program_mechanic/bank_accounts.html", bank_accounts=bank_accounts, bank_form=bank_form)


@mechanic_bp.route("/mechanic/add_bank_account", methods=["POST"])
@login_required
def add_bank_account():
    form = BankAccountForm()
    if form.validate_on_submit():
        if form.is_default.data:
            BusinessBankAccount.query.filter_by(user_id=current_user.id).update({'is_default': False})

        new_acc = BusinessBankAccount(
            user_id=current_user.id,
            bank_name=form.bank_name.data,
            account_name=form.account_name.data,
            account_number=form.account_number.data,
            bsb_branch=form.bsb_branch.data,
            swift_code=form.swift_code.data,
            is_default=form.is_default.data
        )

        if BusinessBankAccount.query.filter_by(user_id=current_user.id).count() == 0:
            new_acc.is_default = True

        db.session.add(new_acc)
        db.session.commit()
        flash("Bank account added successfully.", "success")
    return redirect(url_for("mechanic_bp.bank_accounts"))


@mechanic_bp.route("/mechanic/bank_account/<int:account_id>/set_default", methods=["POST"])
@login_required
def set_default_bank_account(account_id):
    acc = BusinessBankAccount.query.filter_by(id=account_id, user_id=current_user.id).first_or_404()
    BusinessBankAccount.query.filter_by(user_id=current_user.id).update({'is_default': False})
    acc.is_default = True
    db.session.commit()
    flash("Default bank account updated.", "success")
    return redirect(url_for("mechanic_bp.bank_accounts"))


@mechanic_bp.route("/mechanic/bank_account/<int:account_id>/delete", methods=["POST"])
@login_required
def delete_bank_account(account_id):
    acc = BusinessBankAccount.query.filter_by(id=account_id, user_id=current_user.id).first_or_404()
    db.session.delete(acc)
    db.session.commit()
    flash("Bank account deleted.", "info")
    return redirect(url_for("mechanic_bp.bank_accounts"))


@mechanic_bp.route("/mechanic/job_card/<int:id>/edit_vehicle", methods=["POST"])
@login_required
def edit_job_vehicle(id):
    from app.models.mechanic import MechJobCard
    job_card = MechJobCard.query.get_or_404(id)
    
    # Ensure ownership
    if job_card.vehicle.client.user_id != current_user.id:
        flash("Unauthorized", "error")
        return redirect(url_for('mechanic_bp.mechanic_dashboard'))
        
    job_card.vehicle.make = request.form.get("make")
    job_card.vehicle.model = request.form.get("model")
    job_card.vehicle.vin = request.form.get("vin")
    
    year_str = request.form.get("year")
    if year_str and year_str.isdigit():
        job_card.vehicle.year = int(year_str)
    else:
        job_card.vehicle.year = None
    
    mileage_str = request.form.get("mileage")
    if mileage_str and mileage_str.isdigit():
        job_card.vehicle.mileage = int(mileage_str)
        
    job_card.next_service_due = request.form.get("next_service_due")
    
    db.session.commit()
    flash("Vehicle details updated successfully.", "success")
    return redirect(url_for('mechanic_bp.job_card_detail', id=job_card.id))
