with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

start_idx = 0
for i, line in enumerate(lines):
    if "def accept_quote(id):" in line:
        start_idx = i
        break

end_idx = 0
for i in range(start_idx, len(lines)):
    if "def record_deposit(id):" in lines[i]:
        end_idx = i - 2
        break

# We will replace the entire accept_quote function with a clean, perfectly indented version!
clean_function = '''def accept_quote(id):
    from app.models.debtors import Debtor, SenderProfile, DebtorLedger
    from datetime import datetime
    from app.models.auth import AitTokenWallet, AitTokenTransaction
    from sqlalchemy import text
    from app.utils.mailer import send_pdf_email
    from app.utils.pdf_render import html_to_pdf_bytes
    
    job_card = MechJobCard.query.get_or_404(id)
    if job_card.status == 'Quote':
        
        # Charge tokens for generating Tax Invoice
        setting = db.session.execute(text("SELECT value FROM system_settings WHERE key = 'mechanic_quote_cents'")).fetchone()
        quote_cost = int(setting[0]) if setting else 500
        token_cost = quote_cost // 100
        
        wallet = AitTokenWallet.query.filter_by(user_id=current_user.id).first()
        if not wallet or wallet.balance < token_cost:
            flash("Insufficient tokens to generate Tax Invoice. Please top up your wallet.", "danger")
            return redirect(url_for("mechanic_bp.mock_bill"))
            
        wallet.balance -= token_cost
        txn = AitTokenTransaction(
            wallet_id=wallet.id,
            amount=-token_cost,
            description=f"Generated and sent Tax Invoice {job_card.job_number}"
        )
        db.session.add(txn)
        
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
        
        # AUTO-EMAIL TAX INVOICE LOGIC
        if client and client.email:
            try:
                active_shop = MechShop.query.filter_by(user_id=current_user.id, onboarding_status='active').first()
                from app.models.debtors import BusinessBankAccount
                bank_account = BusinessBankAccount.query.filter_by(user_id=current_user.id).order_by(BusinessBankAccount.is_default.desc()).first()
                
                doc_type = "Tax Invoice"
                subject = f"Your {doc_type} #{job_card.job_number} from {active_shop.business_name if active_shop else 'AIT ProTrade'}"
                
                body = f"""Hello,\\n\\nYour {doc_type} #{job_card.job_number} is ready. We have attached a PDF copy for your records.\\n\\nThank you for choosing us!"""
                
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
                
                today_date = datetime.utcnow().strftime('%Y-%m-%d')
                pdf_html_content = render_template("program_mechanic/public_job_card.html", job_card=job_card, shop=active_shop, today_date=today_date, bank_account=bank_account)
                pdf_bytes = html_to_pdf_bytes(pdf_html_content, base_url=request.host_url)
                
                file_name = f"{doc_type.replace(' ', '_')}_{job_card.job_number}.pdf"
                
                send_pdf_email(
                    to_email=client.email,
                    subject=subject,
                    body=body,
                    html=html,
                    pdf_bytes=pdf_bytes,
                    filename=file_name
                )
                
                flash("Quote Confirmed and Tax Invoice automatically emailed to client!", "success")
                
            except Exception as e:
                current_app.logger.error(f"Auto-email failed: {e}")
                flash("Quote Confirmed! WARNING: Auto-email failed. Please print and send the Tax Invoice manually.", "warning")
        else:
            flash("Quote Confirmed! WARNING: Client has no email address. Please WhatsApp or Print the Tax Invoice manually to remain legally compliant.", "warning")
            
        if debtor:
            return redirect(url_for('mechanic_bp.job_card_detail', id=job_card.id))
            
    return redirect(url_for('mechanic_bp.job_card_detail', id=id))
'''

new_lines = lines[:start_idx] + [clean_function + '\n'] + lines[end_idx:]

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.writelines(new_lines)
