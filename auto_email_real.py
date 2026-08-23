import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

regex = r'(\s*db\.session\.commit\(\)\s*flash\("Quote accepted! Invoice posted to ledger\.", "success"\)\s*)(if debtor:\s*return redirect\(url_for\(\'mechanic_bp\.job_card_detail\', id=job_card\.id\)\)\s*return redirect\(url_for\(\'mechanic_bp\.job_card_detail\', id=id\)\))'

email_logic = '''
        # AUTO-EMAIL TAX INVOICE LOGIC
        if client and client.email:
            try:
                from app.utils.mailer import send_pdf_email
                from app.utils.pdf_render import html_to_pdf_bytes
                
                active_shop = MechShop.query.filter_by(user_id=current_user.id, onboarding_status='active').first()
                from app.models.debtors import BusinessBankAccount
                bank_account = BusinessBankAccount.query.filter_by(user_id=current_user.id).order_by(BusinessBankAccount.is_default.desc()).first()
                
                doc_type = "Tax Invoice"
                subject = f"Your {doc_type} #{job_card.job_number} from {active_shop.business_name if active_shop else 'AIT ProTrade'}"
                
                body = f"Hello,\\n\\nYour {doc_type} #{job_card.job_number} is ready. We have attached a PDF copy for your records.\\n\\nThank you for choosing us!"
                
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
                
                today_date = db.func.current_date()
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
            flash("Quote Confirmed! WARNING: Client has no email address. Please PRINT and hand them the Tax Invoice manually.", "warning")
            
    \\2'''

# Notice I replaced \1 entirely with just \2 + new logic, which means I will lose db.session.commit(). Let's include db.session.commit()
email_logic = '''
        db.session.commit()
        # AUTO-EMAIL TAX INVOICE LOGIC
        if client and client.email:
            try:
                from app.utils.mailer import send_pdf_email
                from app.utils.pdf_render import html_to_pdf_bytes
                from datetime import datetime
                
                active_shop = MechShop.query.filter_by(user_id=current_user.id, onboarding_status='active').first()
                from app.models.debtors import BusinessBankAccount
                bank_account = BusinessBankAccount.query.filter_by(user_id=current_user.id).order_by(BusinessBankAccount.is_default.desc()).first()
                
                doc_type = "Tax Invoice"
                subject = f"Your {doc_type} #{job_card.job_number} from {active_shop.business_name if active_shop else 'AIT ProTrade'}"
                
                body = f"Hello,\\n\\nYour {doc_type} #{job_card.job_number} is ready. We have attached a PDF copy for your records.\\n\\nThank you for choosing us!"
                
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
            flash("Quote Confirmed! WARNING: Client has no email address. Please print and hand them the Tax Invoice manually.", "warning")
            
        \\2'''

content = re.sub(regex, email_logic, content)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
