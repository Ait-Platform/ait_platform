import re

with open('templates/program_debtors/soa_template.html', 'r', encoding='utf-8') as f:
    content = f.read()

email_original = '''        <div class="mb-6 mt-4">
          <label class="block text-sm font-semibold text-gray-700 mb-2">Recipient Email Address</label>
          <input type="email" name="to_email" value="{{ debtor.email or '' }}" required class="w-full px-3 py-2 border border-gray-300 rounded focus:outline-none focus:border-blue-500" placeholder="client@example.com">
          <p class="text-xs text-gray-500 mt-2">You can change this before sending.</p>
        </div>'''

email_new = '''        <div class="mb-4 mt-4">
          <label class="block text-sm font-semibold text-gray-700 mb-2">Recipient Email Address</label>
          <input type="email" name="to_email" value="{{ debtor.email or '' }}" required class="w-full px-3 py-2 border border-gray-300 rounded focus:outline-none focus:border-blue-500" placeholder="client@example.com">
          <p class="text-xs text-gray-500 mt-2">You can change this before sending.</p>
        </div>
        
        <div class="mb-6 border-t border-gray-100 pt-4">
          <p class="text-xs font-bold text-gray-500 uppercase tracking-wider mb-3">Optional: Log Communications</p>
          <label class="flex items-center gap-2 mb-2 cursor-pointer">
            <input type="checkbox" name="log_whatsapp" value="1" class="rounded border-gray-300 text-blue-600 focus:ring-blue-500 w-4 h-4">
            <span class="text-sm font-medium text-slate-700">I have also sent a WhatsApp message</span>
          </label>
          <label class="flex items-center gap-2 cursor-pointer">
            <input type="checkbox" name="log_phone" value="1" class="rounded border-gray-300 text-blue-600 focus:ring-blue-500 w-4 h-4">
            <span class="text-sm font-medium text-slate-700">I have also called the client via phone</span>
          </label>
        </div>'''

content = content.replace(email_original, email_new)

with open('templates/program_debtors/soa_template.html', 'w', encoding='utf-8') as f:
    f.write(content)

with open('app/program_debtors/routes.py', 'r', encoding='utf-8') as f:
    routes_content = f.read()

backend_original = '''        send_pdf_email(
            to_email=to_email,
            subject=f"Statement of Account - {profile.business_name if profile else 'Billing'}",
            body_text=f"Dear {debtor.name},\\n\\nPlease find attached your latest Statement of Account.\\n\\nThank you.",
            pdf_bytes=pdf_bytes,
            filename=f"SOA_{debtor.name.replace(' ', '_')}.pdf"
        )
        flash(f"Statement successfully emailed to {to_email}", "success")
    except Exception as e:'''

backend_new = '''        send_pdf_email(
            to_email=to_email,
            subject=f"Statement of Account - {profile.business_name if profile else 'Billing'}",
            body_text=f"Dear {debtor.name},\\n\\nPlease find attached your latest Statement of Account.\\n\\nThank you.",
            pdf_bytes=pdf_bytes,
            filename=f"SOA_{debtor.name.replace(' ', '_')}.pdf"
        )
        
        # Log Communications
        from app.models.auth import InviteLog
        log_whatsapp = request.form.get("log_whatsapp")
        log_phone = request.form.get("log_phone")
        program_slug = "debtors"
        if debtor.slug_reference == 'mechanic':
            program_slug = "mechanic"
            
        if log_whatsapp:
            db.session.add(InviteLog(sender_id=current_user.id, recipient_phone=debtor.phone or "Unknown", program_slug=program_slug, invite_type="WhatsApp Message", status="Logged"))
        if log_phone:
            db.session.add(InviteLog(sender_id=current_user.id, recipient_phone=debtor.phone or "Unknown", program_slug=program_slug, invite_type="Phone Call", status="Logged"))
        if log_whatsapp or log_phone:
            db.session.commit()
            
        flash(f"Statement successfully emailed to {to_email}", "success")
    except Exception as e:'''

routes_content = routes_content.replace(backend_original, backend_new)

with open('app/program_debtors/routes.py', 'w', encoding='utf-8') as f:
    f.write(routes_content)

print("Updated Debtors email logging")
