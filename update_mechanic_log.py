with open('templates/program_mechanic/email_preview.html', 'r', encoding='utf-8') as f:
    content = f.read()

email_original = '''                <div class="mb-8">
                    <label for="email" class="block text-sm font-bold text-slate-700 mb-2">
                        Client Email Address
                    </label>
                    <input 
                        type="email" 
                        id="email" 
                        name="email" 
                        value="{{ default_email }}"
                        required
                        autofocus
                        class="w-full px-4 py-3 rounded-lg border-2 border-slate-300 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 shadow-sm transition"
                        placeholder="e.g. client@example.com"
                    >
                </div>'''

email_new = '''                <div class="mb-6">
                    <label for="email" class="block text-sm font-bold text-slate-700 mb-2">
                        Client Email Address
                    </label>
                    <input 
                        type="email" 
                        id="email" 
                        name="email" 
                        value="{{ default_email }}"
                        required
                        autofocus
                        class="w-full px-4 py-3 rounded-lg border-2 border-slate-300 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 shadow-sm transition"
                        placeholder="e.g. client@example.com"
                    >
                </div>
                
                <div class="mb-4 bg-slate-50 border border-slate-200 rounded-lg p-4">
                    <p class="text-xs font-bold text-slate-500 uppercase tracking-wider mb-3">Optional: Log Communications</p>
                    <label class="flex items-center gap-3 mb-3 cursor-pointer">
                        <input type="checkbox" name="log_whatsapp" value="1" class="rounded border-slate-300 text-indigo-600 focus:ring-indigo-500 w-4 h-4">
                        <span class="text-sm font-medium text-slate-700">I have also sent a WhatsApp message</span>
                    </label>
                    <label class="flex items-center gap-3 cursor-pointer">
                        <input type="checkbox" name="log_phone" value="1" class="rounded border-slate-300 text-indigo-600 focus:ring-indigo-500 w-4 h-4">
                        <span class="text-sm font-medium text-slate-700">I have also called the client via phone</span>
                    </label>
                </div>'''

content = content.replace(email_original, email_new)
with open('templates/program_mechanic/email_preview.html', 'w', encoding='utf-8') as f:
    f.write(content)

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    routes_content = f.read()

backend_original = '''        send_pdf_email(
            to_email=to_email,
            subject=f"ProTrade {doc_type} - {job_card.job_number}",
            body_text=f"Dear {job_card.vehicle.client.name},\\n\\nPlease find attached your {doc_type}.\\n\\nThank you.",
            pdf_bytes=pdf_bytes,
            filename=filename
        )
        flash(f"{doc_type} successfully emailed to {to_email}", "success")
    except Exception as e:'''

backend_new = '''        send_pdf_email(
            to_email=to_email,
            subject=f"ProTrade {doc_type} - {job_card.job_number}",
            body_text=f"Dear {job_card.vehicle.client.name},\\n\\nPlease find attached your {doc_type}.\\n\\nThank you.",
            pdf_bytes=pdf_bytes,
            filename=filename
        )
        
        # Log Communications
        from app.models.auth import InviteLog
        log_whatsapp = request.form.get("log_whatsapp")
        log_phone = request.form.get("log_phone")
        if log_whatsapp:
            db.session.add(InviteLog(sender_id=current_user.id, recipient_phone=job_card.vehicle.client.phone or "Unknown", program_slug="mechanic", invite_type="WhatsApp Message", status="Logged"))
        if log_phone:
            db.session.add(InviteLog(sender_id=current_user.id, recipient_phone=job_card.vehicle.client.phone or "Unknown", program_slug="mechanic", invite_type="Phone Call", status="Logged"))
        if log_whatsapp or log_phone:
            db.session.commit()
            
        flash(f"{doc_type} successfully emailed to {to_email}", "success")
    except Exception as e:'''

routes_content = routes_content.replace(backend_original, backend_new)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(routes_content)

print("Updated Mechanic email logging")
