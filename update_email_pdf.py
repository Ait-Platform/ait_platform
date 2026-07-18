import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

replacement = '''
@billing_bp.route("/billing/utilities/<int:property_id>/consumption/<month>/email", methods=["POST"])
@login_required
def email_consumption(property_id, month):
    from app.models.billing import BilProperty, BilSectionalUnit, BilMuniAccount, BilMeter, BilConsumption
    from app.utils.mailer import send_pdf_email
    from app.utils.pdf_render import html_to_pdf_bytes
    from flask import request, current_app, render_template
    
    data = request.get_json()
    email = data.get("email") if data else None
    
    if not email:
        return {"success": False, "error": "Email address is required"}, 400
        
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id and not current_user.has_role('admin'):
        return {"success": False, "error": "Unauthorized"}, 403
        
    units = BilSectionalUnit.query.filter_by(property_id=prop.id).all()
    all_meters = []
    for u in units:
        all_meters.extend(u.meters)
        
    muni_accounts = BilMuniAccount.query.filter_by(property_id=prop.id).all()
    muni_acc_numbers = [acc.account_number for acc in muni_accounts if acc.account_number]
    if muni_acc_numbers:
        muni_meters = BilMeter.query.filter(BilMeter.municipal_bill_number.in_(muni_acc_numbers)).all()
        for m in muni_meters:
            if m not in all_meters:
                all_meters.append(m)
                
    for acc in muni_accounts:
        if acc.water_meter and acc.water_meter not in all_meters:
            all_meters.append(acc.water_meter)
        if acc.elec_meter and acc.elec_meter not in all_meters:
            all_meters.append(acc.elec_meter)
            
    meter_ids = [m.id for m in all_meters]
    if meter_ids:
        consumptions = BilConsumption.query.filter(
            BilConsumption.meter_id.in_(meter_ids),
            BilConsumption.month == month
        ).all()
    else:
        consumptions = []
    
    cons_map = {c.meter_id: c for c in consumptions}
    
    data = []
    for m in all_meters:
        c = cons_map.get(m.id)
        data.append({
            'meter': m,
            'consumption': c
        })
        
    html = render_template("program_billing/consumption_table.html", property=prop, month=month, data=data)
    
    # Hide the Print/Email buttons in the PDF
    html = html.replace('<button onclick="sendEmail()"', '<button style="display:none;"')
    html = html.replace('<button onclick="window.print()"', '<button style="display:none;"')
    
    try:
        pdf_bytes = html_to_pdf_bytes(html)
    except Exception as e:
        return {"success": False, "error": "Failed to generate PDF: " + str(e)}
        
    subject = f"Consumption Review - {prop.name} - {month}"
    body = f"Hello,\\n\\nPlease find the consumption review for {prop.name} for the billing month of {month} attached as a PDF.\\n\\nRegards,\\nAIT Platform"
    
    try:
        success = send_pdf_email(email, subject, body, pdf_bytes, filename=f"Consumption_Review_{prop.name}_{month}.pdf")
        if success:
            return {"success": True}
        else:
            return {"success": False, "error": "Mailer returned false."}
    except Exception as e:
        return {"success": False, "error": str(e)}
'''

# Find the start of email_consumption
start_idx = text.find('@billing_bp.route("/billing/utilities/<int:property_id>/consumption/<month>/email", methods=["POST"])')
if start_idx != -1:
    text = text[:start_idx] + replacement.strip()

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)

print("Replaced email_consumption with PDF attachment version")
