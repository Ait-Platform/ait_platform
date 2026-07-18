import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

email_soa = '''@billing_bp.route("/billing/soa/tenant/<int:tenant_id>/<month>/email", methods=["POST"])
@login_required
def email_soa(tenant_id, month):
    from app.models.billing import BilTenant, BilMuniAccount
    import tempfile
    import os
    from flask import send_file, request
    from app.utils.mailer import send_pdf_email
    
    tenant = BilTenant.query.get_or_404(tenant_id)
    
    data_req = request.get_json()
    email = data_req.get("email") if data_req else None
    if not email:
        return {"success": False, "error": "Email address is required"}, 400
        
    prop = tenant.sectional_unit.property
    
    tenant_meter_ids = [m.id for m in tenant.sectional_unit.meters]
    
    elec_rows, elec_total = build_electrical_rows(prop.id, month, filter_meter_ids=tenant_meter_ids)
    water_meters, water_total = build_water_rows(prop.id, month, filter_meter_ids=tenant_meter_ids)
    
    # Calculate mapped charges
    muni_accounts = BilMuniAccount.query.filter_by(property_id=prop.id).all()
    
    mapped_charges = []
    mapped_total = 0.0
    
    for acc in muni_accounts:
        if acc.rates_charge_to == 'tenant':
            val = round((acc.rates_general_monthly or 0) + (acc.rates_sra_monthly or 0), 2)
            if val > 0:
                mapped_charges.append({"description": "Rates & SRA", "amount": val})
                mapped_total += val
                
        if acc.arrears_charge_to == 'tenant':
            if acc.arrears_amount and acc.arrears_amount > 0:
                mapped_charges.append({"description": "Arrears", "amount": acc.arrears_amount})
                mapped_total += acc.arrears_amount
                
        if acc.arrangement_charge_to == 'tenant':
            if acc.ca_installment_amount and acc.ca_installment_amount > 0:
                mapped_charges.append({"description": "Arrangement Installment", "amount": acc.ca_installment_amount})
                mapped_total += acc.ca_installment_amount
                
    grand_total = round(elec_total + water_total + mapped_total, 2)
    
    data = {
        "tenant": tenant,
        "property": prop,
        "month": month,
        "electricity": {
            "rows": elec_rows,
            "subtotal": elec_total
        },
        "water": {
            "meters": water_meters,
            "total": water_total
        },
        "mapped_charges": mapped_charges,
        "mapped_total": mapped_total,
        "grand_total": grand_total
    }
    
    html_string = render_template("program_billing/soa_document.html", **data)
    
    # Hide the action buttons in the PDF
    html_string = html_string.replace('class="print:hidden', 'style="display:none;"')
    
    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
        pdf_path = tmp.name

    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            page.set_content(html_string, wait_until="networkidle")
            page.pdf(
                path=pdf_path, 
                format="A4", 
                print_background=True,
                display_header_footer=True,
                header_template="<span></span>",
                footer_template="<div style='width: 100%; text-align: center; font-size: 10px; color: #6b7280; padding-bottom: 10px;'>Page <span class='pageNumber'></span> of <span class='totalPages'></span></div>",
                margin={"top": "30px", "bottom": "40px", "left": "20px", "right": "20px"}
            )
            browser.close()
            
        with open(pdf_path, 'rb') as f_pdf:
            pdf_bytes = f_pdf.read()
            
        os.remove(pdf_path)
            
        subject = f"Statement of Account - {tenant.name} - {month}"
        body = f"Hello {tenant.name},\\n\\nPlease find your Statement of Account for the billing month of {month} attached as a PDF.\\n\\nRegards,\\n{prop.name} Management"
        
        success = send_pdf_email(email, subject, body, pdf_bytes, filename=f"SOA_{tenant.name.replace(' ', '_')}_{month}.pdf")
        if success:
            return {"success": True}
        else:
            return {"success": False, "error": "Mailer returned false."}
            
    except Exception as e:
        import traceback
        traceback.print_exc()
        if os.path.exists(pdf_path):
            os.remove(pdf_path)
        return {"success": False, "error": str(e)}
'''

idx = text.find('def generate_soa(')
idx = text.rfind('@billing_bp', 0, idx)

new_text = text[:idx] + email_soa + '\n' + text[idx:]

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(new_text)

print("Added email_soa to routes.py")
