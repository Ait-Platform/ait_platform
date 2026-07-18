import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

email_route = '''
@billing_bp.route("/billing/utilities/<int:property_id>/metsoa/<month>/email", methods=["POST"])
@login_required
def email_metsoa(property_id, month):
    from app.models.billing import BilProperty
    from flask import request
    from app.utils.mailer import send_pdf_email
    import tempfile
    import os
    
    data_req = request.get_json()
    email = data_req.get("email") if data_req else None
    if not email:
        return {"success": False, "error": "Email address is required"}, 400
        
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id and not current_user.has_role('admin'):
        return {"success": False, "error": "Unauthorized"}, 403

    elec_rows, elec_total = build_electrical_rows(prop.id, month)
    water_meters, water_total = build_water_rows(prop.id, month)
    
    grand_total = round(elec_total + water_total, 2)
    
    data = {
        "tenant": None,
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
        "grand_total": grand_total
    }

    html_string = render_template("program_billing/metsoa.html", **data)
    
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
                margin={"top": "30px", "bottom": "40px", "left": "10px", "right": "10px"}
            )
            browser.close()
            
        with open(pdf_path, 'rb') as f_pdf:
            pdf_bytes = f_pdf.read()
            
        os.remove(pdf_path)
            
        subject = f"METSOA Review - {prop.name} - {month}"
        body = f"Hello,\\n\\nPlease find the METSOA statement for {prop.name} for the billing month of {month} attached as a PDF.\\n\\nRegards,\\nAIT Platform"
        
        success = send_pdf_email(email, subject, body, pdf_bytes, filename=f"{month}-MetSoa-{prop.name}.pdf")
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

# insert it before metsoa_pdf
idx = text.find('def metsoa_pdf(')
idx = text.rfind('@billing_bp', 0, idx)

new_text = text[:idx] + email_route + '\n' + text[idx:]

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(new_text)
print('Updated routes.py with email_metsoa')
