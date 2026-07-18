import os
import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Update build_electrical_rows signature and logic
elec_old = '''def build_electrical_rows(property_id, month):
    rows = []
    total_due = 0

    meters = get_all_property_meters(property_id)
    active_meter_ids = [m.id for m in meters if (m.status or 'active').lower() == 'active']'''

elec_new = '''def build_electrical_rows(property_id, month, is_exception=False):
    rows = []
    total_due = 0

    meters = get_all_property_meters(property_id)
    if is_exception:
        active_meter_ids = [m.id for m in meters if (m.status or 'active').lower() != 'active']
    else:
        active_meter_ids = [m.id for m in meters if (m.status or 'active').lower() == 'active']'''

text = text.replace(elec_old, elec_new)

# 2. Update build_water_rows signature and logic
water_old = '''def build_water_rows(property_id, month):
    water_meters = []
    total_water_due = 0

    meters = get_all_property_meters(property_id)
    active_meter_ids = [m.id for m in meters if (m.status or 'active').lower() == 'active']'''

water_new = '''def build_water_rows(property_id, month, is_exception=False):
    water_meters = []
    total_water_due = 0

    meters = get_all_property_meters(property_id)
    if is_exception:
        active_meter_ids = [m.id for m in meters if (m.status or 'active').lower() != 'active']
    else:
        active_meter_ids = [m.id for m in meters if (m.status or 'active').lower() == 'active']'''

text = text.replace(water_old, water_new)

# 3. Update utilities_hub
hub_old = '''        elif action == "exceptions":
            return redirect(url_for("billing_bp.meter_exceptions", property_id=property_id))'''

hub_new = '''        elif action == "exceptions":
            return redirect(url_for("billing_bp.exception_metsoa", property_id=property_id, month=month))'''

text = text.replace(hub_old, hub_new)

# 4. Add exception_metsoa and email_exception_metsoa routes
new_routes = '''
@billing_bp.route("/billing/utilities/<int:property_id>/exception_metsoa/<month>")
@login_required
def exception_metsoa(property_id, month):
    from app.models.billing import BilProperty
    
    prop = BilProperty.query.get_or_404(property_id)

    if prop:
        manager_id = prop.manager_id
        from app.models.billing import BilStatementPayment
        payment = BilStatementPayment.query.filter_by(manager_id=manager_id, month=month).first()
        if not payment or payment.amount_paid_cents <= 0:
            if current_user.id == manager_id:
                flash(f"Please unlock statements for {month} before viewing or generating PDFs.", "warning")
                return redirect(url_for('billing_bp.billing_checkout', month=month))
            elif current_user.has_role('admin'):
                pass
            else:
                flash("Your manager has not unlocked statements for this month yet.", "danger")
                return redirect(url_for("public_bp.welcome"))

    elec_rows, elec_total = build_electrical_rows(prop.id, month, is_exception=True)
    water_meters, water_total = build_water_rows(prop.id, month, is_exception=True)
    
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

    return render_template("program_billing/exception_metsoa.html", **data)

@billing_bp.route("/billing/utilities/<int:property_id>/exception_metsoa/<month>/email", methods=["POST"])
@login_required
def email_exception_metsoa(property_id, month):
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

    elec_rows, elec_total = build_electrical_rows(prop.id, month, is_exception=True)
    water_meters, water_total = build_water_rows(prop.id, month, is_exception=True)
    
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

    html_string = render_template("program_billing/exception_metsoa.html", **data)
    
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
            
        subject = f"Exception METSOA Review - {prop.name} - {month}"
        body = f"Hello,\\n\\nPlease find the Exception METSOA statement for {prop.name} for the billing month of {month} attached as a PDF.\\n\\nRegards,\\nAIT Platform"
        
        success = send_pdf_email(email, subject, body, pdf_bytes, filename=f"{month}-Exception-MetSoa-{prop.name}.pdf")
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

idx = text.find('def metsoa(')
idx = text.rfind('@billing_bp', 0, idx)

new_text = text[:idx] + new_routes + '\n' + text[idx:]

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(new_text)

print('Updated routes.py')
