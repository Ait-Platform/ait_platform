import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

elec_old = '''def build_electrical_rows(property_id, month, is_exception=False):'''
elec_new = '''def build_electrical_rows(property_id, month, is_exception=False, filter_meter_ids=None):'''
text = text.replace(elec_old, elec_new)

elec_old_loop = '''    for r in tenant_records:
        if r.meter_id not in active_meter_ids:
            continue'''
elec_new_loop = '''    for r in tenant_records:
        if r.meter_id not in active_meter_ids:
            continue
        if filter_meter_ids is not None and r.meter_id not in filter_meter_ids:
            continue'''
text = text.replace(elec_old_loop, elec_new_loop)

water_old = '''def build_water_rows(property_id, month, is_exception=False):'''
water_new = '''def build_water_rows(property_id, month, is_exception=False, filter_meter_ids=None):'''
text = text.replace(water_old, water_new)

water_old_loop = '''    for r in tenant_records:
        if r.meter_id not in active_meter_ids:
            continue'''
water_new_loop = '''    for r in tenant_records:
        if r.meter_id not in active_meter_ids:
            continue
        if filter_meter_ids is not None and r.meter_id not in filter_meter_ids:
            continue'''
text = text.replace(water_old_loop, water_new_loop)

# Generate SOA logic substitution
old_generate = '''@billing_bp.route("/billing/soa/tenant/<int:tenant_id>/generate", methods=["GET"])
@login_required
def generate_soa(tenant_id):
    from app.models.billing import BilTenant
    tenant = BilTenant.query.get_or_404(tenant_id)
    # Placeholder for actual SOA generation logic which aggregates MetSoa + Arrears
    flash("SOA Generation logic to be implemented. This will pull METSOA charges and tenant arrears.", "info")
    return redirect(url_for("billing_bp.soa_dashboard"))'''

new_generate = '''@billing_bp.route("/billing/soa/tenant/<int:tenant_id>/generate", methods=["GET"])
@login_required
def generate_soa(tenant_id):
    from app.models.billing import BilTenant, BilMuniAccount
    import tempfile
    import os
    from flask import send_file
    
    tenant = BilTenant.query.get_or_404(tenant_id)
    month = request.args.get("month")
    
    if not month:
        flash("Month is required.", "danger")
        return redirect(url_for("billing_bp.soa_dashboard"))
        
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
    
    # If the user clicks print in the browser
    if request.args.get("view") == "html":
        return html_string
        
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
            
        return send_file(pdf_path, as_attachment=True, download_name=f"SOA_{tenant.name.replace(' ', '_')}_{month}.pdf")
    except Exception as e:
        import traceback
        traceback.print_exc()
        flash(f"Failed to generate SOA: {str(e)}", "danger")
        return redirect(url_for('billing_bp.soa_dashboard', property_id=prop.id, month=month))
'''

text = text.replace(old_generate, new_generate)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)

print('Successfully updated routes.py')
