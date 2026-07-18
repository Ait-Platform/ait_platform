import re
with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

replacement = '''
@billing_bp.route("/billing/metsoa/<int:property_id>/<month>/pdf")
@login_required
def metsoa_pdf(property_id, month):
    from app.models.billing import BilProperty
    import tempfile
    
    prop = BilProperty.query.get_or_404(property_id)

    if prop:
        manager_id = prop.manager_id
        from app.models.billing import BilStatementPayment
        payment = BilStatementPayment.query.filter_by(manager_id=manager_id, month=month).first()
        if not payment or payment.amount_paid_cents <= 0:
            if current_user.id == manager_id:
                flash(f"Please unlock statements for {month} before generating PDFs.", "warning")
                return redirect(url_for('billing_bp.billing_checkout', month=month))
            elif current_user.has_role('admin'):
                pass
            else:
                flash("Your manager has not unlocked statements for this month yet.", "danger")
                return redirect(url_for("public_bp.welcome"))

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
    
    # Hide PDF button in PDF itself
    html_string = html_string.replace('href="{{ url_for(\'billing_bp.metsoa_pdf\', property_id=property.id, month=month) }}"', 'style="display:none;"')
    
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
                margin={"top": "40px", "bottom": "60px", "left": "30px", "right": "30px"}
            )
            browser.close()
            
        return send_file(pdf_path, as_attachment=True, download_name=f"{month}-MetSoa-{prop.name}.pdf")
    except Exception as e:
        import traceback
        traceback.print_exc()
        flash("Failed to generate PDF.", "danger")
        return redirect(url_for('billing_bp.metsoa', property_id=prop.id, month=month))
'''

# Find the start of metsoa_pdf
start_idx = text.find('@billing_bp.route("/billing/metsoa/<int:tenant_id>/<month>/pdf")')
if start_idx != -1:
    end_idx = text.find('\n\n', text.find('return redirect(url_for(\'billing_bp.metsoa\', tenant_id=tenant_id, month=month))', start_idx))
    if end_idx == -1: end_idx = len(text)
    text = text[:start_idx] + replacement.strip() + text[end_idx:]

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)

print('Replaced metsoa_pdf route')
