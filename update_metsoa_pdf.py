import re
with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

metsoa_pdf_old = """@billing_bp.route("/billing/metsoa/<int:tenant_id>/<month>/pdf")
@login_required
def metsoa_pdf(tenant_id, month):
    tenant = BilTenant.query.get(tenant_id)
    if not tenant:
        return "Tenant not found", 404

    prop = BilProperty.query.get(tenant.sectional_unit.property_id) if tenant.sectional_unit and tenant.sectional_unit.property_id else None"""

metsoa_pdf_new = """@billing_bp.route("/billing/metsoa/<int:property_id>/<month>/pdf")
@login_required
def metsoa_pdf(property_id, month):
    prop = BilProperty.query.get_or_404(property_id)"""

text = text.replace(metsoa_pdf_old, metsoa_pdf_new)

metsoa_pdf_body_old = """    elec_rows, elec_total = build_electrical_rows(tenant.sectional_unit_id, month)
    water_meters, water_total = build_water_rows(tenant.sectional_unit_id, month)
    
    grand_total = round(elec_total + water_total, 2)
    
    html = render_template("program_billing/metsoa_pdf_template.html",
                           tenant=tenant,
                           property=prop,
                           month=month,
                           elec_rows=elec_rows,
                           elec_total=elec_total,
                           water_meters=water_meters,
                           water_total=water_total,
                           grand_total=grand_total)"""

metsoa_pdf_body_new = """    elec_rows, elec_total = build_electrical_rows(prop.id, month)
    water_meters, water_total = build_water_rows(prop.id, month)
    
    grand_total = round(elec_total + water_total, 2)
    
    html = render_template("program_billing/metsoa_pdf_template.html",
                           tenant=None,
                           property=prop,
                           month=month,
                           elec_rows=elec_rows,
                           elec_total=elec_total,
                           water_meters=water_meters,
                           water_total=water_total,
                           grand_total=grand_total)"""

text = text.replace(metsoa_pdf_body_old, metsoa_pdf_body_new)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)

print('Updated metsoa_pdf route')
