import re
with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

replacement = '''
@billing_bp.route("/billing/metsoa/<int:property_id>/<month>")
@login_required
def metsoa(property_id, month):
    from app.models.billing import BilProperty
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id and not current_user.has_role('admin'):
        abort(403)
        
    manager_id = prop.manager_id
    from app.models.billing import BilStatementPayment
    payment = BilStatementPayment.query.filter_by(manager_id=manager_id, month=month).first()
    if not payment or payment.amount_paid_cents <= 0:
        if current_user.id == manager_id:
            flash(f"Please unlock statements for {month} before viewing or generating PDFs.", "warning")
            return redirect(url_for('billing_bp.billing_checkout', month=month))
        elif current_user.has_role('admin'):
            flash(f"Notice: Manager has not paid for {month} statements.", "info")
        else:
            flash("Your manager has not unlocked statements for this month yet.", "danger")
            return redirect(url_for("public_bp.welcome"))

    elec_rows, elec_total = build_electrical_rows(prop.id, month)
    water_meters, water_total = build_water_rows(prop.id, month)
    
    grand_total = round(elec_total + water_total, 2)
    
    return render_template("program_billing/metsoa.html", 
                           tenant=None, 
                           property=prop,
                           month=month,
                           elec_rows=elec_rows,
                           elec_total=elec_total,
                           water_meters=water_meters,
                           water_total=water_total,
                           grand_total=grand_total)
'''

start_idx = text.find('@billing_bp.route("/billing/metsoa/<int:tenant_id>/<month>")')
if start_idx != -1:
    end_idx = text.find('@billing_bp.route', start_idx + 10)
    if end_idx == -1: end_idx = len(text)
    text = text[:start_idx] + replacement.strip() + '\n\n' + text[end_idx:]

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)

print('Replaced metsoa route properly')
