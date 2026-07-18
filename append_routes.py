new_routes = '''
@billing_bp.route("/billing/utilities", methods=["GET", "POST"])
@login_required
def utilities_hub():
    from app.models.billing import BilProperty
    from datetime import datetime
    
    properties = BilProperty.query.filter_by(manager_id=current_user.id).all()
    current_month = datetime.now().strftime("%Y-%m")
    
    if request.method == "POST":
        property_id = request.form.get("property_id")
        month = request.form.get("billing_month")
        action = request.form.get("action")
        
        if not property_id or not month:
            flash("Please select both a property and a month.", "warning")
            return redirect(url_for("billing_bp.utilities_hub"))
            
        if action == "consumption":
            return redirect(url_for("billing_bp.consumption_review", property_id=property_id, month=month))
        elif action == "metsoa":
            from app.models.billing import BilSectionalUnit
            units = BilSectionalUnit.query.filter_by(property_id=property_id).all()
            tenant_id = None
            if units and units[0].tenants:
                tenant_id = units[0].tenants[0].id
                
            if tenant_id:
                return redirect(url_for("billing_bp.metsoa", tenant_id=tenant_id, month=month))
            else:
                flash("No tenants found for this property to generate METSOA.", "danger")
                return redirect(url_for("billing_bp.utilities_hub"))

    return render_template("program_billing/utilities_hub.html", properties=properties, current_month=current_month)

@billing_bp.route("/billing/utilities/<int:property_id>/consumption/<month>")
@login_required
def consumption_review(property_id, month):
    from app.models.billing import BilProperty, BilSectionalUnit, BilMuniAccount, BilMeter, BilConsumption
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id and not current_user.has_role('admin'):
        abort(403)
        
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
            
    # Get consumption records for these meters for this month
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
        
    return render_template("program_billing/consumption_table.html", property=prop, month=month, data=data)
'''

with open('app/program_billing/routes.py', 'a', encoding='utf-8') as f:
    f.write('\n' + new_routes)
print('Done appending routes')
