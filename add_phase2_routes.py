with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

new_routes = """
@billing_bp.route("/billing/onboarding/capture_readings", methods=["GET"])
@login_required
def capture_readings():
    prop = BilProperty.query.filter(
        BilProperty.manager_id == current_user.id,
        BilProperty.onboarding_status == 'draft_readings'
    ).first()

    if not prop:
        flash("No property in Initial Capture stage.", "warning")
        return redirect(url_for('billing_bp.learner_dashboard'))

    accounts = BilMuniAccount.query.filter_by(property_id=prop.id).all()
    
    # Map meters for Jinja template
    acc_meters_map = {}
    for acc in accounts:
        meters = BilMeter.query.filter_by(municipal_bill_number=acc.account_number).all()
        acc_meters_map[acc.account_number] = meters

    # Define a helper function to pass to Jinja
    def get_meters_for_account(acc_num):
        return acc_meters_map.get(acc_num, [])

    return render_template("program_billing/capture_readings.html", property=prop, accounts=accounts, get_meters_for_account=get_meters_for_account)

@billing_bp.route("/billing/onboarding/save_readings", methods=["POST"])
@login_required
def save_readings():
    import json
    from datetime import datetime
    from dateutil.relativedelta import relativedelta
    
    property_id = request.form.get("property_id")
    prop = BilProperty.query.filter_by(id=property_id, manager_id=current_user.id).first()
    
    if not prop or prop.onboarding_status != 'draft_readings':
        return "Unauthorized", 403

    payload_json = request.form.get("payload_json")
    if payload_json:
        payload = json.loads(payload_json)
        for acc_data in payload:
            # 1. Update Financials
            muni_acc = BilMuniAccount.query.get(acc_data.get('id'))
            if muni_acc and muni_acc.property_id == prop.id:
                muni_acc.valuation = float(acc_data.get('valuation') or 0.0)
                muni_acc.rates_amount = float(acc_data.get('rates_amount') or 0.0)
                muni_acc.arrangement_amount = float(acc_data.get('arr_amount') or 0.0)
                muni_acc.arrangement_duration = int(acc_data.get('arr_dur') or 0)
                
                # 2. Process Readings
                for m_data in acc_data.get('meters', []):
                    meter = BilMeter.query.get(m_data.get('meter_id'))
                    if meter and meter.municipal_bill_number == muni_acc.account_number:
                        try:
                            prev_date = datetime.strptime(m_data.get('prev_date'), "%Y-%m-%d").date()
                            curr_date = datetime.strptime(m_data.get('curr_date'), "%Y-%m-%d").date()
                        except:
                            prev_date = datetime.utcnow().date() - relativedelta(days=30)
                            curr_date = datetime.utcnow().date()
                            
                        prev_read = float(m_data.get('prev_read') or 0.0)
                        curr_read = float(m_data.get('curr_read') or 0.0)
                        usage = curr_read - prev_read if curr_read > prev_read else 0
                        days = (curr_date - prev_date).days or 30
                        
                        cons = BilConsumption(
                            meter_id=meter.id,
                            meter_number=meter.meter_number,
                            month=curr_date.strftime("%Y-%m"),
                            last_date=prev_date,
                            new_date=curr_date,
                            last_read=prev_read,
                            new_read=curr_read,
                            days=days,
                            consumption=usage
                        )
                        db.session.add(cons)

        # Advance Status
        prop.onboarding_status = "draft_collating"
        db.session.commit()
        
        # Sync Municipality Accounts
        from app.program_billing.helpers import sync_muni_accounts
        sync_muni_accounts()
        
        flash("Initial Baseline captured successfully! Proceeding to Collation.", "success")
        
    return redirect(url_for('billing_bp.learner_dashboard'))
"""

lines.append(new_routes)
with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.writelines(lines)
print("Added capture_readings and save_readings routes.")
