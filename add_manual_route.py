with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

import re

# Update onboarding_start_setup
content = content.replace("onboarding_status='draft_extracting'", "onboarding_status='draft_manual'")
content = content.replace("You can now proceed to View Extraction.", "You can now proceed to Manual Capture.")

new_route = """

@billing_bp.route("/billing/onboarding/manual_capture", methods=["GET", "POST"])
@login_required
def manual_capture():
    prop_id = request.args.get('property_id')
    if request.method == "POST":
        prop_id = request.form.get('property_id')
        
    if not prop_id:
        flash("Property not found.", "error")
        return redirect(url_for('billing_bp.learner_dashboard'))
        
    prop = BilProperty.query.filter_by(id=prop_id, manager_id=current_user.id).first()
    if not prop:
        flash("Property not found or access denied.", "error")
        return redirect(url_for('billing_bp.learner_dashboard'))
        
    if request.method == "POST":
        try:
            # Section A: Owner Details
            owner_name = request.form.get("owner_name", "").strip()
            owner_address = request.form.get("owner_address", "").strip()
            account_number = request.form.get("account_number", "").strip()
            
            # Create/Update Owner
            from app.models.billing import RefMuniOwner, BilMuniAccount, BilMeter, BilConsumption
            owner = RefMuniOwner.query.filter_by(name=owner_name).first()
            if not owner:
                owner = RefMuniOwner(name=owner_name)
                db.session.add(owner)
                db.session.flush()
                
            # Create/Update Muni Account
            muni_acc = BilMuniAccount.query.filter_by(account_number=account_number).first()
            if not muni_acc:
                muni_acc = BilMuniAccount(account_number=account_number, owner_id=owner.id)
                db.session.add(muni_acc)
            else:
                muni_acc.owner_id = owner.id
            db.session.flush()
            
            prop.address = owner_address
            prop.municipal_bill_number = account_number
            
            # Section C: Rates & Arrangements
            prop.metro_valuation = float(request.form.get("metro_valuation") or 0.0)
            prop.metro_rates_amount = float(request.form.get("metro_rates_amount") or 0.0)
            prop.metro_arrangement_amount = float(request.form.get("metro_arrangement_amount") or 0.0)
            prop.metro_arrangement_duration = int(request.form.get("metro_arrangement_duration") or 0)
            
            # Section B: Meter Details (Water)
            water_meter_no = request.form.get("water_meter_number", "").strip()
            if water_meter_no:
                muni_acc.muni_water_meter_no = water_meter_no
                water_meter = BilMeter.query.filter_by(meter_number=water_meter_no).first()
                if not water_meter:
                    water_meter = BilMeter(meter_number=water_meter_no, utility_type="water", pointing_to="Entire Property", municipal_bill_number=account_number)
                    db.session.add(water_meter)
                    db.session.flush()
                muni_acc.water_meter_id = water_meter.id
                
                # Baseline
                w_prev = float(request.form.get("water_prev_reading") or 0.0)
                w_curr = float(request.form.get("water_curr_reading") or 0.0)
                w_usage = float(request.form.get("water_usage") or 0.0)
                if w_usage == 0 and w_curr > 0 and w_prev > 0:
                    w_usage = w_curr - w_prev
                
                if w_curr > 0:
                    cons = BilConsumption(
                        meter_id=water_meter.id,
                        meter_number=water_meter.meter_number,
                        month=datetime.utcnow().strftime("%Y-%m"),
                        last_date=datetime.utcnow().date() - relativedelta(days=30),
                        new_date=datetime.utcnow().date(),
                        last_read=w_prev,
                        new_read=w_curr,
                        days=30,
                        consumption=w_usage
                    )
                    db.session.add(cons)

            # Section B: Meter Details (Electricity)
            elec_meter_no = request.form.get("elec_meter_number", "").strip()
            if elec_meter_no:
                muni_acc.muni_elec_meter_no = elec_meter_no
                elec_meter = BilMeter.query.filter_by(meter_number=elec_meter_no).first()
                if not elec_meter:
                    elec_meter = BilMeter(meter_number=elec_meter_no, utility_type="electricity", pointing_to="Entire Property", municipal_bill_number=account_number)
                    db.session.add(elec_meter)
                    db.session.flush()
                muni_acc.elec_meter_id = elec_meter.id
                
                # Baseline
                e_prev = float(request.form.get("elec_prev_reading") or 0.0)
                e_curr = float(request.form.get("elec_curr_reading") or 0.0)
                e_usage = float(request.form.get("elec_usage") or 0.0)
                if e_usage == 0 and e_curr > 0 and e_prev > 0:
                    e_usage = e_curr - e_prev
                
                if e_curr > 0:
                    cons = BilConsumption(
                        meter_id=elec_meter.id,
                        meter_number=elec_meter.meter_number,
                        month=datetime.utcnow().strftime("%Y-%m"),
                        last_date=datetime.utcnow().date() - relativedelta(days=30),
                        new_date=datetime.utcnow().date(),
                        last_read=e_prev,
                        new_read=e_curr,
                        days=30,
                        consumption=e_usage
                    )
                    db.session.add(cons)
            
            # Advance Status
            prop.onboarding_status = 'draft_readings'
            db.session.commit()
            
            # Sync Municipality Accounts
            from app.program_billing.helpers import sync_muni_accounts
            sync_muni_accounts()
            
            flash(f"Manual bill capture for '{prop.name}' successful! You can now continue the setup process.", "success")
            return redirect(url_for('billing_bp.learner_dashboard'))
            
        except Exception as e:
            db.session.rollback()
            import traceback
            traceback.print_exc()
            flash(f"An error occurred during manual setup: {str(e)}", "error")
            return redirect(url_for('billing_bp.manual_capture', property_id=prop.id))
            
    return render_template("program_billing/manual_capture.html", property=prop)
"""

content = content + new_route

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
