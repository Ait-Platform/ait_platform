with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

import re

old_manual_capture_start = """    if request.method == "POST":
        property_id = request.form.get("property_id")
        prop = BilProperty.query.get_or_404(property_id)

        # Section A: Property & Owner Details"""

old_manual_capture_end = """            # Advance Status
            prop.onboarding_status = "draft_collating"
            db.session.commit()
            flash("Manual Capture Complete. Proceeding to Collation.", "success")
            return redirect(url_for("billing_bp.learner_dashboard"))

    return render_template("program_billing/manual_capture.html", property=draft_property)"""

# I need to extract the exact block to replace it.
# Let's find everything from 'def manual_capture():' to the end of that function.
regex = r'def manual_capture\(\):(.*?)return render_template\("program_billing/manual_capture\.html", property=draft_property\)'

new_manual_capture = """def manual_capture():
    draft_property = BilProperty.query.filter(
        BilProperty.manager_id == current_user.id,
        BilProperty.onboarding_status == 'draft_manual'
    ).first()

    if not draft_property:
        flash("No property in manual capture stage.", "warning")
        return redirect(url_for('billing_bp.learner_dashboard'))

    if request.method == "POST":
        import json
        from datetime import datetime
        from dateutil.relativedelta import relativedelta
        
        property_id = request.form.get("property_id")
        prop = BilProperty.query.get_or_404(property_id)

        payload_json = request.form.get("payload_json")
        if payload_json:
            accounts_data = json.loads(payload_json)
            
            bulk_meter_map = {}
            
            for acc_data in accounts_data:
                # 1. Ensure Owner
                owner_name = acc_data.get('owner_name', '').strip()
                if not owner_name: continue
                owner = RefMuniOwner.query.filter_by(name=owner_name).first()
                if not owner:
                    owner = RefMuniOwner(name=owner_name)
                    db.session.add(owner)
                    db.session.flush()
                
                # 2. Ensure Municipal Account & Attach Financials
                acc_number = acc_data.get('account_number', '').strip()
                if not acc_number: continue
                muni_acc = BilMuniAccount.query.filter_by(account_number=acc_number).first()
                if not muni_acc:
                    muni_acc = BilMuniAccount(account_number=acc_number, owner_id=owner.id)
                    db.session.add(muni_acc)
                else:
                    muni_acc.owner_id = owner.id
                
                muni_acc.valuation = float(acc_data.get('valuation') or 0.0)
                muni_acc.rates_amount = float(acc_data.get('rates_amount') or 0.0)
                muni_acc.arrangement_amount = float(acc_data.get('arrangement_amount') or 0.0)
                muni_acc.arrangement_duration = int(acc_data.get('arrangement_duration') or 0)
                db.session.flush()
                
                # 3. Process Bulk Meters first
                for m_data in acc_data.get('meters', []):
                    if m_data.get('assignment') == 'bulk':
                        m_number = m_data.get('meter_number', '').strip()
                        u_type = m_data.get('utility_type', 'water')
                        status = m_data.get('status', 'active')
                        
                        if m_number:
                            meter = BilMeter.query.filter_by(meter_number=m_number).first()
                            if not meter:
                                meter = BilMeter(meter_number=m_number, utility_type=u_type, pointing_to="Bulk Supply", municipal_bill_number=acc_number, status=status)
                                db.session.add(meter)
                                db.session.flush()
                            else:
                                meter.status = status
                                meter.municipal_bill_number = acc_number
                                
                            bulk_meter_map[u_type] = meter.id
                            
                            # Baseline
                            try:
                                prev_date = datetime.strptime(m_data.get('prev_date'), "%Y-%m-%d").date() if m_data.get('prev_date') else datetime.utcnow().date() - relativedelta(days=30)
                                curr_date = datetime.strptime(m_data.get('curr_date'), "%Y-%m-%d").date() if m_data.get('curr_date') else datetime.utcnow().date()
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

                # 4. Process Linked Meters
                for m_data in acc_data.get('meters', []):
                    if m_data.get('assignment') == 'linked':
                        m_number = m_data.get('meter_number', '').strip()
                        u_type = m_data.get('utility_type', 'water')
                        status = m_data.get('status', 'active')
                        
                        if m_number:
                            parent_id = bulk_meter_map.get(u_type)
                            meter = BilMeter.query.filter_by(meter_number=m_number).first()
                            if not meter:
                                meter = BilMeter(meter_number=m_number, utility_type=u_type, parent_meter_id=parent_id, pointing_to="Sub-Unit", municipal_bill_number=acc_number, status=status)
                                db.session.add(meter)
                                db.session.flush()
                            else:
                                meter.status = status
                                meter.municipal_bill_number = acc_number
                                meter.parent_meter_id = parent_id
                                
                            # Baseline
                            try:
                                prev_date = datetime.strptime(m_data.get('prev_date'), "%Y-%m-%d").date() if m_data.get('prev_date') else datetime.utcnow().date() - relativedelta(days=30)
                                curr_date = datetime.strptime(m_data.get('curr_date'), "%Y-%m-%d").date() if m_data.get('curr_date') else datetime.utcnow().date()
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

        prop.onboarding_status = "draft_collating"
        db.session.commit()
        flash("Manual Capture Complete. Proceeding to Collation.", "success")
        return redirect(url_for("billing_bp.learner_dashboard"))

    return render_template("program_billing/manual_capture.html", property=draft_property)"""

import re
content = re.sub(regex, new_manual_capture, content, flags=re.DOTALL)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated backend route for manual capture.")
