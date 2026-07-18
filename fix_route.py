with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

start_idx = -1
for i, line in enumerate(lines):
    if '@billing_bp.route("/billing/onboarding/manual_capture"' in line:
        start_idx = i
        break

end_idx = -1
if start_idx != -1:
    for i in range(start_idx + 1, len(lines)):
        if '@billing_bp.route' in lines[i]:
            end_idx = i
            break

if start_idx != -1 and end_idx != -1:
    new_route = '''@billing_bp.route("/billing/onboarding/manual_capture", methods=["GET"])
@login_required
def manual_capture():
    from app.models.billing import BilProperty, BilMuniAccount, BilMeter
    property_id = request.args.get('property_id')

    if property_id:
        draft_property = BilProperty.query.filter_by(id=property_id, manager_id=current_user.id).first()
    else:
        # Fallback to any draft if no property_id specified (for backwards compatibility)
        draft_property = BilProperty.query.filter(
            BilProperty.manager_id == current_user.id,
            BilProperty.onboarding_status.like('draft_%')
        ).first()

    if not draft_property:
        flash("No property found to edit.", "warning")
        return redirect(url_for('billing_bp.learner_dashboard'))

    accounts_db = BilMuniAccount.query.filter_by(property_id=draft_property.id).all()
    account_numbers = [a.account_number for a in accounts_db if a.account_number]
    all_meters = BilMeter.query.filter(BilMeter.municipal_bill_number.in_(account_numbers)).all() if account_numbers else []
    bulk_meters = [m for m in all_meters if m.pointing_to == 'Bulk Supply']

    return render_template("program_billing/manual_capture.html",
        property=draft_property,
        accounts=accounts_db,
        bulk_meters=bulk_meters)

'''
    new_lines = lines[:start_idx] + [new_route] + lines[end_idx:]
    with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
        f.writelines(new_lines)
    print("Rewrote manual_capture route")
else:
    print("Could not find boundaries")
