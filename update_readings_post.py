import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

target = '''        if added_count > 0:
            flash(f"Successfully saved {added_count} meter reading(s) for {reading_month}!", "success")
        else:
            flash("No readings were entered.", "warning")
            
        return redirect(url_for('billing_bp.input_readings', property_id=prop.id))'''

injection = '''        if added_count > 0:
            if prop.onboarding_status == 'draft_readings':
                prop.onboarding_status = 'draft_financials'
                db.session.commit()
                flash(f"Successfully saved {added_count} meter reading(s). Proceed to Financial Requirements.", "success")
                return redirect(url_for('billing_bp.learner_dashboard'))
            flash(f"Successfully saved {added_count} meter reading(s) for {reading_month}!", "success")
        else:
            flash("No readings were entered.", "warning")
            
        return redirect(url_for('billing_bp.input_readings', property_id=prop.id))'''

content = content.replace(target, injection)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
