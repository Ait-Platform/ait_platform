import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

target = '''        db.session.commit()
        flash("Property details updated successfully!", "success")
        return redirect(url_for('billing_bp.learner_dashboard'))'''

injection = '''        db.session.commit()
        
        onboarding_param = request.args.get('onboarding')
        if prop.onboarding_status == 'draft_financials' and onboarding_param == 'complete':
            prop.onboarding_status = 'active'
            db.session.commit()
            flash("Onboarding complete! Your property is now fully active.", "success")
        else:
            flash("Property details updated successfully!", "success")
            
        return redirect(url_for('billing_bp.learner_dashboard'))'''

content = content.replace(target, injection)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
