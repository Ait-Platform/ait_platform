import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

target = '''@billing_bp.route("/billing/onboarding", methods=["GET"])
@login_required
def ai_onboarding():
    return render_template("program_billing/ai_onboarding.html")'''

injection = '''@billing_bp.route("/billing/onboarding", methods=["GET"])
@login_required
def ai_onboarding():
    property_id = request.args.get('property_id')
    draft_property = None
    if property_id:
        draft_property = BilProperty.query.get(property_id)
        if draft_property and draft_property.manager_id != current_user.id:
            from flask import abort
            abort(403)
    return render_template("program_billing/ai_onboarding.html", draft_property=draft_property)'''

content = content.replace(target, injection)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
