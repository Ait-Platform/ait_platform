import re

# 1. Update def setup_wizard() in routes.py
with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    routes = f.read()

old_setup = '''@billing_bp.route("/billing/setup", methods=["GET"])
@login_required
def setup_wizard():
    return render_template("program_billing/setup_wizard.html")'''

new_setup = '''@billing_bp.route("/billing/setup", methods=["GET"])
@login_required
def setup_wizard():
    property_id = request.args.get('property_id')
    if not property_id:
        flash("Missing property ID", "error")
        return redirect(url_for('billing_bp.learner_dashboard'))
    from app.models import BilProperty
    property = BilProperty.query.get_or_404(property_id)
    if property.manager_id != current_user.id:
        from flask import abort
        abort(403)
    return render_template("program_billing/setup_wizard.html", property=property)'''

routes = routes.replace(old_setup, new_setup)
with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(routes)


# 2. Update manager_dashboard.html Tile 2
with open('templates/program_billing/manager_dashboard.html', 'r', encoding='utf-8') as f:
    dashboard = f.read()

old_tile2 = '''          <!-- Tile 2: View Extraction -->
          {% if draft_property and draft_property.onboarding_status in ['draft_extracting', 'draft_collating'] %}
          <a href="{{ url_for('billing_bp.ai_onboarding') }}?property_id={{ draft_property.id }}" class="block rounded-xl border p-6 shadow-sm transition hover:shadow bg-indigo-50 border-indigo-100 hover:bg-indigo-100 group">
            <div class="font-semibold text-slate-900 group-hover:text-indigo-700">2. View Extraction</div>
            <div class="mt-1 text-sm text-slate-600">Upload bills. ({{ uploaded_bills }} / {{ draft_property.expected_bills }} uploaded).</div>
          </a>
          {% else %}
          <div class="block rounded-xl border p-6 shadow-sm bg-slate-50 border-slate-200 opacity-75 cursor-not-allowed">
            <div class="font-semibold text-slate-900">2. View Extraction</div>
            <div class="mt-1 text-sm text-slate-600">Locked until a property setup is initiated.</div>
          </div>
          {% endif %}'''

new_tile2 = '''          <!-- Tile 2: Setup Wizard -->
          {% if draft_property and draft_property.onboarding_status in ['draft_extracting', 'draft_collating', 'draft_setup'] %}
          <a href="{{ url_for('billing_bp.setup_wizard') }}?property_id={{ draft_property.id }}" class="block rounded-xl border p-6 shadow-sm transition hover:shadow bg-indigo-50 border-indigo-100 hover:bg-indigo-100 group">
            <div class="font-semibold text-slate-900 group-hover:text-indigo-700">2. Setup Wizard</div>
            <div class="mt-1 text-sm text-slate-600">Proceed to the 12-step property wizard.</div>
          </a>
          {% else %}
          <div class="block rounded-xl border p-6 shadow-sm bg-slate-50 border-slate-200 opacity-75 cursor-not-allowed">
            <div class="font-semibold text-slate-900">2. Setup Wizard</div>
            <div class="mt-1 text-sm text-slate-600">Locked until a property setup is initiated.</div>
          </div>
          {% endif %}'''

dashboard = dashboard.replace(old_tile2, new_tile2)
with open('templates/program_billing/manager_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(dashboard)

print("Updated Tile 2 and setup_wizard route.")
