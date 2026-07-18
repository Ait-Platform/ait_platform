import re

# 1. Update routes.py
with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    routes_content = f.read()

# Append the missing routes if they don't exist
missing_routes = """
# === RESTORED WIZARD ROUTES ===

@billing_bp.route('/save_architecture_draft/<int:property_id>', methods=['POST'])
@login_required
def save_architecture_draft(property_id):
    from app.models.billing import BilArchitectureDraft
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id:
        return jsonify({"error": "Unauthorized"}), 403

    data = request.json
    draft = BilArchitectureDraft.query.filter_by(property_id=prop.id).first()
    if not draft:
        draft = BilArchitectureDraft(property_id=prop.id)
        from app.extensions import db
        db.session.add(draft)
    
    draft.draft_json = data
    from app.extensions import db
    db.session.commit()
    
    return jsonify({"status": "success"})

@billing_bp.route("/billing/onboarding/save_global_architecture/<int:property_id>", methods=["POST"])
@login_required
def save_global_architecture(property_id):
    from app.models.billing import BilArchitectureDraft
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id:
        return jsonify({"error": "Unauthorized"}), 403

    data = request.json
    if not data:
        return jsonify({"error": "No data provided"}), 400

    from app.extensions import db
    from app.models import BilMuniAccount, RefMuniOwner, BilMeter
    
    try:
        # Process global payload (owners, accounts, meters)
        owners_data = data.get('owners', [])
        accounts_data = data.get('accounts', [])
        meters_data = data.get('meters', [])

        # Process Owners
        owner_map = {}
        for o_data in owners_data:
            name = o_data.get('name', '').strip()
            if name:
                owner = RefMuniOwner.query.filter_by(name=name).first()
                if not owner:
                    owner = RefMuniOwner(name=name)
                    db.session.add(owner)
                    db.session.flush()
                owner_map[o_data.get('id')] = owner.id

        # Process Accounts
        for acc_data in accounts_data:
            acc_num = acc_data.get('account_number', '').strip()
            if acc_num:
                acc = BilMuniAccount.query.filter_by(account_number=acc_num, property_id=prop.id).first()
                if not acc:
                    acc = BilMuniAccount(
                        property_id=prop.id,
                        account_number=acc_num,
                        is_bulk_account=(1 if acc_data.get('is_bulk') else 0)
                    )
                    db.session.add(acc)
                
                temp_owner_id = acc_data.get('owner_id')
                if temp_owner_id in owner_map:
                    acc.owner_id = owner_map[temp_owner_id]
                
                db.session.flush()

        # Process Meters
        for m_data in meters_data:
            m_num = m_data.get('meter_number', '').strip()
            if m_num:
                meter = BilMeter.query.filter_by(meter_number=m_num).first()
                if not meter:
                    meter = BilMeter(
                        meter_number=m_num,
                        utility_type=m_data.get('utility_type'),
                        municipal_bill_number=m_data.get('account_number')
                    )
                    db.session.add(meter)

        prop.onboarding_status = 'draft_manual'
        
        # Clear the draft!
        BilArchitectureDraft.query.filter_by(property_id=prop.id).delete()
        
        db.session.commit()
        return jsonify({"message": "Architecture saved successfully!"}), 200
        
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500

@billing_bp.route('/property/<int:property_id>/architecture_summary')
@login_required
def architecture_summary(property_id):
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id:
        flash("Unauthorized access.", "error")
        return redirect(url_for('billing_bp.learner_dashboard'))
    
    # Gather data for summary
    accounts = BilMuniAccount.query.filter_by(property_id=prop.id).all()
    return render_template('program_billing/architecture_summary.html', property=prop, accounts=accounts)
"""

if "def architecture_summary" not in routes_content:
    routes_content += missing_routes

# Fix onboarding_start_setup redirect
old_setup_start = """    flash(f"Setup initialized for '{prop_name}' (Expects {bills} bill{'s' if bills != 1 else ''}). You can now proceed to View Extraction.", "success")
    return redirect(url_for('billing_bp.learner_dashboard'))"""

new_setup_start = """    flash(f"Setup initialized for '{prop_name}'. You can now proceed with the Setup Wizard.", "success")
    return redirect(url_for('billing_bp.setup_wizard', property_id=prop.id))"""

routes_content = routes_content.replace(old_setup_start, new_setup_start)

# In case old_setup_start wasn't found due to slight variations:
old_setup_start2 = "return redirect(url_for('billing_bp.learner_dashboard'))"
# We need to replace it carefully inside onboarding_start_setup
if new_setup_start not in routes_content:
    def replacer(match):
        return f"    flash(f\"Setup initialized for '{{prop_name}}'. You can now proceed with the Setup Wizard.\", \"success\")\n    return redirect(url_for('billing_bp.setup_wizard', property_id=prop.id))"
    
    routes_content = re.sub(
        r'flash\(.*?"success"\)\s+return redirect\(url_for\(\'billing_bp\.learner_dashboard\'\)\)',
        replacer,
        routes_content
    )

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(routes_content)

print("routes.py updated.")

# 2. Rewrite manager_dashboard.html completely to match requested simplified UI
new_dashboard = """{% extends "layout.html" %}
{% block title %}Property Management Dashboard{% endblock %}

{% block content %}
<div class="min-h-screen bg-slate-50 py-12 px-4 sm:px-6 lg:px-8 font-sans">
  <div class="max-w-7xl mx-auto space-y-8">
    
    {% include "partials/flash_messages.html" %}

    <div class="flex flex-col sm:flex-row justify-between items-start sm:items-center bg-white p-6 rounded-2xl shadow-sm border border-slate-200">
      <div>
        <h1 class="text-3xl font-extrabold text-slate-900 tracking-tight">Property Portfolio</h1>
        <p class="text-sm text-slate-500 mt-1">Manage your properties, meters, and billing configurations.</p>
      </div>
      <div class="mt-4 sm:mt-0">
        <button onclick="document.getElementById('setupModal').classList.remove('hidden'); setTimeout(() => document.getElementById('property_name_input').focus(), 100);" class="inline-flex items-center space-x-2 bg-blue-600 hover:bg-blue-700 text-white font-semibold py-2 px-6 rounded-xl transition shadow-md hover:shadow-lg">
          <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4"></path></svg>
          <span>Add Property</span>
        </button>
      </div>
    </div>

    <!-- Setup Modal -->
    <div id="setupModal" class="hidden fixed inset-0 z-50 flex items-center justify-center bg-slate-900 bg-opacity-50 p-4 sm:p-6 backdrop-blur-sm transition-opacity">
      <div class="bg-white rounded-2xl shadow-2xl w-full max-w-lg overflow-hidden transform transition-all">
        <div class="px-6 py-4 border-b border-slate-100 flex justify-between items-center bg-slate-50">
          <h3 class="font-bold text-slate-800 text-lg">Add New Property</h3>
          <button onclick="document.getElementById('setupModal').classList.add('hidden')" class="text-slate-400 hover:text-slate-600 text-2xl leading-none">&times;</button>
        </div>
        <form action="{{ url_for('billing_bp.onboarding_start_setup') }}" method="POST" class="p-6">
          <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
          <div class="mb-6">
            <label class="block text-sm font-bold text-slate-700 mb-2">Property Name</label>
            <input type="text" id="property_name_input" name="property_name" required placeholder="e.g. Dale View Complex" class="w-full border-2 border-slate-200 rounded-xl px-4 py-3 focus:border-blue-500 focus:ring-4 focus:ring-blue-100 transition outline-none text-slate-700 font-medium placeholder-slate-400">
          </div>
          <div class="flex justify-end space-x-3">
            <button type="button" onclick="document.getElementById('setupModal').classList.add('hidden')" class="px-5 py-2.5 text-slate-600 font-medium hover:bg-slate-100 rounded-xl transition">Cancel</button>
            <button type="submit" class="px-6 py-2.5 bg-blue-600 text-white font-bold rounded-xl hover:bg-blue-700 shadow-md hover:shadow-lg transition">Continue to Wizard</button>
          </div>
        </form>
      </div>
    </div>

    <!-- Property Data Table -->
    <div class="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
      <div class="px-6 py-5 border-b border-slate-100 bg-white">
        <h2 class="text-lg font-bold text-slate-800">Your Properties</h2>
      </div>

      <div class="overflow-x-auto">
        <table class="w-full text-left border-collapse min-w-max">
          <thead>
            <tr class="bg-slate-50 text-slate-500 text-xs font-semibold uppercase tracking-wider border-b border-slate-200">
              <th class="px-6 py-4">Property</th>
              <th class="px-6 py-4">Map</th>
              <th class="px-6 py-4">Edit</th>
              <th class="px-6 py-4">Hub for Readings</th>
              <th class="px-6 py-4 text-right">Delete</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-slate-100">
            {% for row in properties %}
            <tr class="hover:bg-slate-50 transition duration-150">
              <td class="px-6 py-4">
                <div class="font-bold text-slate-800">{{ row.property_name }}</div>
                <div class="text-xs text-slate-400 mt-0.5">ID: {{ row.property_id }}</div>
              </td>
              <td class="px-6 py-4">
                <a href="{{ url_for('billing_bp.architecture_summary', property_id=row.property_id) }}" class="inline-flex items-center px-3 py-1.5 bg-blue-50 text-blue-700 text-xs font-bold rounded-lg hover:bg-blue-100 transition border border-blue-100">
                  <svg class="w-3.5 h-3.5 mr-1.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 20l-5.447-2.724A1 1 0 013 16.382V5.618a1 1 0 011.447-.894L9 7m0 13l6-3m-6 3V7m6 10l4.553 2.276A1 1 0 0021 18.382V7.618a1 1 0 00-.553-.894L15 4m0 13V4m0 0L9 7"></path></svg>
                  View Map
                </a>
              </td>
              <td class="px-6 py-4">
                <a href="{{ url_for('billing_bp.edit_property', property_id=row.property_id) }}" class="inline-flex items-center px-3 py-1.5 bg-slate-100 text-slate-700 text-xs font-bold rounded-lg hover:bg-slate-200 transition border border-slate-200">
                  <svg class="w-3.5 h-3.5 mr-1.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15.232 5.232l3.536 3.536m-2.036-5.036a2.5 2.5 0 113.536 3.536L6.5 21.036H3v-3.572L16.732 3.732z"></path></svg>
                  Edit
                </a>
              </td>
              <td class="px-6 py-4">
                <a href="{{ url_for('billing_bp.property_hub', property_id=row.property_id) }}" class="inline-flex items-center px-3 py-1.5 bg-green-50 text-green-700 text-xs font-bold rounded-lg hover:bg-green-100 transition border border-green-100">
                  <svg class="w-3.5 h-3.5 mr-1.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"></path></svg>
                  Hub for Readings
                </a>
              </td>
              <td class="px-6 py-4 text-right">
                <form action="{{ url_for('billing_bp.delete_property', property_id=row.property_id) }}" method="POST" class="inline" data-name="{{ row.property_name }}" onsubmit="return confirmDelete(event, this.getAttribute('data-name'))">
                  <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
                  <button type="submit" class="p-2 text-red-500 hover:text-red-700 hover:bg-red-50 rounded-lg transition" title="Delete Property">
                    <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"></path></svg>
                  </button>
                </form>
              </td>
            </tr>
            {% else %}
            <tr>
              <td colspan="5" class="px-6 py-12 text-center">
                <div class="flex flex-col items-center justify-center">
                  <svg class="w-12 h-12 text-slate-300 mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-6 0a1 1 0 001-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 001 1m-6 0h6"></path></svg>
                  <p class="text-slate-500 font-medium">No properties found in your portfolio.</p>
                  <p class="text-sm text-slate-400 mt-1">Click 'Add Property' to get started.</p>
                </div>
              </td>
            </tr>
            {% endfor %}
          </tbody>
        </table>
      </div>
    </div>
  </div>
</div>

<script>
function confirmDelete(event, propertyName) {
    if (!confirm(`Are you absolutely sure you want to delete '${propertyName}'? This action cannot be undone.`)) {
        event.preventDefault();
        return false;
    }
    return true;
}
</script>
{% endblock %}
"""

with open('templates/program_billing/manager_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(new_dashboard)

print("Dashboard rewritten to user specification.")
