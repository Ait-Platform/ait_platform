import re

with open("D:/Users/yeshk/Documents/ait_platform/templates/school_billing/setup_wizard.html", "r", encoding="utf-8") as f:
    content = f.read()

# Replace step limits
content = content.replace("step < 6", "step < 8")
content = content.replace("step === 6", "step === 8")
content = content.replace("x-show=\"step === 5\"", "x-show=\"step === 6\"") # Arrays to Step 6
content = content.replace("x-show=\"step === 6\"", "x-show=\"step === 7\"") # Rates to Step 7
content = content.replace("<!-- Step 5: Arrangements -->", "<!-- Step 6: Arrangements -->")
content = content.replace("<!-- Step 6: Rates -->", "<!-- Step 7: Rates -->")

# Now inject the new Step 5 (Tenant Arrears) right before Step 6
step_5_html = """
        <!-- Step 5: Tenant Arrears -->
        <div x-show="step === 5 && tenant_name !== ''" x-transition.opacity.duration.300ms class="space-y-6">
          <div class="text-center">
            <h2 class="text-2xl font-bold text-slate-800">Tenant Arrears</h2>
            <p class="text-slate-500 mt-2">Does this tenant have historical unpaid debt?</p>
          </div>
          
          <div class="space-y-4">
            <label class="block text-sm font-medium text-slate-700">Does the tenant have arrears?</label>
            <select x-model="has_tenant_arrears" class="w-full border border-slate-300 rounded-xl p-3 focus:ring-2 focus:ring-orange-500 outline-none">
              <option value="no">No</option>
              <option value="yes">Yes</option>
            </select>
            
            <template x-if="has_tenant_arrears === 'yes'">
              <div class="space-y-4 border rounded-xl p-4 bg-slate-50 mt-4">
                <div>
                  <label class="block text-sm font-medium text-slate-700 mb-1">Total Historical Debt (ZAR)</label>
                  <input type="number" x-model="tenant_arrears_total" class="w-full border border-slate-300 rounded-xl p-3 focus:ring-2 focus:ring-orange-500 outline-none" placeholder="e.g. 44000">
                </div>
                <div>
                  <label class="block text-sm font-medium text-slate-700 mb-1">Monthly Repayment Installment (ZAR/month)</label>
                  <input type="number" x-model="tenant_arrears_installment" class="w-full border border-orange-300 bg-white rounded-xl p-3 focus:ring-2 focus:ring-orange-500 outline-none" placeholder="e.g. 1000">
                  <p class="text-xs text-slate-500 mt-1">This amount will be added to their monthly statement until the debt is cleared.</p>
                </div>
              </div>
            </template>
          </div>
        </div>
        
        <!-- Skip Step 5 if no tenant -->
        <div x-show="step === 5 && tenant_name === ''" x-transition.opacity.duration.300ms class="space-y-6 text-center py-10">
            <h2 class="text-xl font-bold text-slate-800">No Tenant Configured</h2>
            <p class="text-slate-500 mt-2">You left the tenant blank, so there are no tenant arrears to configure.</p>
        </div>
"""
content = content.replace("<!-- Step 6: Arrangements -->", step_5_html + "\n        <!-- Step 6: Arrangements -->")

# Now inject Step 8 (Agent Fees) right before the Navigation buttons
step_8_html = """
        <!-- Step 8: Agent Fees -->
        <div x-show="step === 8" x-transition.opacity.duration.300ms class="space-y-6">
          <div class="text-center">
            <h2 class="text-2xl font-bold text-slate-800">Agent & Admin Fees</h2>
            <p class="text-slate-500 mt-2">Are you an agent managing this property?</p>
          </div>
          
          <div class="space-y-4">
            <label class="block text-sm font-medium text-slate-700">Do you charge an admin or platform fee?</label>
            <select x-model="has_agent_fee" class="w-full border border-slate-300 rounded-xl p-3 focus:ring-2 focus:ring-orange-500 outline-none">
              <option value="no">No</option>
              <option value="yes">Yes</option>
            </select>
            
            <template x-if="has_agent_fee === 'yes'">
              <div class="space-y-4 border rounded-xl p-4 bg-slate-50 mt-4">
                <div>
                  <label class="block text-sm font-medium text-slate-700 mb-1">Monthly Admin Fee (ZAR/month)</label>
                  <input type="number" x-model="agent_fee_amount" class="w-full border border-slate-300 rounded-xl p-3 focus:ring-2 focus:ring-orange-500 outline-none" placeholder="e.g. 220">
                </div>
                <div>
                  <label class="block text-sm font-medium text-slate-700 mb-1">Who pays this fee?</label>
                  <select x-model="agent_fee_target" class="w-full border border-orange-300 bg-white rounded-xl p-3 focus:ring-2 focus:ring-orange-500 outline-none">
                    <option value="owner">Charge to the Owner (Deducted from Landlord Statement)</option>
                    <option value="tenant">Charge to the Tenant (Added to Tenant Statement)</option>
                  </select>
                </div>
              </div>
            </template>
          </div>
        </div>
"""
content = content.replace("<!-- Navigation -->", step_8_html + "\n        <!-- Navigation -->")

# Update step indicators visually
new_indicators = """        <div class="flex items-center justify-between px-6 py-4 bg-orange-50 border-b overflow-x-auto">
          <template x-for="i in 8">
            <div class="flex flex-col items-center min-w-[50px] mx-1">
              <div class="w-8 h-8 rounded-full flex items-center justify-center font-bold text-sm"
                   :class="step >= i ? 'bg-orange-500 text-white' : 'bg-orange-200 text-orange-700'">
                <span x-text="i"></span>
              </div>
              <div class="text-[10px] mt-1 font-medium whitespace-nowrap"
                   :class="step >= i ? 'text-orange-700' : 'text-orange-400'">
                <span x-show="i === 1">Terms</span>
                <span x-show="i === 2">Property</span>
                <span x-show="i === 3">Meters</span>
                <span x-show="i === 4">Tenant</span>
                <span x-show="i === 5">T. Arrears</span>
                <span x-show="i === 6">M. Arrears</span>
                <span x-show="i === 7">Rates</span>
                <span x-show="i === 8">Agent</span>
              </div>
            </div>
          </template>
        </div>"""
content = re.sub(r'<div class="flex items-center justify-between px-6 py-4 bg-orange-50 border-b overflow-x-auto">.*?</div>', new_indicators, content, flags=re.DOTALL)


# Update alpine data
alpine_data = """      x-data="{
        step: 1,
        property_name: '',
        address: '',
        landlord_name: '',
        tenant_name: '',
        tenant_email: '',
        rent_amount: '',
        has_bulk_water: 'no',
        has_bulk_elec: 'no',
        has_arrangement: 'no',
        metro_arrangement_amount: '',
        metro_arrangement_duration: '',
        tenant_arrangement_charge: '',
        has_rates: 'no',
        metro_rates_amount: '',
        tenant_rates_charge: '',
        has_tenant_arrears: 'no',
        tenant_arrears_total: '',
        tenant_arrears_installment: '',
        has_agent_fee: 'no',
        agent_fee_amount: '',
        agent_fee_target: 'owner',
        meters: [],
        tenant_meters: [],"""
content = re.sub(r'x-data="\{.*?tenant_meters: \[\],', alpine_data, content, flags=re.DOTALL)


# Update submitPayload to include the new fields
payload_update = """
            tenant_meters: this.tenant_meters,
            has_arrangement: this.has_arrangement,
            metro_arrangement_amount: this.metro_arrangement_amount,
            metro_arrangement_duration: this.metro_arrangement_duration,
            tenant_arrangement_charge: this.tenant_arrangement_charge,
            has_rates: this.has_rates,
            metro_rates_amount: this.metro_rates_amount,
            tenant_rates_charge: this.tenant_rates_charge,
            has_tenant_arrears: this.has_tenant_arrears,
            tenant_arrears_total: this.tenant_arrears_total,
            tenant_arrears_installment: this.tenant_arrears_installment,
            has_agent_fee: this.has_agent_fee,
            agent_fee_amount: this.agent_fee_amount,
            agent_fee_target: this.agent_fee_target,
            meters: this.meters
"""
content = content.replace("tenant_meters: this.tenant_meters,\n            has_arrangement: this.has_arrangement,", payload_update.replace("            tenant_meters: this.tenant_meters,\n", ""))


with open("D:/Users/yeshk/Documents/ait_platform/templates/school_billing/setup_wizard.html", "w", encoding="utf-8") as f:
    f.write(content)
    
print("Updated setup_wizard.html successfully!")
