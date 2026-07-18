import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    html = f.read()

# 1. Update Step 11 Description Note
old_desc = """        <h2 class="text-xl font-bold text-slate-800 mb-2">Step 11: Rates</h2>
        <p class="text-sm text-slate-600 mb-6">Enter any rates for each account (if applicable).</p>"""
new_desc = """        <h2 class="text-xl font-bold text-slate-800 mb-2">Step 11: Rates</h2>
        <p class="text-sm text-slate-600 mb-2">Enter any rates for each account (if applicable).</p>
        <div class="bg-blue-50 border border-blue-200 rounded-lg p-3 mb-6">
          <p class="text-sm text-blue-800"><i class="fas fa-info-circle mr-1"></i><strong>Note:</strong> This section is for your records. Setting a baseline will help generate accurate monthly financials.</p>
        </div>"""
html = html.replace(old_desc, new_desc)

# 2. Update buildRatesDashboard Javascript
old_build_rates = """function buildRatesDashboard() {
    const container = document.getElementById('rates-container');
    container.innerHTML = '';
    wizardData.accounts.forEach(acc => {
      if(!acc.number) return;
      const savedRate = (wizardData.rates && wizardData.rates.find(x => x.account_id === acc.id)) || { amount: 0, charge_to: 'owner' };
      
      let cardColor = acc.isBulk ? 'bg-amber-50 border-amber-300' : 'bg-sky-50 border-sky-300';
      let isChecked = savedRate.amount > 0;
      let detailsClass = isChecked ? '' : 'hidden';
      
      container.innerHTML += `
        <div class="p-3 border rounded-lg shadow-sm rate-card ${cardColor}" data-acc-id="${acc.id}">
          <div class="flex items-center justify-between">
            <span class="font-bold text-slate-800 text-base">${acc.number} ${acc.isBulk ? '<span class="text-[10px] bg-amber-200 text-amber-800 px-2 py-0.5 rounded ml-2 font-bold uppercase border border-amber-300 shadow-sm">Bulk</span>' : '<span class="text-[10px] bg-sky-200 text-sky-800 px-2 py-0.5 rounded ml-2 font-bold uppercase border border-sky-300 shadow-sm">Sub</span>'}</span>
            <label class="flex items-center space-x-2 text-sm font-bold text-slate-700 cursor-pointer bg-white px-3 py-1.5 border border-slate-300 rounded shadow-sm hover:bg-slate-50 transition">
              <input type="checkbox" class="rate-toggle rounded border-slate-400 text-blue-600 focus:ring-blue-500" ${isChecked ? 'checked' : ''} onchange="toggleRates(this, '${acc.id}')">
              <span>Has Rates?</span>
            </label>
          </div>
          <div class="rate-details mt-4 pt-4 border-t border-slate-200/60 ${detailsClass}">
            <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div>
                <label class="block text-xs font-bold text-slate-600 mb-1">Rates Amount (R)</label>
                <div class="relative">
                  <span class="absolute left-3 top-2 text-slate-500 font-bold">R</span>
                  <input type="number" class="rate-amount w-full rounded border border-slate-400 pl-8 pr-3 py-1.5 text-sm outline-none focus:border-blue-500 focus:ring-2 focus:ring-blue-100 shadow-inner transition bg-white" value="${savedRate.amount || ''}" onchange="triggerAutoSave()" step="0.01">
                </div>
              </div>
              <div>
                <label class="block text-xs font-bold text-slate-600 mb-1">Date</label>
                <input type="date" class="rate-date w-full rounded border border-slate-400 px-3 py-1.5 text-sm outline-none focus:border-blue-500 focus:ring-2 focus:ring-blue-100 shadow-inner transition bg-white" value="${savedRate.date || ''}" onchange="triggerAutoSave()">
              </div>
              <div>
                <label class="block text-xs font-bold text-slate-600 mb-1">Charge To</label>
                <select class="rate-charge-to w-full rounded border border-slate-400 px-3 py-1.5 text-sm outline-none focus:border-blue-500 focus:ring-2 focus:ring-blue-100 shadow-inner transition bg-white" onchange="triggerAutoSave()">
                  <option value="owner" ${savedRate.charge_to === 'owner' ? 'selected' : ''}>Owner</option>
                  <option value="tenant" ${savedRate.charge_to === 'tenant' ? 'selected' : ''}>Tenant</option>
                </select>
              </div>
            </div>
          </div>
        </div>
      `;
    });
  }"""

new_build_rates = """  function buildRatesDashboard() {
    const container = document.getElementById('rates-container');
    container.innerHTML = '';
    wizardData.accounts.forEach((acc, idx) => {
      if(!acc.number) return;
      const savedRate = (wizardData.rates && wizardData.rates.find(x => x.account_id === acc.id)) || { amount: 0, charge_to: 'owner', reference: '', erf_details: '', property_category: '', market_value: 0, rateable_value: 0, general_randage: 0, sra_randage: 0, deferred: 0, sra_monthly: 0, general_monthly: 0 };
      
      let cardColor = acc.isBulk ? 'bg-amber-50 border-amber-300' : 'bg-sky-50 border-sky-300';
      let isChecked = (savedRate.amount > 0 || savedRate.market_value > 0);
      let detailsClass = isChecked ? '' : 'hidden';
      
      container.innerHTML += `
        <div class="p-3 border rounded-lg shadow-sm rate-card ${cardColor}" data-acc-id="${acc.id}">
          <div class="flex items-center justify-between">
            <span class="font-bold text-slate-800 text-base">${acc.number} ${acc.isBulk ? '<span class="text-[10px] bg-amber-200 text-amber-800 px-2 py-0.5 rounded ml-2 font-bold uppercase border border-amber-300 shadow-sm">Bulk</span>' : '<span class="text-[10px] bg-sky-200 text-sky-800 px-2 py-0.5 rounded ml-2 font-bold uppercase border border-sky-300 shadow-sm">Sub</span>'}</span>
            <label class="flex items-center space-x-2 text-sm font-bold text-slate-700 cursor-pointer bg-white px-3 py-1.5 border border-slate-300 rounded shadow-sm hover:bg-slate-50 transition">
              <input type="checkbox" class="rate-toggle rounded border-slate-400 text-blue-600 focus:ring-blue-500" ${isChecked ? 'checked' : ''} onchange="toggleRates(this, '${acc.id}')">
              <span>Has Rates?</span>
            </label>
          </div>
          
          <div class="rate-details mt-4 pt-4 border-t border-slate-200/60 ${detailsClass}">
            
            <div class="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
              <div>
                <label class="block text-xs font-bold text-slate-600 mb-1">Reference</label>
                <input type="text" class="rate-reference w-full rounded border border-slate-400 px-3 py-1.5 text-sm bg-white" value="${savedRate.reference || ''}" onchange="triggerAutoSave()">
              </div>
              <div>
                <label class="block text-xs font-bold text-slate-600 mb-1">Erf Details</label>
                <input type="text" class="rate-erf w-full rounded border border-slate-400 px-3 py-1.5 text-sm bg-white" value="${savedRate.erf_details || ''}" onchange="triggerAutoSave()">
              </div>
              <div>
                <label class="block text-xs font-bold text-slate-600 mb-1">Property Category</label>
                <input type="text" class="rate-category w-full rounded border border-slate-400 px-3 py-1.5 text-sm bg-white" value="${savedRate.property_category || ''}" onchange="triggerAutoSave()">
              </div>
            </div>

            <div class="grid grid-cols-1 md:grid-cols-4 gap-4 mb-4">
              <div>
                <label class="block text-xs font-bold text-slate-600 mb-1">Market Value (R)</label>
                <input type="number" id="mv-${acc.id}" class="rate-market-value w-full rounded border border-slate-400 px-3 py-1.5 text-sm bg-white" value="${savedRate.market_value || ''}" oninput="calcRates('${acc.id}')" onchange="triggerAutoSave()" step="0.01">
              </div>
              <div>
                <label class="block text-xs font-bold text-slate-600 mb-1">Rateable Value (R)</label>
                <input type="number" id="rv-${acc.id}" class="rate-rateable-value w-full rounded border border-slate-400 px-3 py-1.5 text-sm bg-white" value="${savedRate.rateable_value || ''}" oninput="calcRates('${acc.id}')" onchange="triggerAutoSave()" step="0.01">
              </div>
              <div>
                <label class="block text-xs font-bold text-slate-600 mb-1">Gen. Randage (c/R)</label>
                <input type="number" id="gr-${acc.id}" class="rate-gen-randage w-full rounded border border-slate-400 px-3 py-1.5 text-sm bg-white" value="${savedRate.general_randage || ''}" oninput="calcRates('${acc.id}')" onchange="triggerAutoSave()" step="0.0001">
              </div>
              <div>
                <label class="block text-xs font-bold text-slate-600 mb-1">SRA Randage (c/R)</label>
                <input type="number" id="sr-${acc.id}" class="rate-sra-randage w-full rounded border border-slate-400 px-3 py-1.5 text-sm bg-white" value="${savedRate.sra_randage || ''}" oninput="calcRates('${acc.id}')" onchange="triggerAutoSave()" step="0.0001">
              </div>
            </div>

            <div class="grid grid-cols-1 md:grid-cols-4 gap-4 mb-4 bg-slate-100 p-3 rounded border border-slate-300 shadow-inner">
              <div>
                <label class="block text-xs font-bold text-slate-700 mb-1">Deferred Rates (R)</label>
                <input type="number" class="rate-deferred w-full rounded border border-slate-400 px-3 py-1.5 text-sm bg-white font-bold" value="${savedRate.deferred || ''}" onchange="triggerAutoSave()" step="0.01">
              </div>
              <div>
                <label class="block text-xs font-bold text-slate-700 mb-1">General Monthly (R)</label>
                <input type="number" id="gm-${acc.id}" class="rate-gen-monthly w-full rounded border border-slate-400 px-3 py-1.5 text-sm bg-white font-bold text-blue-700" value="${savedRate.general_monthly || ''}" onchange="triggerAutoSave()" step="0.01" readonly>
              </div>
              <div>
                <label class="block text-xs font-bold text-slate-700 mb-1">SRA Monthly (R)</label>
                <input type="number" id="sm-${acc.id}" class="rate-sra-monthly w-full rounded border border-slate-400 px-3 py-1.5 text-sm bg-white font-bold text-blue-700" value="${savedRate.sra_monthly || ''}" onchange="triggerAutoSave()" step="0.01" readonly>
              </div>
              <div>
                <label class="block text-xs font-bold text-slate-700 mb-1">Total Rates (R)</label>
                <input type="number" id="tm-${acc.id}" class="rate-amount w-full rounded border border-slate-400 px-3 py-1.5 text-sm bg-white font-bold text-blue-700" value="${savedRate.amount || ''}" onchange="triggerAutoSave()" step="0.01" readonly>
              </div>
            </div>
            
            <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <label class="block text-xs font-bold text-slate-600 mb-1">Date Applied</label>
                <input type="date" class="rate-date w-full rounded border border-slate-400 px-3 py-1.5 text-sm bg-white" value="${savedRate.date || ''}" onchange="triggerAutoSave()">
              </div>
              <div>
                <label class="block text-xs font-bold text-slate-600 mb-1">Charge To</label>
                <select class="rate-charge-to w-full rounded border border-slate-400 px-3 py-1.5 text-sm bg-white" onchange="triggerAutoSave()">
                  <option value="owner" ${savedRate.charge_to === 'owner' ? 'selected' : ''}>Owner</option>
                  <option value="tenant" ${savedRate.charge_to === 'tenant' ? 'selected' : ''}>Tenant</option>
                </select>
              </div>
            </div>

          </div>
        </div>
      `;
    });
  }
  
  window.calcRates = function(accId) {
    const mv = parseFloat(document.getElementById('mv-'+accId).value) || 0;
    const rv = parseFloat(document.getElementById('rv-'+accId).value) || 0;
    const gr = parseFloat(document.getElementById('gr-'+accId).value) || 0;
    const sr = parseFloat(document.getElementById('sr-'+accId).value) || 0;
    
    // Convert c/R to decimal for formula
    // e.g. 1.437 c/R = 0.01437
    const grDec = gr / 100;
    const srDec = sr / 100;
    
    const genMonthly = (rv * grDec) / 12;
    const sraMonthly = (mv * srDec) / 12;
    const total = genMonthly + sraMonthly;
    
    document.getElementById('gm-'+accId).value = genMonthly.toFixed(2);
    document.getElementById('sm-'+accId).value = sraMonthly.toFixed(2);
    document.getElementById('tm-'+accId).value = total.toFixed(2);
  }"""
html = html.replace(old_build_rates.strip(), new_build_rates.strip())

# 3. Update gatherRates
old_gather = """  function gatherRates() {
    wizardData.rates = [];
    let valid = true;
    document.querySelectorAll('.rate-card').forEach(card => {
      const isChecked = card.querySelector('.rate-toggle').checked;
      if(isChecked) {
        const accId = card.getAttribute('data-acc-id');
        const amount = parseFloat(card.querySelector('.rate-amount').value);
        const date = card.querySelector('.rate-date').value;
        const charge = card.querySelector('.rate-charge-to').value;
        if(isNaN(amount) || amount <= 0 || !date) {
          alert('Please provide a valid Rates amount and date for checked accounts.');
          valid = false;
        } else {
          wizardData.rates.push({ account_id: accId, amount: amount, date: date, charge_to: charge });
        }
      }
    });
    return valid;
  }"""

new_gather = """  function gatherRates() {
    wizardData.rates = [];
    let valid = true;
    document.querySelectorAll('.rate-card').forEach(card => {
      const isChecked = card.querySelector('.rate-toggle').checked;
      if(isChecked) {
        const accId = card.getAttribute('data-acc-id');
        
        const reference = card.querySelector('.rate-reference').value;
        const erf_details = card.querySelector('.rate-erf').value;
        const property_category = card.querySelector('.rate-category').value;
        const market_value = parseFloat(card.querySelector('.rate-market-value').value) || 0;
        const rateable_value = parseFloat(card.querySelector('.rate-rateable-value').value) || 0;
        const general_randage = parseFloat(card.querySelector('.rate-gen-randage').value) || 0;
        const sra_randage = parseFloat(card.querySelector('.rate-sra-randage').value) || 0;
        const deferred = parseFloat(card.querySelector('.rate-deferred').value) || 0;
        const sra_monthly = parseFloat(card.querySelector('.rate-sra-monthly').value) || 0;
        const general_monthly = parseFloat(card.querySelector('.rate-gen-monthly').value) || 0;
        
        const amount = parseFloat(card.querySelector('.rate-amount').value) || 0;
        const date = card.querySelector('.rate-date').value;
        const charge = card.querySelector('.rate-charge-to').value;
        
        // Removed validation to allow 0 or incomplete saving, since this is for baseline recording
        wizardData.rates.push({ 
            account_id: accId, 
            amount: amount, 
            date: date, 
            charge_to: charge,
            reference: reference,
            erf_details: erf_details,
            property_category: property_category,
            market_value: market_value,
            rateable_value: rateable_value,
            general_randage: general_randage,
            sra_randage: sra_randage,
            deferred: deferred,
            sra_monthly: sra_monthly,
            general_monthly: general_monthly
        });
      }
    });
    return valid;
  }"""
html = html.replace(old_gather.strip(), new_gather.strip())

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(html)
print("Updated setup_wizard.html for Detailed Rates")
