import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    html = f.read()

# 1. Update the Market Value HTML
old_mv = """              <div>
                <label class="block text-xs font-bold text-slate-600 mb-1">Market Value (R)</label>
                <input type="number" id="mv-${acc.id}" class="rate-market-value w-full rounded border border-slate-400 px-3 py-1.5 text-sm bg-white" value="${savedRate.market_value || ''}" oninput="calcRates('${acc.id}')" onchange="triggerAutoSave()" step="0.01">
              </div>"""

new_mv = """              <div>
                <label class="flex justify-between items-end mb-1">
                  <span class="block text-xs font-bold text-slate-600">Market Value (R)</span>
                  <span id="mv-disp-${acc.id}" class="text-[10px] font-bold text-blue-600"></span>
                </label>
                <input type="number" id="mv-${acc.id}" class="rate-market-value w-full rounded border border-slate-400 px-3 py-1.5 text-sm bg-white" value="${savedRate.market_value || ''}" oninput="calcRates('${acc.id}')" onchange="triggerAutoSave()" step="0.01">
              </div>"""
html = html.replace(old_mv, new_mv)

# 2. Update the Rateable Value HTML
old_rv = """              <div>
                <label class="block text-xs font-bold text-slate-600 mb-1">Rateable Value (R)</label>
                <input type="number" id="rv-${acc.id}" class="rate-rateable-value w-full rounded border border-slate-400 px-3 py-1.5 text-sm bg-white" value="${savedRate.rateable_value || ''}" oninput="calcRates('${acc.id}')" onchange="triggerAutoSave()" step="0.01">
              </div>"""

new_rv = """              <div>
                <label class="flex justify-between items-end mb-1">
                  <span class="block text-xs font-bold text-slate-600">Rateable Value (R)</span>
                  <span id="rv-disp-${acc.id}" class="text-[10px] font-bold text-blue-600"></span>
                </label>
                <input type="number" id="rv-${acc.id}" class="rate-rateable-value w-full rounded border border-slate-400 px-3 py-1.5 text-sm bg-white" value="${savedRate.rateable_value || ''}" oninput="calcRates('${acc.id}')" onchange="triggerAutoSave()" step="0.01">
              </div>"""
html = html.replace(old_rv, new_rv)

# 3. Add calcRates trigger to Deferred Rates
old_def = """                <input type="number" class="rate-deferred w-full rounded border border-slate-400 px-3 py-1.5 text-sm bg-white font-bold" value="${savedRate.deferred || ''}" onchange="triggerAutoSave()" step="0.01">"""
new_def = """                <input type="number" id="def-${acc.id}" class="rate-deferred w-full rounded border border-slate-400 px-3 py-1.5 text-sm bg-white font-bold" value="${savedRate.deferred || ''}" oninput="calcRates('${acc.id}')" onchange="triggerAutoSave()" step="0.01">"""
html = html.replace(old_def, new_def)

# 4. Update calcRates function to include Deferred and update displays
old_calc = """  window.calcRates = function(accId) {
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

new_calc = """  window.calcRates = function(accId) {
    const mv = parseFloat(document.getElementById('mv-'+accId).value) || 0;
    const rv = parseFloat(document.getElementById('rv-'+accId).value) || 0;
    const gr = parseFloat(document.getElementById('gr-'+accId).value) || 0;
    const sr = parseFloat(document.getElementById('sr-'+accId).value) || 0;
    const def = parseFloat(document.getElementById('def-'+accId).value) || 0;
    
    // Format Display for Millions
    const mvDisp = document.getElementById('mv-disp-'+accId);
    if(mvDisp) {
        if(mv > 0) mvDisp.innerText = `R ${(mv / 1000000).toFixed(2)}m`;
        else mvDisp.innerText = '';
    }
    
    const rvDisp = document.getElementById('rv-disp-'+accId);
    if(rvDisp) {
        if(rv > 0) rvDisp.innerText = `R ${(rv / 1000000).toFixed(2)}m`;
        else rvDisp.innerText = '';
    }
    
    // Convert c/R to decimal for formula
    // e.g. 1.437 c/R = 0.01437
    const grDec = gr / 100;
    const srDec = sr / 100;
    
    const genMonthly = (rv * grDec) / 12;
    const sraMonthly = (mv * srDec) / 12;
    const total = genMonthly + sraMonthly + def;
    
    document.getElementById('gm-'+accId).value = genMonthly.toFixed(2);
    document.getElementById('sm-'+accId).value = sraMonthly.toFixed(2);
    document.getElementById('tm-'+accId).value = total.toFixed(2);
  }"""
html = html.replace(old_calc, new_calc)

# We also need to add a setTimeout call to calcRates inside buildRatesDashboard 
# so the "R 1.31m" appears immediately on load if values exist!
old_build_end = """        </div>
      `;
    });
  }"""
new_build_end = """        </div>
      `;
      // Trigger initial calculation to render the millions labels
      setTimeout(() => calcRates(acc.id), 50);
    });
  }"""
html = html.replace(old_build_end, new_build_end)


with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(html)
print("Updated HTML with millions formatting and deferred rates calc!")
