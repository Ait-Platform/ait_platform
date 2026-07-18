import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    html = f.read()

# 1. Add formatNumberSpace and parseAmount functions
formatter_js = """
  window.formatNumberSpace = function(val) {
    if(val === undefined || val === null || val === '') return '';
    let raw = String(val).replace(/[^\\d.]/g, '');
    if(!raw) return '';
    let parts = raw.split('.');
    parts[0] = parts[0].replace(/\\B(?=(\\d{3})+(?!\\d))/g, ' ');
    return parts.join('.');
  }
  
  window.parseAmount = function(val) {
    if(val === undefined || val === null || val === '') return 0;
    return parseFloat(String(val).replace(/[^\\d.-]/g, '')) || 0;
  }
"""

if "window.formatNumberSpace" not in html:
    html = html.replace("  // --- STEP 1: ACCOUNTS ---", formatter_js + "\n  // --- STEP 1: ACCOUNTS ---")

# 2. Change Market Value and Rateable Value inputs from number to text with formatting
mv_old = """<input type="number" id="mv-${acc.id}" class="rate-market-value"""
mv_new = """<input type="text" id="mv-${acc.id}" class="rate-market-value"""
html = html.replace(mv_old, mv_new)

rv_old = """<input type="number" id="rv-${acc.id}" class="rate-rateable-value"""
rv_new = """<input type="text" id="rv-${acc.id}" class="rate-rateable-value"""
html = html.replace(rv_old, rv_new)

# Add value formatting and oninput formatting
html = html.replace('value="${savedRate.market_value || \'\'}" oninput="calcRates(\'${acc.id}\')"', 
                    'value="${savedRate.market_value ? formatNumberSpace(savedRate.market_value) : \'\'}" oninput="this.value=formatNumberSpace(this.value); calcRates(\'${acc.id}\')"')
html = html.replace('value="${savedRate.rateable_value || \'\'}" oninput="calcRates(\'${acc.id}\')"', 
                    'value="${savedRate.rateable_value ? formatNumberSpace(savedRate.rateable_value) : \'\'}" oninput="this.value=formatNumberSpace(this.value); calcRates(\'${acc.id}\')"')

# 3. Replace parseFloat with parseAmount in calcRates and gatherRates
html = html.replace("parseFloat(document.getElementById('mv-'+accId).value)", "parseAmount(document.getElementById('mv-'+accId).value)")
html = html.replace("parseFloat(document.getElementById('rv-'+accId).value)", "parseAmount(document.getElementById('rv-'+accId).value)")
html = html.replace("parseFloat(document.getElementById('gr-'+accId).value)", "parseAmount(document.getElementById('gr-'+accId).value)")
html = html.replace("parseFloat(document.getElementById('sr-'+accId).value)", "parseAmount(document.getElementById('sr-'+accId).value)")
html = html.replace("parseFloat(document.getElementById('def-'+accId).value)", "parseAmount(document.getElementById('def-'+accId).value)")

html = html.replace("parseFloat(card.querySelector('.rate-market-value').value)", "parseAmount(card.querySelector('.rate-market-value').value)")
html = html.replace("parseFloat(card.querySelector('.rate-rateable-value').value)", "parseAmount(card.querySelector('.rate-rateable-value').value)")
html = html.replace("parseFloat(card.querySelector('.rate-gen-randage').value)", "parseAmount(card.querySelector('.rate-gen-randage').value)")
html = html.replace("parseFloat(card.querySelector('.rate-sra-randage').value)", "parseAmount(card.querySelector('.rate-sra-randage').value)")
html = html.replace("parseFloat(card.querySelector('.rate-deferred').value)", "parseAmount(card.querySelector('.rate-deferred').value)")
html = html.replace("parseFloat(card.querySelector('.rate-sra-monthly').value)", "parseAmount(card.querySelector('.rate-sra-monthly').value)")
html = html.replace("parseFloat(card.querySelector('.rate-gen-monthly').value)", "parseAmount(card.querySelector('.rate-gen-monthly').value)")
html = html.replace("parseFloat(card.querySelector('.rate-amount').value)", "parseAmount(card.querySelector('.rate-amount').value)")

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(html)
print("Updated space formatting and parseAmount!")
