import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    html = f.read()

old_js = """      if (response.ok) {
        // Clear draft on successful save!
        window.location.href = "{{ url_for('billing_bp.architecture_summary', property_id=property.id) }}?from_wizard=1";
      } else {
        const errData = await response.json();
        alert("Server Error: " + (errData.error || "Please ensure all data is valid."));
      }"""

new_js = """      if (response.ok) {
        // Clear draft on successful save!
        window.location.href = "{{ url_for('billing_bp.architecture_summary', property_id=property.id) }}?from_wizard=1";
      } else {
        const text = await response.text();
        try {
           const errData = JSON.parse(text);
           alert("Server Error: " + (errData.error || "Please ensure all data is valid."));
        } catch (e) {
           if (text.toLowerCase().includes("csrf") || text.toLowerCase().includes("bad request")) {
               alert("Your session expired while you were filling out the wizard! Please save your data to a text file (just in case), do a hard refresh (Ctrl+Shift+R), and try again.");
           } else {
               alert("An unexpected server error occurred (500). The data could not be parsed.");
           }
        }
      }"""

if "JSON.parse(text)" not in html:
    html = html.replace(old_js, new_js)
    with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
        f.write(html)
    print("JS error handling updated!")
else:
    print("Already updated!")
