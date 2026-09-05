import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Change the HTML for complianceLink to use a JS function
old_link = """<a id="complianceLink" href="#" target="_blank" class="inline-flex items-center px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white text-sm font-bold rounded shadow-sm transition">
                            <i class="fas fa-file-pdf mr-2"></i>
                            <span id="complianceText">View Current Annexure</span>
                        </a>"""
new_link = """<a id="complianceLink" href="#" onclick="simulatePdfDownload(event)" class="inline-flex items-center px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white text-sm font-bold rounded shadow-sm transition">
                            <i id="complianceIcon" class="fas fa-file-pdf mr-2"></i>
                            <span id="complianceText">View Current Annexure</span>
                        </a>"""
text = text.replace(old_link, new_link)

# Add the simulatePdfDownload JS function
js_func = """
function simulatePdfDownload(e) {
    e.preventDefault();
    const btnText = document.getElementById('complianceText');
    const btnIcon = document.getElementById('complianceIcon');
    const originalText = btnText.innerText;
    
    // Simulate opening in external PDF app
    btnIcon.className = "fas fa-check-circle mr-2 text-green-300";
    btnText.innerText = "Opened in external PDF App!";
    
    setTimeout(() => {
        btnIcon.className = "fas fa-file-pdf mr-2";
        btnText.innerText = originalText;
    }, 3000);
}
"""
text = text.replace("function updateAuditorBanner(slideNumber) {", js_func + "\nfunction updateAuditorBanner(slideNumber) {")

# Remove the actual dynamic href injections so it doesn't navigate
# Replace {% if docs['annexure_a'] %}...
text = re.sub(r'\{% if docs\[\'annexure_a\'\] %\}.*?linkEl\.href = ".*?";.*?\{% else %\}.*?linkEl\.href = "#";.*?\{% endif %\}', '', text, flags=re.DOTALL)
text = re.sub(r'\{% if docs\[\'annexure_b\'\] %\}.*?linkEl\.href = ".*?";.*?\{% else %\}.*?linkEl\.href = "#";.*?\{% endif %\}', '', text, flags=re.DOTALL)

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
