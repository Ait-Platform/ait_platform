import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Add Live Status to Tab A
status_html = """                    <div class="bg-indigo-50 border-l-4 border-indigo-600 p-6 rounded-r-lg mb-8">
                        <h4 class="font-bold text-indigo-900 mb-2"><i class="fas fa-satellite-dish mr-2"></i>Live Room Status</h4>
                        <div class="grid grid-cols-2 gap-4 mb-4">
                            <div class="bg-white p-3 rounded shadow-sm border border-slate-200">
                                <p class="text-xs text-slate-500 font-bold uppercase mb-1">Facilitator</p>
                                <p id="a-status-f" class="font-bold text-amber-600"><i class="fas fa-clock mr-1"></i> Waiting for Start</p>
                            </div>
                            <div class="bg-white p-3 rounded shadow-sm border border-slate-200">
                                <p class="text-xs text-slate-500 font-bold uppercase mb-1">Participants</p>
                                <p id="a-status-p" class="font-bold text-slate-600"><i class="fas fa-users mr-1"></i> Joining Lobby</p>
                            </div>
                        </div>
                        <h4 class="font-bold text-indigo-900 mb-2 mt-4">Evaluation Evidence</h4>
                        <p class="text-sm text-indigo-800 mb-4" id="auditorMessage">
                            Waiting for workshop to begin...
                        </p>
                        <a id="complianceLink" href="#" target="_blank" class="inline-flex items-center px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white text-sm font-bold rounded shadow-sm transition">
                            <i class="fas fa-file-pdf mr-2"></i>
                            <span id="complianceText">View Current Annexure</span>
                        </a>
                    </div>"""

text = re.sub(r'<div class="bg-indigo-50 border-l-4 border-indigo-600 p-6 rounded-r-lg mb-8">.*?</div>', status_html, text, flags=re.DOTALL)

# Add fetch call to log_event inside switchTab
log_js = """    // Log auditor navigation to database
    fetch('/sace/log_event', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': document.querySelector('meta[name="csrf-token"]').getAttribute('content')
        },
        body: JSON.stringify({
            action: 'AUDITOR_VIEWED_TAB_' + tabId.toUpperCase(),
            details: 'Auditor switched to ' + (tabId === 'a' ? 'Guide' : tabId === 'f' ? 'Facilitator Board' : 'Participant Board')
        })
    });"""

text = text.replace("// Show active tab", log_js + "\n\n    // Show active tab")

# Add logic to update the A tab status text when sync loop runs
sync_js = """    if(slideNumber == 1) {
        document.getElementById('a-status-f').innerHTML = '<i class="fas fa-play-circle mr-1"></i> Active (Slide 1)';
        document.getElementById('a-status-f').className = 'font-bold text-green-600';
        document.getElementById('a-status-p').innerHTML = '<i class="fas fa-sync fa-spin mr-1"></i> Synced to Timetable';
        document.getElementById('a-status-p').className = 'font-bold text-green-600';
"""
text = text.replace("if(slideNumber == 1) {", sync_js)

sync_js_2 = """    } else if(slideNumber == 2) {
        document.getElementById('a-status-f').innerHTML = '<i class="fas fa-play-circle mr-1"></i> Active (Slide 2)';
        document.getElementById('a-status-p').innerHTML = '<i class="fas fa-sync fa-spin mr-1"></i> Synced to Presentation';
"""
text = text.replace("} else if(slideNumber == 2) {", sync_js_2)

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
