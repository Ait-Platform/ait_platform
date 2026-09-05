import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Extract from <script> to end
pattern = r'<script>.*?function switchTab\(tabId\).*?</script>'

proper_script = """<script>
function switchTab(tabId) {
    // Hide all tabs
    document.getElementById('tab-a').classList.add('hidden');
    document.getElementById('tab-f').classList.add('hidden');
    document.getElementById('tab-p').classList.add('hidden');
    
    // Reset buttons to look "shut" and disabled
    const inactiveClass = "flex items-center px-4 py-2 bg-slate-800 text-slate-500 font-bold rounded-t-lg transition border-b-2 border-transparent opacity-50 hover:opacity-100 hover:bg-slate-700";
    document.getElementById('btn-tab-a').className = inactiveClass;
    document.getElementById('btn-tab-f').className = inactiveClass;
    document.getElementById('btn-tab-p').className = inactiveClass;
    
    // Reset lights to RED (shut)
    const redLight = "w-3 h-3 rounded-full bg-red-600 shadow-[0_0_8px_rgba(220,38,38,0.5)] mr-2"; 
    const greenLight = "w-3 h-3 rounded-full bg-green-500 shadow-[0_0_12px_rgba(34,197,94,1)] mr-2";
    if (document.getElementById('light-f')) document.getElementById('light-f').className = redLight;
    if (document.getElementById('light-p')) document.getElementById('light-p').className = redLight;

    // Log auditor navigation to database
    try {
        const csrfMeta = document.querySelector('meta[name="csrf-token"]');
        const csrfToken = csrfMeta ? csrfMeta.getAttribute('content') : '';
        fetch('/sace/log_event', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': csrfToken
            },
            body: JSON.stringify({
                action: 'AUDITOR_VIEWED_TAB_' + tabId.toUpperCase(),
                details: 'Auditor switched to ' + (tabId === 'a' ? 'Guide' : tabId === 'f' ? 'Facilitator Board' : 'Participant Board')
            })
        });
    } catch(e) {
        console.error("Audit log failed", e);
    }

    // Show active tab
    document.getElementById('tab-' + tabId).classList.remove('hidden');
    
    // Style active button and set green light
    if (tabId === 'a') {
        document.getElementById('btn-tab-a').className = "flex items-center px-4 py-2 bg-indigo-600 text-white font-bold rounded-t-lg transition border-b-2 border-indigo-400";
        document.getElementById('dynamicAuditorNote').classList.add('hidden');
    } else if (tabId === 'f') {
        document.getElementById('btn-tab-f').className = "flex items-center px-4 py-2 bg-white text-slate-900 font-bold rounded-t-lg transition border-b-2 border-green-500";
        if (document.getElementById('light-f')) document.getElementById('light-f').className = greenLight;
        document.getElementById('dynamicAuditorNote').classList.remove('hidden');
    } else if (tabId === 'p') {
        document.getElementById('btn-tab-p').className = "flex items-center px-4 py-2 bg-white text-slate-900 font-bold rounded-t-lg transition border-b-2 border-green-500";
        if (document.getElementById('light-p')) document.getElementById('light-p').className = greenLight;
        document.getElementById('dynamicAuditorNote').classList.remove('hidden');
    }
}

// Add event listener to catch messages from iframe
window.addEventListener('message', function(event) {
    if (event.data && event.data.action === 'switchToParticipant') {
        switchTab('p');
    }
});

// Ensure A is open on load
switchTab('a');

// Sync loop
setInterval(function() {
    fetch('/sace/workshop/get_state')
        .then(response => response.json())
        .then(data => {
            updateAuditorBanner(data.active_slide || 1);
        }).catch(err => console.error(err));
}, 1000);

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

function updateAuditorBanner(slideNumber) {
    const msgEl = document.getElementById('auditorMessage');
    const syncEl = document.getElementById('syncText');
    
    if (syncEl) syncEl.innerText = "Slide " + slideNumber;
    
    if(slideNumber == 1) {
        if(document.getElementById('a-status-f')) {
            document.getElementById('a-status-f').innerHTML = '<i class="fas fa-play-circle mr-1"></i> Active (Slide 1)';
            document.getElementById('a-status-f').className = 'font-bold text-green-600';
            document.getElementById('a-status-p').innerHTML = '<i class="fas fa-sync fa-spin mr-1"></i> Synced to Timetable';
            document.getElementById('a-status-p').className = 'font-bold text-green-600';
        }
        if(msgEl) msgEl.innerText = "Slide 1 - Observe how the Timetable is dynamically rendered to all Participants seamlessly.";
    } else if(slideNumber == 2) {
        if(document.getElementById('a-status-f')) {
            document.getElementById('a-status-f').innerHTML = '<i class="fas fa-play-circle mr-1"></i> Active (Slide 2)';
            document.getElementById('a-status-p').innerHTML = '<i class="fas fa-sync fa-spin mr-1"></i> Synced to Presentation';
        }
        if(msgEl) msgEl.innerText = "Slide 2 - The presentation content begins. The Facilitator follows structured notes.";
    }
}
</script>"""

text = re.sub(pattern, proper_script, text, flags=re.DOTALL)

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
