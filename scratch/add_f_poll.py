import re

with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    text = f.read()

poll_code = """
    // REAL SERVER POLLING for F-Board
    setInterval(() => {
        if (evaluatorMode) return; // don't poll if offline
        
        const csrfToken = document.querySelector('meta[name="csrf-token"]').getAttribute('content');
        fetch('/sace/workshop/get_state', {
            headers: {'X-CSRFToken': csrfToken}
        })
        .then(res => res.json())
        .then(data => {
            if (data.status === 'success') {
                if (data.state && sessionState !== data.state) {
                    sessionState = data.state;
                    updateView();
                }
                
                // Update Live Roster
                if (data.roster && document.getElementById('roster-list')) {
                    const list = document.getElementById('roster-list');
                    const counter = document.getElementById('permanent-counter');
                    list.innerHTML = '';
                    data.roster.forEach(person => {
                        list.innerHTML += <li class="p-4 hover:bg-white transition flex justify-between items-center">
                            <div><p class="font-bold text-slate-800"></p>
                            <p class="text-xs text-slate-500 font-mono">SACE: </p></div>
                            <span class="bg-green-100 text-green-800 px-2 py-1 rounded text-xs font-bold"><i class="fas fa-check-circle mr-1"></i> Verified</span></li>;
                    });
                    if(counter) counter.innerText = data.roster.length;
                    const dashCounter = document.getElementById('attendance-counter');
                    if(dashCounter) dashCounter.innerText = data.roster.length;
                }
                
                // Update Poll Results (Simulated from actual answers in DB)
                if (data.answers) {
                    let trues = 0; let falses = 0;
                    data.answers.forEach(a => {
                        if (a.answer_data === 'TRUE') trues++;
                        if (a.answer_data === 'FALSE') falses++;
                    });
                    const tBar = document.getElementById('true-bar-slide');
                    const fBar = document.getElementById('false-bar-slide');
                    const tCount = document.getElementById('true-count-slide');
                    const fCount = document.getElementById('false-count-slide');
                    
                    if (tBar && fBar) {
                        const total = trues + falses;
                        if (total > 0) {
                            tBar.style.width = (trues / total * 100) + '%';
                            fBar.style.width = (falses / total * 100) + '%';
                            tCount.innerText = trues;
                            fCount.innerText = falses;
                        }
                    }
                }
            }
        }).catch(err => console.log('Polling error:', err));
    }, 2000);
"""

text = text.replace('// Auto-poll state every 2 seconds to get live attendance', poll_code + '\n    // Auto-poll state every 2 seconds to get live attendance')

with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(text)
