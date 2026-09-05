import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

roster_js = """
    // Populate the lobby roster
    document.addEventListener('DOMContentLoaded', function() {
        const rosterList = document.getElementById('roster-list');
        const counter = document.getElementById('permanent-counter');
        const counterLobby = document.getElementById('attendance-counter');
        
        if (rosterList) {
            rosterList.innerHTML = '';
            mockAttendees.forEach((person, index) => {
                let sace = "10" + Math.floor(Math.random() * 900000 + 100000);
                rosterList.innerHTML += '<li class="p-4 hover:bg-white transition flex justify-between items-center">' +
                    '<div><p class="font-bold text-slate-800">' + person + '</p>' +
                    '<p class="text-xs text-slate-500 font-mono">SACE: ' + sace + '</p></div>' +
                    '<span class="bg-green-100 text-green-800 px-2 py-1 rounded text-xs font-bold"><i class="fas fa-check-circle mr-1"></i> Verified</span></li>';
            });
        }
        if (counter) counter.innerText = mockAttendees.length;
        if (counterLobby) counterLobby.innerText = mockAttendees.length;
    });
"""

text = text.replace("// Setup initial state", roster_js + "\n    // Setup initial state")

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
