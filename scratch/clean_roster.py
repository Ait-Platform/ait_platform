import re

with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Remove the Kinesthetic block that got erroneously injected into the Roster Modal
roster_bad_chunk = '''        <div class="p-0 flex-grow overflow-y-auto bg-slate-50">
            <ul id="roster-list" class="divide-y divide-gray-100">
                <li class="p-6 text-center text-gray-400 text-sm italic">No participants have checked in yet.</li>
            </ul>
        </div>
        
            <div class="bg-white p-4 rounded-xl border border-slate-200 shadow-sm border-l-4 border-l-rose-500">
                <h4 class="font-bold text-rose-800 mb-2"><i class="fas fa-running mr-2"></i>Kinesthetic Memory</h4>
                <p>Instead of passive listening, the platform enforces physical movement and cognitive mapping (e.g., <strong>The Vowel Hops</strong> and <strong>The Number Map</strong>). This proves that the workshop actively uses multisensory learning methodologies.</p>
            </div>'''

roster_good_chunk = '''        <div class="p-0 flex-grow overflow-y-auto bg-slate-50">
            <ul id="roster-list" class="divide-y divide-gray-100">
                <li class="p-6 text-center text-gray-400 text-sm italic">No participants have checked in yet.</li>
            </ul>
        </div>'''

content = content.replace(roster_bad_chunk, roster_good_chunk)

with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
