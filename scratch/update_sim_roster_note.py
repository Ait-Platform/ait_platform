import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# I want to add a note about the roster to the "Evaluation Evidence" or general instructions in Tab A.
auditor_note = """
                    <div class="bg-indigo-50 border-l-4 border-indigo-600 p-6 rounded-r-lg mb-8">
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
                        
                        <div class="bg-indigo-100/50 p-4 rounded mt-4 border border-indigo-200">
                            <h4 class="font-bold text-indigo-900 mb-1 text-sm"><i class="fas fa-clipboard-user mr-1"></i> Note on Attendance Tracking</h4>
                            <p class="text-xs text-indigo-800">For demonstration purposes, a mock list of attendees has been pre-loaded into the Facilitator Board. In a live environment, the system automatically builds this roster as each teacher enters their unique PIN, generating an airtight SACE compliance register.</p>
                        </div>

                        <h4 class="font-bold text-indigo-900 mb-2 mt-6">Evaluation Evidence</h4>
"""

# The previous block was exactly:
# <div class="bg-indigo-50 border-l-4 border-indigo-600 p-6 rounded-r-lg mb-8">
#    <h4 class="font-bold text-indigo-900 mb-2"><i class="fas fa-satellite-dish mr-2"></i>Live Room Status</h4>
#    <div class="grid grid-cols-2 gap-4 mb-4">...</div>
#    <h4 class="font-bold text-indigo-900 mb-2 mt-4">Evaluation Evidence</h4>
# Let's use regex to insert the Note on Attendance Tracking right before Evaluation Evidence.

text = re.sub(
    r'(<div class="grid grid-cols-2 gap-4 mb-4">.*?</div>\s*)<h4 class="font-bold text-indigo-900 mb-2 mt-4">Evaluation Evidence</h4>',
    r'\1<div class="bg-indigo-100/50 p-4 rounded mt-4 border border-indigo-200">\n<h4 class="font-bold text-indigo-900 mb-1 text-sm"><i class="fas fa-clipboard-user mr-1"></i> Note on Attendance Tracking</h4>\n<p class="text-xs text-indigo-800">For demonstration purposes, a mock list of 8 verified attendees has been pre-loaded into the Facilitator Board. In a live environment, the system automatically builds this roster as each teacher logs in, generating an airtight SACE compliance register.</p>\n</div>\n<h4 class="font-bold text-indigo-900 mb-2 mt-6">Evaluation Evidence</h4>',
    text,
    flags=re.DOTALL
)

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
