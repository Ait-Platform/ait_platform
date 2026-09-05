import re

html_path = 'templates/program_sace/provisioning_map.html'
with open(html_path, 'r', encoding='utf-8') as f:
    html = f.read()

# Replace the locked state to include the intro text
old_locked = '''        <!-- Locked State -->
        <div class="p-12 text-center bg-slate-50 border-t border-slate-100">
            <div class="w-24 h-24 bg-slate-200 rounded-full flex items-center justify-center mx-auto mb-6 shadow-inner">
                <i class="fas fa-lock text-4xl text-slate-400"></i>
            </div>
            <h2 class="text-2xl font-black text-slate-700 mb-3">Portal Locked</h2>
            <p class="text-slate-500 max-w-lg mx-auto mb-8 leading-relaxed">
                To access the SACE Auditor Provisioning tools, you must first acknowledge and sign the AIT Intellectual Property Pledge.
            </p>
            <button onclick="openPledgeModal()" class="px-8 py-3 bg-red-600 hover:bg-red-700 text-white text-lg font-bold rounded-xl shadow-md transition group">
                <i class="fas fa-file-signature mr-2 group-hover:scale-110 transition-transform"></i> Sign IP Pledge to Unlock
            </button>
        </div>'''

new_locked = '''        <!-- Locked State -->
        <div class="bg-white p-8 border-t border-slate-100">
            <h3 class="text-lg font-bold text-slate-800 border-b border-slate-100 pb-2 mb-4"><i class="fas fa-info-circle text-indigo-500 mr-2"></i> About the AIT Submission</h3>
            <p class="text-slate-600 leading-relaxed mb-4">
                The Archoney Institute of Technology (AIT) presents this submission for SACE endorsement of the <strong>LITRE Blending Machine</strong> reading intervention program. 
            </p>
            <p class="text-slate-600 leading-relaxed mb-8">
                Due to the proprietary nature of the AIT LITRE Simulator and interactive methodology, standard PDF document review is insufficient. AIT has generated this secure portal to allow you to seamlessly provision SACE Evaluators, generating secure access links for them to evaluate the digital framework firsthand while maintaining intellectual property compliance.
            </p>
        </div>
        <div class="p-12 text-center bg-slate-50 border-t border-slate-200">
            <div class="w-24 h-24 bg-slate-200 rounded-full flex items-center justify-center mx-auto mb-6 shadow-inner">
                <i class="fas fa-lock text-4xl text-slate-400"></i>
            </div>
            <h2 class="text-2xl font-black text-slate-700 mb-3">Portal Locked</h2>
            <p class="text-slate-500 max-w-lg mx-auto mb-8 leading-relaxed">
                To access the SACE Auditor Provisioning tools and Provider Documents, you must first acknowledge and sign the AIT Intellectual Property Pledge on behalf of SACE.
            </p>
            <button onclick="openPledgeModal()" class="px-8 py-3 bg-red-600 hover:bg-red-700 text-white text-lg font-bold rounded-xl shadow-md transition group">
                <i class="fas fa-file-signature mr-2 group-hover:scale-110 transition-transform"></i> Sign IP Pledge to Unlock
            </button>
        </div>'''

html = html.replace(old_locked, new_locked)

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(html)
