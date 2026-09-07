import re

# ------------- 1. R SIDE (provisioning_map.html) -------------
file1 = 'templates/program_sace/provisioning_map.html'
with open(file1, 'r', encoding='utf-8') as f:
    text1 = f.read()

# Replace inline About text with empty string (it's gone from the main flow)
inline_about_r = '''        <!-- About AIT Submission -->
        <div class="px-8 pb-6">
            <h3 class="text-lg font-bold text-slate-900 border-b border-slate-200 pb-2 mb-4"><i class="fas fa-info-circle text-indigo-600 mr-2"></i> About the AIT Submission</h3>
            <p class="text-slate-900 font-medium text-base leading-relaxed mb-4">
                The Archoney Institute of Technology (AIT) presents this submission for SACE endorsement of the <strong>I Learn to Read English Using the LITRE Method</strong>. 
            </p>
            <p class="text-slate-900 font-medium text-base leading-relaxed mb-2">
                AIT has generated this secure portal to allow you to seamlessly assign and monitor SACE Auditors and document flows by:
            </p>
            <ul class="list-disc list-inside text-slate-900 font-medium text-base leading-relaxed mb-2 ml-2 space-y-1">
                <li>generating secure access links for them to evaluate the digital framework,</li>
                <li>integrating a document tracker allowing you to monitor all forms and auditors,</li>
                <li>and maintaining intellectual property compliance.</li>
            </ul>
        </div>'''
text1 = text1.replace(inline_about_r, '')

# Inject button into the Actions bar (for both pledged and unpledged states)
button_html = '''<button onclick="openAboutModal()" class="px-4 py-2 bg-sky-50 text-sky-700 hover:bg-sky-100 font-bold rounded-lg transition border border-sky-200 shadow-sm flex items-center">
                <i class="fas fa-info-circle mr-2"></i> About AIT Submission
            </button>
            '''

text1 = text1.replace('{% if has_pledged %}', '{% if has_pledged %}\n            ' + button_html)
text1 = text1.replace('{% else %}\n            <button onclick="openPledgeModal()"', '{% else %}\n            ' + button_html + '<button onclick="openPledgeModal()"')

# Inject About Modal HTML right after the Pledge Modal
about_modal_r = '''
<!-- About Modal (R) -->
<div id="about-modal" class="fixed inset-0 z-[60] bg-slate-900 bg-opacity-75 hidden flex items-center justify-center p-4 backdrop-blur-sm">
    <div class="bg-white rounded-xl shadow-2xl w-full max-w-2xl overflow-hidden border border-slate-200 flex flex-col max-h-[90vh]">
        <div class="px-6 py-4 border-b border-slate-100 flex justify-between items-center bg-slate-50">
            <h3 class="font-bold text-xl text-slate-800"><i class="fas fa-info-circle text-indigo-600 mr-2"></i> About the AIT Submission</h3>
            <button onclick="closeAboutModal()" class="text-slate-400 hover:text-slate-600 transition">
                <i class="fas fa-times text-xl"></i>
            </button>
        </div>
        <div class="p-6 overflow-y-auto text-slate-900 font-medium text-base leading-relaxed space-y-4">
            <p>
                The Archoney Institute of Technology (AIT) presents this submission for SACE endorsement of the <strong>I Learn to Read English Using the LITRE Method</strong>.
            </p>
            <p>
                AIT has generated this secure portal to allow you to seamlessly assign and monitor SACE Auditors and document flows by:
            </p>
            <ul class="list-disc list-inside space-y-2 ml-2">
                <li>generating secure access links for them to evaluate the digital framework,</li>
                <li>integrating a document tracker allowing you to monitor all forms and auditors,</li>
                <li>and maintaining intellectual property compliance.</li>
            </ul>
        </div>
        <div class="px-6 py-4 bg-slate-50 border-t border-slate-100 flex justify-end">
            <button type="button" onclick="closeAboutModal()" class="px-6 py-2 bg-indigo-600 text-white font-bold rounded-lg shadow-sm hover:bg-indigo-700 transition">
                Understood
            </button>
        </div>
    </div>
</div>
'''

text1 = text1.replace('<!-- Post-Pledge Registration Modal -->', about_modal_r + '\n<!-- Post-Pledge Registration Modal -->')

# Add JS functions
js_funcs_r = '''
    function openAboutModal() {
        document.getElementById('about-modal').classList.remove('hidden');
    }
    function closeAboutModal() {
        document.getElementById('about-modal').classList.add('hidden');
    }
'''
text1 = text1.replace('function openPledgeModal()', js_funcs_r + '\n    function openPledgeModal()')

with open(file1, 'w', encoding='utf-8') as f:
    f.write(text1)


# ------------- 2. A SIDE (auditor_pledge.html) -------------
file2 = 'templates/program_sace/auditor_pledge.html'
with open(file2, 'r', encoding='utf-8') as f:
    text2 = f.read()

# Remove inline About text
inline_about_a = '''        <!-- About AIT Submission -->
        <div class="mb-6">
            <h3 class="text-lg font-bold text-slate-900 border-b border-slate-200 pb-2 mb-4"><i class="fas fa-info-circle text-indigo-600 mr-2"></i> About the AIT Submission</h3>
            <p class="text-slate-900 font-medium text-base leading-relaxed mb-4">
                The Archoney Institute of Technology (AIT) presents this submission for SACE endorsement of the <strong>I Learn to Read English Using the LITRE Method</strong>. 
            </p>
            <p class="text-slate-900 font-medium text-base leading-relaxed mb-2">
                AIT has generated this secure portal to allow you to seamlessly evaluate the digital framework by:
            </p>
            <ul class="list-disc list-inside text-slate-900 font-medium text-base leading-relaxed mb-2 ml-2 space-y-1">
                <li>providing secure, guided access to the evaluation material,</li>
                <li>integrating a progress tracker allowing you to monitor your evaluation,</li>
                <li>and maintaining intellectual property compliance.</li>
            </ul>
        </div>'''
text2 = text2.replace(inline_about_a, '')

# Add button to Header
old_header_a = '''        <h1 class="text-2xl font-black text-slate-800 tracking-tight mb-2">
            <i class="fas fa-file-contract text-indigo-600 mr-2"></i> Evaluator IP Pledge
        </h1>'''
new_header_a = '''        <div class="flex justify-between items-start mb-2">
            <h1 class="text-2xl font-black text-slate-800 tracking-tight">
                <i class="fas fa-file-contract text-indigo-600 mr-2"></i> Evaluator IP Pledge
            </h1>
            <button onclick="openAboutModal()" class="px-4 py-2 bg-sky-50 text-sky-700 hover:bg-sky-100 font-bold rounded-lg transition border border-sky-200 shadow-sm flex items-center text-sm">
                <i class="fas fa-info-circle mr-2"></i> About AIT Submission
            </button>
        </div>'''
text2 = text2.replace(old_header_a, new_header_a)

# Add Modal & JS at bottom of block content
about_modal_a = '''
<!-- About Modal (A) -->
<div id="about-modal" class="fixed inset-0 z-[60] bg-slate-900 bg-opacity-75 hidden flex items-center justify-center p-4 backdrop-blur-sm">
    <div class="bg-white rounded-xl shadow-2xl w-full max-w-2xl overflow-hidden border border-slate-200 flex flex-col max-h-[90vh]">
        <div class="px-6 py-4 border-b border-slate-100 flex justify-between items-center bg-slate-50">
            <h3 class="font-bold text-xl text-slate-800"><i class="fas fa-info-circle text-indigo-600 mr-2"></i> About the AIT Submission</h3>
            <button onclick="closeAboutModal()" class="text-slate-400 hover:text-slate-600 transition">
                <i class="fas fa-times text-xl"></i>
            </button>
        </div>
        <div class="p-6 overflow-y-auto text-slate-900 font-medium text-base leading-relaxed space-y-4">
            <p>
                The Archoney Institute of Technology (AIT) presents this submission for SACE endorsement of the <strong>I Learn to Read English Using the LITRE Method</strong>.
            </p>
            <p>
                AIT has generated this secure portal to allow you to seamlessly evaluate the digital framework by:
            </p>
            <ul class="list-disc list-inside space-y-2 ml-2">
                <li>providing secure, guided access to the evaluation material,</li>
                <li>integrating a progress tracker allowing you to monitor your evaluation,</li>
                <li>and maintaining intellectual property compliance.</li>
            </ul>
        </div>
        <div class="px-6 py-4 bg-slate-50 border-t border-slate-100 flex justify-end">
            <button type="button" onclick="closeAboutModal()" class="px-6 py-2 bg-indigo-600 text-white font-bold rounded-lg shadow-sm hover:bg-indigo-700 transition">
                Understood
            </button>
        </div>
    </div>
</div>

<script>
    function openAboutModal() {
        document.getElementById('about-modal').classList.remove('hidden');
    }
    function closeAboutModal() {
        document.getElementById('about-modal').classList.add('hidden');
    }
</script>
'''

text2 = text2.replace('{% endblock %}', about_modal_a + '\n{% endblock %}')

with open(file2, 'w', encoding='utf-8') as f:
    f.write(text2)

print("Moved About text to Modals.")
