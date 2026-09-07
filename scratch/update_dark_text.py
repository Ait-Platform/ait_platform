import re

# 1. Darken the text in provisioning_map.html
file1 = 'templates/program_sace/provisioning_map.html'
with open(file1, 'r', encoding='utf-8') as f:
    text1 = f.read()

old_about_r = '''        <!-- About AIT Submission -->
        <div class="px-8 pb-6">
            <h3 class="text-lg font-bold text-slate-800 border-b border-slate-100 pb-2 mb-4"><i class="fas fa-info-circle text-indigo-500 mr-2"></i> About the AIT Submission</h3>
            <p class="text-slate-600 text-base leading-relaxed mb-4">
                The Archoney Institute of Technology (AIT) presents this submission for SACE endorsement of the <strong>I Learn to Read English Using the LITRE Method</strong>. 
            </p>
            <p class="text-slate-600 text-base leading-relaxed mb-2">
                AIT has generated this secure portal to allow you to seamlessly assign and monitor SACE Auditors and document flows by :
            </p>
            <ul class="list-disc list-inside text-slate-600 text-base leading-relaxed mb-2 ml-2 space-y-1">
                <li>generating secure access links for them to evaluate the digital framework</li>
                <li>integrating document tracker allowing you to monitor all forms and auditors.</li>
                <li>maintaining intellectual property compliance,</li>
            </ul>
        </div>'''

new_about_r = '''        <!-- About AIT Submission -->
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

text1 = text1.replace(old_about_r, new_about_r)
with open(file1, 'w', encoding='utf-8') as f:
    f.write(text1)


# 2. Add the darkened About section to auditor_pledge.html
file2 = 'templates/program_sace/auditor_pledge.html'
with open(file2, 'r', encoding='utf-8') as f:
    text2 = f.read()

about_a = '''        <!-- About AIT Submission -->
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
        </div>
        
'''

# insert before the pledge box
if "<!-- About AIT Submission -->" not in text2:
    text2 = text2.replace('<div class="bg-slate-50', about_a + '<div class="bg-slate-50')

# also darken the pledge box text for A
text2 = text2.replace('text-slate-700 leading-relaxed', 'text-slate-900 font-medium leading-relaxed')

with open(file2, 'w', encoding='utf-8') as f:
    f.write(text2)

print("Updated text shades.")
