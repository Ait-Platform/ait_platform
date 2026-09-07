import re

html_path = 'templates/program_sace/reading_hub.html'
with open(html_path, 'r', encoding='utf-8') as f:
    text = f.read()

# Completely remove all view-pledge-modal blocks
pattern = r'<!-- View Only Pledge Modal -->.*?</div>\s*</div>'
text = re.sub(pattern, '', text, flags=re.DOTALL)

# Add exactly one at the very end before the last endblock
view_modal = '''
<!-- View Only Pledge Modal -->
<div id="view-pledge-modal" class="hidden fixed inset-0 bg-slate-900 bg-opacity-75 z-50 flex items-center justify-center backdrop-blur-sm px-4">
    <div class="bg-white rounded-2xl shadow-2xl max-w-2xl w-full p-8 relative border-t-8 border-indigo-600">
        <button onclick="document.getElementById('view-pledge-modal').classList.add('hidden')" class="absolute top-4 right-4 text-slate-400 hover:text-slate-600 transition">
            <i class="fas fa-times text-2xl"></i>
        </button>
        
        <div class="flex items-center justify-center mb-6">
            <div class="w-16 h-16 bg-indigo-100 rounded-full flex items-center justify-center text-indigo-600">
                <i class="fas fa-file-contract text-3xl"></i>
            </div>
        </div>
        
        <h2 class="text-3xl font-black text-slate-800 text-center mb-2">Evaluator IP Pledge</h2>
        <p class="text-indigo-600 font-bold text-sm uppercase tracking-wide text-center mb-6 pb-4 border-b border-slate-100">
            I Learn to Read English Using the LITRE Method
        </p>
        
        <div class="bg-slate-50 border border-slate-200 p-6 rounded-lg text-slate-700 leading-relaxed space-y-4 mb-2 text-left text-sm">
            <p>By accessing the evaluation hub, you acknowledge that the digital framework, visual mapping, and associated teaching methodologies are the protected Intellectual Property of the Archoney Institute of Technology (AIT).</p>
            <ul class="list-disc list-inside ml-2 space-y-2 font-semibold text-slate-800">
                <li>Access is provided solely for the purpose of SACE endorsement evaluation.</li>
                <li>No part of the interactive platform or its proprietary methods may be copied, reproduced, or distributed.</li>
            </ul>
        </div>
        <p class="text-center text-emerald-600 font-bold mt-4">
            <i class="fas fa-check-circle mr-2"></i> You agreed to this pledge during registration.
        </p>
    </div>
</div>
'''

# Find the LAST {% endblock %} and replace it
last_idx = text.rfind('{% endblock %}')
if last_idx != -1:
    text = text[:last_idx] + view_modal + '\n{% endblock %}' + text[last_idx + len('{% endblock %}'):]

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(text)
