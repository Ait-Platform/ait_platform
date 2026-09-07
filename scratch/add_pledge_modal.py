import re

html_path = 'templates/program_sace/reading_hub.html'
with open(html_path, 'r', encoding='utf-8') as f:
    text = f.read()

# I will insert the blocking pledge modal just before the "Testing Mode Reset Modal"
pledge_modal = '''
        <!-- Blocking IP Pledge Modal (Required before map unlocks) -->
        {% if not progress.patent %}
        <div class="fixed inset-0 bg-slate-900 bg-opacity-80 z-50 flex items-center justify-center backdrop-blur-sm px-4">
            <div class="bg-white rounded-2xl shadow-2xl max-w-2xl w-full p-8 relative border-t-8 border-indigo-600">
                <div class="flex items-center justify-center mb-6">
                    <div class="w-16 h-16 bg-indigo-100 rounded-full flex items-center justify-center text-indigo-600">
                        <i class="fas fa-file-contract text-3xl"></i>
                    </div>
                </div>
                
                <h2 class="text-3xl font-black text-slate-800 text-center mb-2">Evaluator IP Pledge</h2>
                <p class="text-indigo-600 font-bold text-sm uppercase tracking-wide text-center mb-6 pb-4 border-b border-slate-100">
                    I Learn to Read English Using the LITRE Method
                </p>
                
                <div class="bg-slate-50 border border-slate-200 p-6 rounded-lg text-slate-700 leading-relaxed space-y-4 mb-8 text-left text-sm">
                    <p>Before unlocking the SACE Auditor Map, Evaluators must acknowledge the following Intellectual Property agreement.</p>
                    <p>By accessing the evaluation hub, you acknowledge that the digital framework, visual mapping, and associated teaching methodologies are the protected Intellectual Property of the Archoney Institute of Technology (AIT).</p>
                    <ul class="list-disc list-inside ml-2 space-y-2 font-semibold text-slate-800">
                        <li>Access is provided solely for the purpose of SACE endorsement evaluation.</li>
                        <li>No part of the interactive platform or its proprietary methods may be copied, reproduced, or distributed.</li>
                    </ul>
                </div>
                
                <form action="{{ url_for('sace_bp.acknowledge_patent') }}" method="POST" class="flex justify-center">
                    <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                    <button type="submit" class="px-8 py-4 bg-indigo-600 hover:bg-indigo-700 text-white text-lg font-bold rounded-lg shadow-lg transition flex items-center w-full justify-center">
                        <i class="fas fa-check-circle mr-3"></i> I Agree & Unlock Map
                    </button>
                </form>
            </div>
        </div>
        {% endif %}
'''

text = text.replace('        <!-- Testing Mode Reset Modal -->', pledge_modal + '\n        <!-- Testing Mode Reset Modal -->')

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(text)
