import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

caveat_html = '''
<!-- Evaluator Caveat -->
<div class="bg-amber-50 border-l-4 border-amber-500 p-4 mb-6 shadow-sm rounded-r">
    <div class="flex">
        <div class="flex-shrink-0">
            <i class="fas fa-exclamation-triangle text-amber-500"></i>
        </div>
        <div class="ml-3">
            <h3 class="text-sm font-bold text-amber-800">Evaluator Note: Interactive Demonstration</h3>
            <div class="mt-1 text-sm text-amber-700">
                <p>This environment is a <strong>structural demonstration</strong> of how the AIT platform synchronizes facilitator and participant devices. For endorsement purposes, you may also view the <strong>Linear Presentation (PPP)</strong> from the SACE Hub.</p>
            </div>
        </div>
    </div>
</div>
'''

text = text.replace('<h2 class="text-3xl font-extrabold text-indigo-900 mb-6">SACE Auditor Guide</h2>', '<h2 class="text-3xl font-extrabold text-indigo-900 mb-6">SACE Auditor Guide</h2>\n' + caveat_html)

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
