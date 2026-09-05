import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# I will insert the caveat right after the <!-- Row 1: Header --> block ends.
caveat_html = '''
<!-- Evaluator Caveat -->
<div class="bg-amber-50 border-l-4 border-amber-500 p-4 mx-6 mt-4 shadow-sm">
    <div class="flex">
        <div class="flex-shrink-0">
            <i class="fas fa-exclamation-triangle text-amber-500"></i>
        </div>
        <div class="ml-3">
            <h3 class="text-sm font-medium text-amber-800">Evaluator Caveat: Interactive Structural Demo</h3>
            <div class="mt-2 text-sm text-amber-700">
                <p>This environment is a <strong>structural demonstration</strong> of how the AIT platform synchronizes facilitator and participant devices in real-time. For endorsement purposes, please view the <strong>Linear Presentation (PPP)</strong> from the SACE Hub to evaluate the actual program content.</p>
            </div>
        </div>
    </div>
</div>
'''

header_end = '''<div class="p-6 border-b border-slate-100 flex justify-between items-center">
<h1 class="text-2xl font-extrabold text-slate-800"><i class="fas fa-desktop text-indigo-600 mr-2"></i> SACE Endorsement Simulator</h1>
<a class="text-slate-500 hover:text-slate-700 font-semibold" href="{{ url_for('sace_bp.dashboard') }}"><i class="fas fa-arrow-left mr-1"></i> Back to Dashboard</a>
</div>'''

if header_end in text:
    text = text.replace(header_end, header_end + caveat_html)
else:
    print("Could not find header_end in simulator.html")

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
