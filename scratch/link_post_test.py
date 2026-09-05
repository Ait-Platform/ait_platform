import re

with open('templates/program_sace/reading_hub.html', 'r', encoding='utf-8') as f:
    text = f.read()

# I'll inject the button right after the simulator buttons container
old_block = '''<div class="text-center w-full md:w-1/2">
        <a href="{{ url_for('sace_bp.simulator') }}" class="w-full inline-flex items-center justify-center px-8 py-4 text-lg font-bold text-white bg-slate-700 rounded-xl hover:bg-slate-600 hover:scale-105 transition border border-slate-600">
            <i class="fas fa-mobile-alt mr-3"></i> 2. View Interactive Demo
        </a>
        <p class="text-slate-400 text-sm mt-3">Advanced: See how the platform synchronizes devices in real-time.</p>
    </div>
</div>'''

new_block = '''<div class="text-center w-full md:w-1/2">
        <a href="{{ url_for('sace_bp.simulator') }}" class="w-full inline-flex items-center justify-center px-8 py-4 text-lg font-bold text-white bg-slate-700 rounded-xl hover:bg-slate-600 hover:scale-105 transition border border-slate-600">
            <i class="fas fa-mobile-alt mr-3"></i> 2. View Interactive Demo
        </a>
        <p class="text-slate-400 text-sm mt-3">Advanced: See how the platform synchronizes devices in real-time.</p>
    </div>
</div>

<div class="mt-8 pt-8 border-t border-slate-700 relative z-10 text-center">
    <h3 class="text-xl font-bold text-white mb-4">Workshop Completion & Certification</h3>
    <a href="{{ url_for('sace_bp.post_test') }}" class="inline-flex items-center justify-center px-8 py-3 text-lg font-bold text-indigo-900 bg-yellow-400 rounded-xl hover:bg-yellow-300 hover:scale-105 transition shadow-lg">
        <i class="fas fa-clipboard-list mr-3"></i> Take Post-Test & Get Certificate
    </a>
</div>'''

text = text.replace(old_block, new_block)

with open('templates/program_sace/reading_hub.html', 'w', encoding='utf-8') as f:
    f.write(text)

