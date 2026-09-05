import re

with open('templates/program_sace/reading_hub.html', 'r', encoding='utf-8') as f:
    text = f.read()

old_block = '''<a href="{{ url_for('sace_bp.simulator') }}" class="inline-flex items-center px-8 py-4 text-lg font-bold text-white bg-indigo-600 rounded-full hover:bg-indigo-500 hover:scale-105 transition shadow-[0_0_20px_rgba(79,70,229,0.4)] relative z-10">
            <i class="fas fa-play mr-3"></i> Launch Simulator
          </a>'''

new_block = '''
<div class="flex flex-col md:flex-row items-center justify-center gap-6 relative z-10">
    <div class="text-center w-full md:w-1/2">
        <a href="{{ url_for('sace_bp.presentation') }}" class="w-full inline-flex items-center justify-center px-8 py-4 text-lg font-bold text-white bg-indigo-600 rounded-xl hover:bg-indigo-500 hover:scale-105 transition shadow-[0_0_20px_rgba(79,70,229,0.4)]">
            <i class="fas fa-desktop mr-3"></i> 1. View Linear Presentation (PPP)
        </a>
        <p class="text-slate-400 text-sm mt-3">Start here. Review the actual content of the course linearly.</p>
    </div>
    
    <div class="text-center w-full md:w-1/2">
        <a href="{{ url_for('sace_bp.simulator') }}" class="w-full inline-flex items-center justify-center px-8 py-4 text-lg font-bold text-white bg-slate-700 rounded-xl hover:bg-slate-600 hover:scale-105 transition border border-slate-600">
            <i class="fas fa-mobile-alt mr-3"></i> 2. View Interactive Demo
        </a>
        <p class="text-slate-400 text-sm mt-3">Advanced: See how the platform synchronizes devices in real-time.</p>
    </div>
</div>
'''

text = text.replace(old_block, new_block)

with open('templates/program_sace/reading_hub.html', 'w', encoding='utf-8') as f:
    f.write(text)
