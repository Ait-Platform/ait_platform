import re

html_path = 'templates/program_sace/provisioning_map.html'
with open(html_path, 'r', encoding='utf-8') as f:
    text = f.read()

old_header = '''        <!-- Rule 4: Row 1 Header & Back Button -->
        <div class="flex justify-between items-start border-b border-slate-100 pb-4 mb-4">
            <div>
                <h1 class="text-2xl font-black text-slate-800">
                    <i class="fas fa-book-reader text-indigo-600 mr-2"></i> I Learn to Read Using the LITRE
                </h1>
                <p class="text-slate-500 font-bold mt-1 text-sm uppercase tracking-wide">SACE Control Centre</p>
            </div>
            <a href="{{ url_for('public_bp.welcome') }}" class="px-4 py-2 bg-slate-100 hover:bg-slate-200 text-slate-700 font-bold rounded-lg transition shadow-sm whitespace-nowrap ml-4">
                <i class="fas fa-sign-out-alt mr-1"></i> Exit Platform
            </a>
        </div>'''

new_header = '''        <!-- Rule 4: Row 1 Header & Back Button -->
        <div class="flex justify-between items-start pb-4">
            <div>
                <h1 class="text-xl font-black text-slate-800 tracking-tight">
                    <i class="fas fa-shield-alt text-indigo-600 mr-2"></i> ARCHONEY INSTITUTE OF TECHNOLOGY (AIT)
                </h1>
                <p class="text-slate-500 font-bold mt-1 text-sm uppercase tracking-wide">Provider Activity: I Learn to Read English Using the LITRE Method</p>
            </div>
            <a href="{{ url_for('public_bp.welcome') }}" class="px-5 py-2.5 bg-slate-100 text-slate-700 hover:bg-slate-200 font-bold rounded-lg transition border border-slate-200 shadow-sm flex items-center ml-4">
                <i class="fas fa-sign-out-alt mr-2"></i> Exit
            </a>
        </div>'''

text = text.replace(old_header, new_header)

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(text)
