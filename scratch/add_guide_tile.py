import re

with open('templates/program_sace/reading_hub.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Insert next to the Application Form tile
old_tile = """        <!-- Application Form -->
        <div class="flex flex-col p-6 border border-slate-200 rounded-xl bg-slate-50 hover:bg-slate-100 transition shadow-sm">"""

new_tile = """        <!-- Reviewer Guide -->
        <div class="flex flex-col p-6 border border-indigo-200 rounded-xl bg-indigo-50 hover:bg-indigo-100 transition shadow-sm">
            <div class="flex items-center justify-between mb-4">
                <div class="h-12 w-12 bg-indigo-600 text-white rounded-lg flex items-center justify-center font-bold text-xl"><i class="fas fa-book-open"></i></div>
            </div>
            <h3 class="text-xl font-bold text-slate-900 mb-2">SACE Reviewer Guide</h3>
            <p class="text-sm text-slate-600 mb-6 flex-grow">A professional guide explaining how to evaluate the platform using Offline Mode.</p>
            <a href="{{ url_for('sace.reviewer_guide') }}" class="w-full text-center px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-lg shadow-sm transition">
                <i class="fas fa-arrow-right mr-1"></i> Open Guide
            </a>
        </div>
        
        <!-- Application Form -->
        <div class="flex flex-col p-6 border border-slate-200 rounded-xl bg-slate-50 hover:bg-slate-100 transition shadow-sm">"""

if old_tile in content:
    content = content.replace(old_tile, new_tile)
    with open('templates/program_sace/reading_hub.html', 'w', encoding='utf-8') as f:
        f.write(content)
