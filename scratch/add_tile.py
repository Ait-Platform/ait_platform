import re

with open('templates/program_sace/reading_hub.html', 'r', encoding='utf-8') as f:
    content = f.read()

report_tile = '''
    <!-- Third Row (Report) -->
    <div class="grid md:grid-cols-1 gap-6 mt-6">
        <a href="{{ url_for('sace_bp.evaluator_report') }}" class="flex flex-col p-6 border-2 border-emerald-500 rounded-xl bg-emerald-50 hover:bg-emerald-100 transition shadow-sm group">
            <div class="flex items-center justify-between mb-4">
                <div class="h-12 w-12 bg-emerald-600 text-white rounded-lg flex items-center justify-center font-bold text-xl"><i class="fas fa-file-contract"></i></div>
                <span class="bg-emerald-200 text-emerald-800 text-xs px-2 py-1 rounded-full font-bold uppercase tracking-wide">Audit Trail</span>
            </div>
            <h3 class="text-xl font-bold text-slate-900 mb-2">Export Evaluation Report</h3>
            <p class="text-sm text-slate-600 mb-6 flex-grow">Generate a professional, timestamped PDF paper trail of exactly what you tested today to pass onto the SACE Project Manager.</p>
            <div class="mt-auto flex items-center text-emerald-700 font-bold text-sm bg-emerald-100 px-4 py-2 rounded-lg justify-center group-hover:bg-emerald-200 transition">
                View Report <i class="fas fa-arrow-right ml-2 group-hover:translate-x-1 transition-transform"></i>
            </div>
        </a>
    </div>
'''

if 'Export Evaluation Report' not in content:
    # Just split by last two divs
    parts = content.rsplit('</div>\n  </div>\n</div>\n{% endblock %}', 1)
    if len(parts) == 2:
        content = parts[0] + report_tile + '\n</div>\n  </div>\n</div>\n{% endblock %}'
        with open('templates/program_sace/reading_hub.html', 'w', encoding='utf-8') as f:
            f.write(content)
