import re

file_path = 'templates/program_sace/reading_hub.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

pattern_pd = r'\s*<!-- Patent Docs -->\s*<div class="flex items-center justify-between p-4 bg-white border border-slate-200 rounded-lg hover:border-indigo-300 transition shadow-sm">\s*<div class="flex items-center w-1/3">\s*<i class="fas fa-certificate text-indigo-400 mr-3 text-xl"></i>\s*<span class="font-bold text-slate-700">Patent Docs</span>\s*</div>\s*<div class="w-1/3 text-center">\s*<a href="\{\{ url_for\(\'sace_bp\.secure_view\', doc_type=\'patent\'\) \}\}" class="px-4 py-2 bg-slate-100 hover:bg-slate-200 text-slate-700 text-sm font-bold rounded-md transition">View Document</a>\s*</div>\s*<div class="w-1/3 flex justify-end">\s*\{% if progress\.patent %\}<i class="fas fa-check-circle text-green-500 text-2xl"></i>\{% else %\}<span class="w-6 h-6 rounded-full border-2 border-slate-300"></span>\{% endif %\}\s*</div>\s*</div>'

replacement_pd = '''
                <!-- Patent Docs Acknowledgement -->
                <div class="flex flex-col p-4 bg-white border border-slate-200 rounded-lg hover:border-indigo-300 transition shadow-sm">
                    <div class="flex items-center justify-between mb-2">
                        <div class="flex items-center">
                            <i class="fas fa-certificate text-indigo-400 mr-3 text-xl"></i>
                            <span class="font-bold text-slate-700">AIT Intellectual Property</span>
                        </div>
                        <div class="flex justify-end">
                            {% if progress.patent %}<i class="fas fa-check-circle text-green-500 text-2xl"></i>{% else %}<span class="w-6 h-6 rounded-full border-2 border-slate-300"></span>{% endif %}
                        </div>
                    </div>
                    <p class="text-xs text-slate-500 ml-8 mb-3">I acknowledge that the LITRE Blending Machine and associated methodology are the protected Intellectual Property and Patented material of AIT.</p>
                    {% if not progress.patent %}
                    <form action="{{ url_for('sace_bp.acknowledge_patent') }}" method="POST" class="ml-8">
                        <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                        <button type="submit" class="px-4 py-1.5 bg-indigo-50 border border-indigo-200 hover:bg-indigo-100 text-indigo-700 text-sm font-bold rounded transition">Acknowledge</button>
                    </form>
                    {% endif %}
                </div>'''

text = re.sub(pattern_pd, replacement_pd, text)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
