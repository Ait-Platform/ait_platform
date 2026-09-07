import re

file_path = 'templates/program_sace/reading_hub.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

# Extract Application Document block
app_doc_block = '''                <!-- Application Document -->
                <div class="flex items-center justify-between p-4 bg-white border border-slate-200 rounded-lg hover:border-indigo-300 transition shadow-sm">
                    <div class="flex items-center w-1/3">
                        <i class="fas fa-file-signature text-indigo-400 mr-3 text-xl"></i>
                        <span class="font-bold text-slate-700">Application Document</span>
                    </div>
                    <div class="w-1/3 text-center">
                        <a href="{{ url_for('sace_bp.secure_view', doc_type='app_form') }}" class="px-4 py-2 bg-slate-100 hover:bg-slate-200 text-slate-700 text-sm font-bold rounded-md transition">View Document</a>
                    </div>
                    <div class="w-1/3 flex justify-end">
                        {% if progress.app_form %}<i class="fas fa-check-circle text-green-500 text-2xl"></i>{% else %}<span class="w-6 h-6 rounded-full border-2 border-slate-300"></span>{% endif %}
                    </div>
                </div>'''

# Extract old Patent block
patent_block = '''                <!-- Patent Docs Acknowledgement -->
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
                    <p class="text-xs text-slate-500 ml-8 mb-3">I acknowledge that the I Learn to Read English Using the LITRE Method are the protected Intellectual Property and Patented material of AIT.</p>
                    <div class="ml-8">
                        <button onclick="document.getElementById('view-pledge-modal').classList.remove('hidden')" class="px-4 py-1.5 bg-slate-50 border border-slate-200 hover:bg-slate-100 text-slate-700 text-sm font-bold rounded transition">
                            <i class="fas fa-eye mr-1"></i> View Pledge
                        </button>
                    </div>
                </div>'''

# Create new Patent block
new_patent_block = '''                <!-- Patent Docs Acknowledgement -->
                <div class="flex items-center justify-between p-4 bg-white border border-slate-200 rounded-lg hover:border-indigo-300 transition shadow-sm">
                    <div class="flex items-center w-1/3">
                        <i class="fas fa-certificate text-indigo-400 mr-3 text-xl"></i>
                        <span class="font-bold text-slate-700">AIT Intellectual Property</span>
                    </div>
                    <div class="w-1/3 text-center">
                        <button onclick="document.getElementById('view-pledge-modal').classList.remove('hidden')" class="px-4 py-2 bg-slate-100 hover:bg-slate-200 text-slate-700 text-sm font-bold rounded-md transition">View Pledge</button>
                    </div>
                    <div class="w-1/3 flex justify-end">
                        {% if progress.patent %}<i class="fas fa-check-circle text-green-500 text-2xl"></i>{% else %}<span class="w-6 h-6 rounded-full border-2 border-slate-300"></span>{% endif %}
                    </div>
                </div>'''

# Remove both blocks first
text = text.replace(app_doc_block + '\n', '')
text = text.replace(patent_block + '\n', '')
# In case it missed newline
text = text.replace(app_doc_block, '')
text = text.replace(patent_block, '')

# Add them back in the new order (Patent then App Doc)
target = '<div class="space-y-4">'
new_content = target + '\n' + new_patent_block + '\n\n' + app_doc_block
text = text.replace(target, new_content)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)

print("Reordered list and simplified pledge item.")
