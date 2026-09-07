import re

file_path = 'templates/program_sace/reading_hub.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

app_doc = '''                <!-- Application Document -->
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

annexures = '''                <!-- Annexures -->
                <div class="flex items-center justify-between p-4 bg-white border border-slate-200 rounded-lg hover:border-indigo-300 transition shadow-sm">
                    <div class="flex items-center w-1/3">
                        <i class="fas fa-folder-open text-indigo-400 mr-3 text-xl"></i>
                        <span class="font-bold text-slate-700">Annexures A-E</span>
                    </div>
                    <div class="w-1/3 text-center">
                        <a href="{{ url_for('sace_bp.secure_view', doc_type='annexures') }}" class="px-4 py-2 bg-slate-100 hover:bg-slate-200 text-slate-700 text-sm font-bold rounded-md transition">View Document</a>
                    </div>
                    <div class="w-1/3 flex justify-end">
                        {% if progress.annexures %}<i class="fas fa-check-circle text-green-500 text-2xl"></i>{% else %}<span class="w-6 h-6 rounded-full border-2 border-slate-300"></span>{% endif %}
                    </div>
                </div>'''

# We will replace them with:
new_manuals = '''                <!-- Facilitator Manual -->
                <div class="flex items-center justify-between p-4 bg-white border border-slate-200 rounded-lg hover:border-indigo-300 transition shadow-sm">
                    <div class="flex items-center w-1/3">
                        <i class="fas fa-chalkboard-teacher text-indigo-400 mr-3 text-xl"></i>
                        <span class="font-bold text-slate-700">Facilitator Manual</span>
                    </div>
                    <div class="w-1/3 text-center">
                        <a href="{{ url_for('static', filename='pdf/F_Guide.pdf') }}" target="_blank" class="px-4 py-2 bg-slate-100 hover:bg-slate-200 text-slate-700 text-sm font-bold rounded-md transition flex items-center justify-center mx-auto w-max"><i class="fas fa-eye mr-2"></i> View Manual</a>
                    </div>
                    <div class="w-1/3 flex justify-end">
                        <span class="w-6 h-6 rounded-full border-2 border-slate-300"></span>
                    </div>
                </div>

                <!-- Participant Manual -->
                <div class="flex items-center justify-between p-4 bg-white border border-slate-200 rounded-lg hover:border-indigo-300 transition shadow-sm relative">
                    <div class="flex items-center w-1/3">
                        <i class="fas fa-users text-indigo-400 mr-3 text-xl"></i>
                        <div class="flex flex-col">
                            <span class="font-bold text-slate-700">Participant Workshop Manual</span>
                        </div>
                    </div>
                    <div class="w-1/3 text-center">
                        <a href="{{ url_for('static', filename='pdf/P_Guide.pdf') }}" target="_blank" class="px-4 py-2 bg-slate-100 hover:bg-slate-200 text-slate-700 text-sm font-bold rounded-md transition flex items-center justify-center mx-auto w-max"><i class="fas fa-eye mr-2"></i> View Manual</a>
                    </div>
                    <div class="w-1/3 flex justify-end">
                        <span class="w-6 h-6 rounded-full border-2 border-slate-300"></span>
                    </div>
                </div>'''

text = text.replace(app_doc + '\n', '')
text = text.replace(annexures + '\n', '')
text = text.replace(app_doc, '')
text = text.replace(annexures, '')

# Add the new_manuals before Linear Presentation
ppp_target = '<!-- PPP -->'
text = text.replace(ppp_target, new_manuals + '\n\n                ' + ppp_target)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)

