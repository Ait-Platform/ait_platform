import re

file_path = 'templates/program_sace/reading_hub.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

old_p_manual = '''<div class="flex flex-col">
                            <span class="font-bold text-slate-700">Participant Workshop Manual</span>
                        </div>'''

new_p_manual = '''<div class="flex flex-col">
                            <span class="font-bold text-slate-700">Participant Workshop Manual</span>
                            <span class="text-[10px] text-slate-500 mt-1 leading-tight w-48">Note: All Assessments and Videos are the exclusive Intellectual Property of AIT and are hosted natively on this platform.</span>
                        </div>'''

text = text.replace(old_p_manual, new_p_manual)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)

