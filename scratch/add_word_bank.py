import re

file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

# We need to add the Word Bank into app-view-12
word_bank_html = '''
                    <div class="mt-4 pt-4 border-t border-indigo-100">
                        <p class="font-bold text-indigo-900 mb-2"><i class="fas fa-book-open mr-2 text-indigo-500"></i>LITRE Word Bank (Open Syllables)</p>
                        <p class="text-xs text-slate-500 mb-3 italic">Research supports this kinesthetic method for both English and Indigenous languages.</p>
                        
                        <div class="grid grid-cols-2 gap-4">
                            <div class="bg-slate-50 p-3 rounded border border-slate-200">
                                <span class="text-xs font-bold text-slate-400 uppercase tracking-wider block mb-1">English</span>
                                <ul class="list-disc list-inside text-indigo-800 font-medium">
                                    <li>ba - by</li>
                                    <li>he - ro</li>
                                    <li>po - ta - to</li>
                                    <li>ba - na - na</li>
                                </ul>
                            </div>
                            <div class="bg-slate-50 p-3 rounded border border-slate-200">
                                <span class="text-xs font-bold text-slate-400 uppercase tracking-wider block mb-1">Indigenous (Nguni)</span>
                                <ul class="list-disc list-inside text-emerald-700 font-medium">
                                    <li>bo - na (see)</li>
                                    <li>ba - la (count)</li>
                                    <li>i - ka - ti (cat)</li>
                                    <li>u - mu - ntu (person)</li>
                                </ul>
                            </div>
                        </div>
                    </div>
'''

# Find where to inject it (right before the yellow italic box)
search_str = '''<div class="bg-yellow-50 border-l-4 border-yellow-400 p-3 mt-4 text-yellow-800 italic">'''
replacement = word_bank_html + '\n                    ' + search_str

text = text.replace(search_str, replacement)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
