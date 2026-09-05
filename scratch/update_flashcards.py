import re

with open('templates/program_sace/interactive_workshop.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_html = """                            <p class="text-sm text-slate-700 mb-6">Download the Consonant Sheet below. Cut out the letters to build your physical deck, and test your partner.</p>
                            
                            <a href="#" onclick="alert('Downloading PDF...')" class="inline-block px-4 py-2 bg-white border border-indigo-300 text-indigo-700 font-bold rounded shadow-sm hover:bg-indigo-50 mb-6 transition">
                                <i class="fas fa-file-pdf mr-2 text-red-500"></i> Consonant Sheet PDF
                            </a>

                            <label class="flex items-start space-x-3 mb-4 p-3 bg-white rounded border border-indigo-100 text-left cursor-pointer">
                                <input type="checkbox" id="check-game3" class="mt-1 h-5 w-5 text-indigo-600 rounded">
                                <span class="text-sm text-slate-700 font-medium">I confirm we have created our physical flashcards and tested each other.</span>
                            </label>"""

new_html = """                            <div class="text-sm text-slate-700 mb-6 text-left bg-white p-4 rounded-lg shadow-sm border border-indigo-100">
                                <p class="mb-2"><strong class="text-indigo-800">1. Fold:</strong> Take a blank A4 page and fold it in half (long edges touching). Fold it in half again, and then a third time to get exactly 8 equal pieces per page.</p>
                                <p class="mb-2"><strong class="text-indigo-800">2. Cut:</strong> Repeat this with 3 to 4 pages, then cut along the folded lines.</p>
                                <p><strong class="text-indigo-800">3. Write:</strong> Use a marker to write each letter A to Z on a separate card. You have just made your own physical teaching aid to take back to your classroom!</p>
                            </div>

                            <label class="flex items-start space-x-3 mb-4 p-3 bg-white rounded border border-indigo-100 text-left cursor-pointer hover:bg-indigo-50 transition">
                                <input type="checkbox" id="check-game3" class="mt-1 h-5 w-5 text-indigo-600 rounded">
                                <span class="text-sm text-slate-700 font-medium">I confirm I have folded, cut out, and written my physical A-Z flashcards.</span>
                            </label>"""

if old_html in content:
    content = content.replace(old_html, new_html)
else:
    print("Warning: old_html not found perfectly.")

with open('templates/program_sace/interactive_workshop.html', 'w', encoding='utf-8') as f:
    f.write(content)
