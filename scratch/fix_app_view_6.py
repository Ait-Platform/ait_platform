import re

with open('templates/program_sace/interactive_workshop.html', 'r', encoding='utf-8') as f:
    content = f.read()

bad_chunk_pattern = re.compile(r'<!-- App View 6: Number Map \(Slide 10\) -->.*?<!-- App View 9: Consonant Flashcards \(Slide 11\) -->', re.DOTALL)

clean_app_view_6 = '''<!-- App View 6: Number Map (Slide 10) -->
                    <div id="app-view-6" class="app-view hidden">
                        <h3 class="text-xl font-bold text-indigo-900 mb-4 border-b-2 border-indigo-100 pb-2">Game 2: The Number Map</h3>
                        <div class="bg-indigo-50 border border-indigo-200 p-4 rounded-lg shadow-inner">
                            <p class="font-bold text-indigo-800 mb-2"><i class="fas fa-user-friends mr-2"></i>Pair Up!</p>
                            <p class="text-sm text-slate-700 mb-4">Partner A calls out a random number. Partner B must instantly shout the corresponding vowel. <strong>Then switch!</strong></p>
                            <div class="grid grid-cols-5 gap-2 mb-6 text-center">
                                <div class="bg-white py-3 rounded shadow-sm border border-indigo-100"><span class="block text-xs text-slate-400 font-bold mb-1">1</span><span class="font-black text-xl text-indigo-900">A</span></div>
                                <div class="bg-white py-3 rounded shadow-sm border border-indigo-100"><span class="block text-xs text-slate-400 font-bold mb-1">2</span><span class="font-black text-xl text-indigo-900">E</span></div>
                                <div class="bg-white py-3 rounded shadow-sm border border-indigo-100"><span class="block text-xs text-slate-400 font-bold mb-1">3</span><span class="font-black text-xl text-indigo-900">I</span></div>
                                <div class="bg-white py-3 rounded shadow-sm border border-indigo-100"><span class="block text-xs text-slate-400 font-bold mb-1">4</span><span class="font-black text-xl text-indigo-900">O</span></div>
                                <div class="bg-white py-3 rounded shadow-sm border border-indigo-100"><span class="block text-xs text-slate-400 font-bold mb-1">5</span><span class="font-black text-xl text-indigo-900">U</span></div>
                            </div>
                            <label class="flex items-start space-x-3 mb-4 p-3 bg-white rounded border border-indigo-100 cursor-pointer">
                                <input type="checkbox" id="check-game2" class="mt-1 h-5 w-5 text-green-600 rounded">
                                <span class="text-sm text-slate-700 font-medium">I confirm my partner successfully completed the Number Map game.</span>
                            </label>
                            <button class="w-full py-2.5 bg-green-600 text-white font-bold rounded-lg shadow hover:bg-green-700 transition" onclick="if(document.getElementById('check-game2').checked) { alert('Peer Drill Logged!'); } else { alert('Please confirm first!'); }">Submit Verification</button>
                        </div>
                    </div>
                    
                    <!-- App View 9: Consonant Flashcards (Slide 11) -->'''

content = bad_chunk_pattern.sub(clean_app_view_6, content)

with open('templates/program_sace/interactive_workshop.html', 'w', encoding='utf-8') as f:
    f.write(content)
