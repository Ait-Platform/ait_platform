import re

# 1. Update interactive_workshop.html
with open('templates/program_sace/interactive_workshop.html', 'r', encoding='utf-8') as f:
    content = f.read()

app_view_6_replacement = """
                    <!-- App View 6: Number Map (Slide 10) -->
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
"""
content = re.sub(r'<!-- App View 6: Number Map \(Slide 10\) -->.*?</div>\s*</div>', app_view_6_replacement.strip(), content, flags=re.DOTALL)

app_view_9 = """
                    <!-- App View 9: Consonant Flashcards (Slide 11) -->
                    <div id="app-view-9" class="app-view hidden">
                        <h3 class="text-xl font-bold text-indigo-900 mb-4 border-b-2 border-indigo-100 pb-2">Tactile Engagement</h3>
                        <div class="bg-indigo-50 border border-indigo-200 p-6 rounded-lg text-center shadow-inner">
                            <i class="fas fa-cut text-4xl text-indigo-400 mb-4"></i>
                            <p class="font-bold text-indigo-800 mb-2">Consonant Flashcards (10 Mins)</p>
                            <p class="text-sm text-slate-700 mb-6">Download the Consonant Sheet below. Cut out the letters to build your physical deck, and test your partner.</p>
                            
                            <a href="#" onclick="alert('Downloading PDF...')" class="inline-block px-4 py-2 bg-white border border-indigo-300 text-indigo-700 font-bold rounded shadow-sm hover:bg-indigo-50 mb-6 transition">
                                <i class="fas fa-file-pdf mr-2 text-red-500"></i> Consonant Sheet PDF
                            </a>

                            <label class="flex items-start space-x-3 mb-4 p-3 bg-white rounded border border-indigo-100 text-left cursor-pointer">
                                <input type="checkbox" id="check-game3" class="mt-1 h-5 w-5 text-indigo-600 rounded">
                                <span class="text-sm text-slate-700 font-medium">I confirm we have created our physical flashcards and tested each other.</span>
                            </label>
                            
                            <button class="w-full py-2.5 bg-indigo-600 text-white font-bold rounded-lg shadow hover:bg-indigo-700 transition" onclick="if(document.getElementById('check-game3').checked) { alert('Flashcard Activity Logged!'); } else { alert('Please check the confirmation box!'); }">Submit Verification</button>
                        </div>
                    </div>
"""
content = content.replace('<!-- App View 7: Nano Blending -->', app_view_9 + '\n                    <!-- App View 7: Nano Blending -->')

# Update JS mapping for Slide 11
if 'if (currentSlide === 10) appViewIndex = 6;' in content:
    content = content.replace('if (currentSlide === 10) appViewIndex = 6; // Slide 10 (Blending Machine) -> Vowel Checklist', 
                              'if (currentSlide === 10) appViewIndex = 6; // Slide 10 (Blending Machine) -> Vowel Checklist\n        if (currentSlide === 11) appViewIndex = 9; // Slide 11 -> Consonants')
else:
    # fallback if previous replace failed slightly
    pass # we'll check it

with open('templates/program_sace/interactive_workshop.html', 'w', encoding='utf-8') as f:
    f.write(content)

# 2. Update facilitator_dashboard.html
with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    content2 = f.read()

slide_11_repl = """
                    <!-- Slide 11 (Consonant Flashcards) -->
                    <div id="slide-11" class="slide-container absolute inset-0 hidden flex-col items-center justify-center p-8 bg-slate-900 text-white rounded-xl border-4 border-emerald-500">
                        <h2 class="text-3xl font-bold text-emerald-400 mb-2 uppercase tracking-wider">Slide 11: Tactile Engagement</h2>
                        <h3 class="text-xl text-slate-300 mb-6 italic">Consonant Flashcards (10 Minutes)</h3>
                        <div class="bg-slate-800/80 p-6 rounded-lg max-w-xl text-center border border-slate-700 shadow-xl">
                            <h3 class="font-bold text-yellow-300 mb-2"><i class="fas fa-mobile-alt mr-2"></i>App Integration Active</h3>
                            <p class="text-sm text-slate-200">Participants are downloading the Consonant Sheet PDF on their devices.<br><br>Instruct them to cut out the letters to create physical flashcards and test their partner!</p>
                        </div>
                    </div>
"""
content2 = re.sub(r'<!-- Slide 11 \(Practice\)?.*?</div>\s*</div>', slide_11_repl.strip(), content2, flags=re.DOTALL)
# also try the earlier naming just in case
content2 = re.sub(r'<!-- Slide 11 \(Practice Round\)?.*?</div>\s*</div>', slide_11_repl.strip(), content2, flags=re.DOTALL)

with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content2)

# 3. Update annexure_b.html
with open('templates/program_sace/compliance/annexure_b.html', 'r', encoding='utf-8') as f:
    content3 = f.read()

content3 = re.sub(r'<li><strong>Slide 11 \(Practice Round\):.*?method\.</li>', 
                  '<li><strong>Slide 11 (Consonant Flashcards):</strong> Tactile engagement activity. App Verification: The app provides a downloadable PDF Consonant Sheet. Teachers spend 10 minutes cutting out physical flashcards and testing their partners, confirming completion via the app.</li>', 
                  content3)

with open('templates/program_sace/compliance/annexure_b.html', 'w', encoding='utf-8') as f:
    f.write(content3)
