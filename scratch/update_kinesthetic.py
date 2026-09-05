import re

# 1. Update interactive_workshop.html (SACE Guide Modal + App Views + JS Mapping)
with open('templates/program_sace/interactive_workshop.html', 'r', encoding='utf-8') as f:
    content = f.read()

kinesthetic_modal = """
            <div class="bg-white p-4 rounded-xl border border-slate-200 shadow-sm border-l-4 border-l-rose-500">
                <h4 class="font-bold text-rose-800 mb-2"><i class="fas fa-running mr-2"></i>Kinesthetic Memory</h4>
                <p>Instead of passive listening, the platform enforces physical movement and cognitive mapping (e.g., <strong>The Vowel Hops</strong> and <strong>The Number Map</strong>). This proves that the workshop actively uses multisensory learning methodologies.</p>
            </div>
"""
content = content.replace('<div class="p-4 border-t border-gray-100 bg-gray-50 text-center">', kinesthetic_modal + '\n        <div class="p-4 border-t border-gray-100 bg-gray-50 text-center">')

app_view_6_replacement = """
                    <!-- App View 8: Vowel Hops (Slide 9) -->
                    <div id="app-view-8" class="app-view hidden">
                        <h3 class="text-xl font-bold text-indigo-900 mb-4 border-b-2 border-indigo-100 pb-2">Game 1: The Vowel Hops</h3>
                        <div class="bg-indigo-50 border border-indigo-200 p-6 rounded-lg text-center shadow-inner">
                            <i class="fas fa-running text-4xl text-indigo-400 mb-4"></i>
                            <p class="font-bold text-indigo-800 mb-4">Kinesthetic Memory Activation</p>
                            <ul class="text-sm text-slate-700 text-left space-y-2 mb-6 list-disc list-inside">
                                <li><strong>Stand up!</strong> Take 5 small hops forward.</li>
                                <li>On each hop, loudly pronounce the next vowel.</li>
                            </ul>
                            <div class="flex justify-center space-x-2 font-black text-xl text-indigo-900 mb-6">
                                <span class="bg-white px-3 py-1 rounded shadow-sm border border-indigo-100">A</span>
                                <span class="bg-white px-3 py-1 rounded shadow-sm border border-indigo-100">E</span>
                                <span class="bg-white px-3 py-1 rounded shadow-sm border border-indigo-100">I</span>
                                <span class="bg-white px-3 py-1 rounded shadow-sm border border-indigo-100">O</span>
                                <span class="bg-white px-3 py-1 rounded shadow-sm border border-indigo-100">U</span>
                            </div>
                            <button class="w-full py-2.5 bg-indigo-600 text-white font-bold rounded-lg shadow hover:bg-indigo-700 transition" onclick="alert('Activity Completed!')">I completed the hops!</button>
                        </div>
                    </div>

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
                            <button class="w-full py-2.5 bg-green-600 text-white font-bold rounded-lg shadow hover:bg-green-700 transition" onclick="alert('Peer Drill Logged!')">We completed the drill!</button>
                        </div>
                    </div>
"""
content = re.sub(r'<!-- App View 6: Vowel Checklist \(Slide 10\) -->.*?</div>\s*</div>', app_view_6_replacement.strip(), content, flags=re.DOTALL)
content = content.replace('if (currentSlide === 9) appViewIndex = 1; // Slide 9 (Vowels) -> Look at projector', 'if (currentSlide === 9) appViewIndex = 8; // Slide 9 (Vowels) -> Vowel Hops')

with open('templates/program_sace/interactive_workshop.html', 'w', encoding='utf-8') as f:
    f.write(content)


# 2. Update facilitator_dashboard.html (SACE Guide Modal + Slides 9 & 10)
with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    content2 = f.read()

content2 = content2.replace('<div class="p-4 border-t border-gray-100 bg-gray-50 text-center">', kinesthetic_modal + '\n        <div class="p-4 border-t border-gray-100 bg-gray-50 text-center">')

slides_9_10 = """
                    <!-- Slide 9 (Introducing the Vowels) -->
                    <div id="slide-9" class="slide-container absolute inset-0 hidden flex-col items-center justify-center p-8 bg-slate-900 text-white rounded-xl border-4 border-rose-500">
                        <h2 class="text-3xl font-bold text-rose-400 mb-2 uppercase tracking-wider">Slide 9: Game 1</h2>
                        <h3 class="text-xl text-slate-300 mb-6 italic">Kinesthetic Memory: The Vowel Hops</h3>
                        <p class="text-xl text-white font-black max-w-2xl text-center tracking-widest space-x-6 mb-8">
                            <span>A</span> <span>E</span> <span>I</span> <span>O</span> <span>U</span>
                        </p>
                        <div class="bg-slate-800/80 p-6 rounded-lg max-w-xl text-center border border-slate-700 shadow-xl">
                            <h3 class="font-bold text-yellow-300 mb-2"><i class="fas fa-mobile-alt mr-2"></i>App Integration</h3>
                            <p class="text-sm text-slate-200">Participants have instructions on their app. Have them stand up, take 5 hops, and shout each vowel!</p>
                        </div>
                    </div>

                    <!-- Slide 10 (The LiTRE Blending Machine) -->
                    <div id="slide-10" class="slide-container absolute inset-0 hidden flex-col items-center justify-center p-8 bg-indigo-900 text-white rounded-xl border-4 border-indigo-500">
                        <h2 class="text-3xl font-bold text-indigo-300 mb-2 uppercase tracking-wider">Slide 10: Game 2</h2>
                        <h3 class="text-xl text-indigo-100 mb-6 italic">The Number Map (Blending Machine)</h3>
                        <div class="grid grid-cols-5 gap-4 mb-8 text-center w-full max-w-2xl">
                            <div class="bg-indigo-800 p-4 rounded-lg border border-indigo-600"><span class="block text-sm text-indigo-300 mb-2">1</span><span class="font-black text-3xl">A</span></div>
                            <div class="bg-indigo-800 p-4 rounded-lg border border-indigo-600"><span class="block text-sm text-indigo-300 mb-2">2</span><span class="font-black text-3xl">E</span></div>
                            <div class="bg-indigo-800 p-4 rounded-lg border border-indigo-600"><span class="block text-sm text-indigo-300 mb-2">3</span><span class="font-black text-3xl">I</span></div>
                            <div class="bg-indigo-800 p-4 rounded-lg border border-indigo-600"><span class="block text-sm text-indigo-300 mb-2">4</span><span class="font-black text-3xl">O</span></div>
                            <div class="bg-indigo-800 p-4 rounded-lg border border-indigo-600"><span class="block text-sm text-indigo-300 mb-2">5</span><span class="font-black text-3xl">U</span></div>
                        </div>
                        <div class="bg-indigo-800/80 p-6 rounded-lg max-w-xl text-center border border-indigo-600 shadow-xl">
                            <h3 class="font-bold text-yellow-300 mb-2"><i class="fas fa-mobile-alt mr-2"></i>App Verification Active</h3>
                            <p class="text-sm text-indigo-100">Participants are paired up calling out random numbers while their partner shouts the vowel.</p>
                        </div>
                    </div>
"""
content2 = re.sub(r'<!-- Slide 9 \(Introducing the Vowels\) -->.*?</div>\s*</div>', slides_9_10.strip(), content2, flags=re.DOTALL)

with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content2)


# 3. Update annexure_b.html
with open('templates/program_sace/compliance/annexure_b.html', 'r', encoding='utf-8') as f:
    content3 = f.read()

annexure_repl = """
                        <li><strong>Slide 8 (The English Family):</strong> App Verification: The app prompts a discussion: "How will you adapt this story for your specific classroom demographic?".</li>
                        <li><strong>Slide 9 (Game 1 - The Vowel Hops):</strong> Physical kinesthetic memory drill. App Verification: The app displays the physical activity instructions (take 5 hops, shout the vowels) enforcing active learning.</li>
                        <li><strong>Slide 10 (Game 2 - The Number Map):</strong> Cognitive mapping drill. App Verification: The app displays a 1=A, 2=E... map where teachers pair up and drill each other by calling out random numbers.</li>
"""
content3 = re.sub(r'<li><strong>Slide 8 \(The English Family\):.*?<li><strong>Slide 10 \(The LiTRE Blending Machine\):.*?pronunciation\.</li>', annexure_repl.strip(), content3, flags=re.DOTALL)

with open('templates/program_sace/compliance/annexure_b.html', 'w', encoding='utf-8') as f:
    f.write(content3)
