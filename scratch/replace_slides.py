import re

with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = """
                    <!-- Slide 8 (The English Family) -->
                    <div id="slide-8" class="slide-container absolute inset-0 hidden flex-col items-center justify-center p-8 bg-slate-900 text-white rounded-xl">
                        <h2 class="text-3xl font-bold text-indigo-300 mb-6 uppercase tracking-wider">Slide 8: The English Family</h2>
                        <p class="text-lg text-slate-300 max-w-2xl text-center mb-8">Once upon a time, there was a family with a very unique way of speaking...</p>
                        <div class="bg-indigo-900/50 p-6 rounded-lg max-w-xl text-center border border-indigo-700">
                            <h3 class="font-bold text-yellow-300 mb-2"><i class="fas fa-mobile-alt mr-2"></i>App Verification Active</h3>
                            <p class="text-sm">Participants are currently submitting their demographic adaptation strategies.</p>
                        </div>
                    </div>

                    <!-- Slide 9 (Introducing the Vowels) -->
                    <div id="slide-9" class="slide-container absolute inset-0 hidden flex-col items-center justify-center p-8 bg-slate-900 text-white rounded-xl">
                        <h2 class="text-3xl font-bold text-indigo-300 mb-6 uppercase tracking-wider">Slide 9: Introducing the Vowels</h2>
                        <p class="text-xl text-white font-black max-w-2xl text-center tracking-widest space-x-4">
                            <span>A</span> <span>E</span> <span>I</span> <span>O</span> <span>U</span>
                        </p>
                        <p class="text-sm text-slate-400 mt-8">(Participants are directed to look at the projector)</p>
                    </div>

                    <!-- Slide 10 (The LiTRE Blending Machine) -->
                    <div id="slide-10" class="slide-container absolute inset-0 hidden flex-col items-center justify-center p-8 bg-indigo-900 text-white rounded-xl border-4 border-indigo-500">
                        <h2 class="text-3xl font-bold text-white mb-2 uppercase tracking-wider">Slide 10: The LiTRE Blending Machine</h2>
                        <h3 class="text-xl text-indigo-200 mb-6 italic">The Palm as the Meeting Place</h3>
                        <div class="bg-indigo-800/80 p-6 rounded-lg max-w-xl text-center border border-indigo-600 shadow-xl">
                            <h3 class="font-bold text-yellow-300 mb-2"><i class="fas fa-mobile-alt mr-2"></i>App Verification Active</h3>
                            <p class="text-sm text-indigo-100 mb-2">Participants are conducting a Peer Pronunciation Check.</p>
                            <p class="text-xs text-indigo-300 italic">They must pair up and grade each other's vowel sounds.</p>
                        </div>
                    </div>

                    <!-- Slide 11 (Practice) -->
                    <div id="slide-11" class="slide-container absolute inset-0 hidden flex-col items-center justify-center p-8 bg-slate-900 text-white rounded-xl">
                        <h2 class="text-3xl font-bold text-indigo-300 mb-6 uppercase tracking-wider">Slide 11: Practice Round</h2>
                        <p class="text-lg text-slate-300 max-w-2xl text-center">Let's practice the palm method together.</p>
                    </div>
"""

pattern = re.compile(r"<!-- Slide 8 \(Case Study\) -->.*?<!-- Slide 12 \(Who is Leaving Home\?\) -->", re.DOTALL)
content = pattern.sub(replacement + "\n                    <!-- Slide 12 (Who is Leaving Home?) -->", content)

with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
