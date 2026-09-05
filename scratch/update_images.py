import re

with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = """
                    <!-- Slide 8 (The English Family) -->
                    <div id="slide-8" class="slide-container absolute inset-0 hidden flex-col items-center justify-center bg-black rounded-xl overflow-hidden relative">
                        <img src="{{ url_for('static', filename='sace_slides/8Family.png') }}" class="max-h-full max-w-full object-contain" alt="The English Family">
                        <div class="absolute bottom-6 bg-indigo-900/90 p-4 rounded-lg text-center border border-indigo-500 shadow-2xl backdrop-blur-sm z-10 text-white">
                            <h3 class="font-bold text-yellow-300 text-sm mb-1"><i class="fas fa-mobile-alt mr-2"></i>App Verification Active</h3>
                            <p class="text-xs">Participants submitting demographic adaptation strategy.</p>
                        </div>
                    </div>

                    <!-- Slide 9 (Introducing the Vowels) -->
                    <div id="slide-9" class="slide-container absolute inset-0 hidden flex-col items-center justify-center bg-black rounded-xl overflow-hidden relative">
                        <img src="{{ url_for('static', filename='sace_slides/9Vowels.png') }}" class="max-h-full max-w-full object-contain" alt="Vowels">
                        <div class="absolute bottom-6 bg-rose-900/90 p-4 rounded-lg text-center border border-rose-500 shadow-2xl backdrop-blur-sm z-10 text-white">
                            <h3 class="font-bold text-yellow-300 text-sm mb-1"><i class="fas fa-running mr-2"></i>Kinesthetic Game 1</h3>
                            <p class="text-xs">Vowel Hops. Participants are standing up and logging via app.</p>
                        </div>
                    </div>

                    <!-- Slide 10 (The LiTRE Blending Machine) -->
                    <div id="slide-10" class="slide-container absolute inset-0 hidden flex-col items-center justify-center bg-black rounded-xl overflow-hidden relative">
                        <img src="{{ url_for('static', filename='sace_slides/10BlendingMachine.png') }}" class="max-h-full max-w-full object-contain" alt="Blending Machine">
                        <div class="absolute bottom-6 bg-indigo-900/90 p-4 rounded-lg text-center border border-indigo-500 shadow-2xl backdrop-blur-sm z-10 text-white">
                            <h3 class="font-bold text-yellow-300 text-sm mb-1"><i class="fas fa-user-friends mr-2"></i>Kinesthetic Game 2</h3>
                            <p class="text-xs">Number Map Peer Assessment active in app.</p>
                        </div>
                    </div>

                    <!-- Slide 11 (Consonant Flashcards / ta) -->
                    <div id="slide-11" class="slide-container absolute inset-0 hidden flex-col items-center justify-center bg-black rounded-xl overflow-hidden relative">
                        <img src="{{ url_for('static', filename='sace_slides/11ta.png') }}" class="max-h-full max-w-full object-contain" alt="ta">
                        <div class="absolute bottom-6 bg-emerald-900/90 p-4 rounded-lg text-center border border-emerald-500 shadow-2xl backdrop-blur-sm z-10 text-white">
                            <h3 class="font-bold text-yellow-300 text-sm mb-1"><i class="fas fa-cut mr-2"></i>Tactile Engagement</h3>
                            <p class="text-xs">Consonant Flashcards building (10 Mins). PDF downloaded via app.</p>
                        </div>
                    </div>

                    <!-- Slide 12 (ma) -->
                    <div id="slide-12" class="slide-container absolute inset-0 hidden flex-col items-center justify-center bg-black rounded-xl overflow-hidden relative">
                        <img src="{{ url_for('static', filename='sace_slides/12ma.png') }}" class="max-h-full max-w-full object-contain" alt="ma">
                    </div>
                    
                    <!-- Slide 13 (tomato) -->
                    <div id="slide-13" class="slide-container absolute inset-0 hidden flex-col items-center justify-center bg-black rounded-xl overflow-hidden relative">
                        <img src="{{ url_for('static', filename='sace_slides/13atomato.png') }}" class="max-h-full max-w-full object-contain" alt="tomato">
                    </div>
"""

pattern = re.compile(r'<!-- Slide 8 \(The English Family\) -->.*?<!-- Slide 12 \(Who is Leaving Home\?\) -->\s*<div id="slide-12".*?</div>', re.DOTALL)
content = pattern.sub(replacement.strip(), content)

# Update slide counter max
content = content.replace('<span id="slide-counter" class="font-bold text-slate-600">0 / 4</span>', '<span id="slide-counter" class="font-bold text-slate-600">0 / 13</span>')

with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
