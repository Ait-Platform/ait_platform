import re

# 1. Update Exit Links
for file in ['templates/program_sace/simulator.html', 'templates/program_sace/presentation_ppp.html']:
    with open(file, 'r', encoding='utf-8') as f:
        text = f.read()
    text = text.replace("url_for('sace_bp.dashboard')", "url_for('sace_bp.reading_index')")
    with open(file, 'w', encoding='utf-8') as f:
        f.write(text)

# 2. Fix Simulator Sizing
with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Make the outer container scale better
text = text.replace('h-[85vh]', 'h-[90vh] min-h-[600px]')
# Make the phone mockup smaller
text = text.replace('h-[750px]', 'h-[550px] md:h-[600px]')
# Make F-board slides smaller
text = text.replace('max-h-[65vh]', 'max-h-[55vh]')

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)

# 3. Add Tactile Engagement Overlay to PPP
with open('templates/program_sace/presentation_ppp.html', 'r', encoding='utf-8') as f:
    text = f.read()

tactile_overlay = '''
            <!-- Tactile Engagement Overlay -->
            <div id="tactile-overlay" class="hidden absolute inset-0 bg-slate-900/90 flex flex-col items-center justify-center p-8 z-50 text-white">
                <i class="fas fa-cut text-5xl text-indigo-400 mb-6"></i>
                <h3 class="text-3xl font-bold text-white mb-6">Tactile Engagement: Consonant Flashcards</h3>
                <div class="bg-white/10 p-8 rounded-xl max-w-2xl text-left border border-indigo-500/30">
                    <p class="mb-4 text-lg"><strong class="text-indigo-300">1. Fold:</strong> Take a blank A4 page and fold it in half (long edges touching). Fold it in half again, and then a third time to get exactly 8 equal pieces per page.</p>
                    <p class="mb-4 text-lg"><strong class="text-indigo-300">2. Cut:</strong> Repeat this with 3 to 4 pages, then cut along the folded lines.</p>
                    <p class="text-lg"><strong class="text-indigo-300">3. Write:</strong> Use a marker to write each letter A to Z on a separate card. You have just made your own physical teaching aid to take back to your classroom!</p>
                </div>
                <button onclick="document.getElementById('tactile-overlay').classList.add('hidden')" class="mt-8 px-6 py-3 bg-indigo-600 hover:bg-indigo-500 rounded-lg font-bold transition">Close Exercise View</button>
            </div>
'''
text = text.replace('<!-- Image Container -->', tactile_overlay + '\n            <!-- Image Container -->')

js_inject = '''
        // Setup Audio
        const audioBtn = document.getElementById('btn-audio');
'''
js_replacement = '''
        // Handle Tactile Engagement Overlay (Slide 22 / Index 21)
        if (currentIndex === 21) {
            document.getElementById('tactile-overlay').classList.remove('hidden');
            document.getElementById('tactile-overlay').classList.add('flex');
        } else {
            document.getElementById('tactile-overlay').classList.add('hidden');
            document.getElementById('tactile-overlay').classList.remove('flex');
        }

        // Setup Audio
        const audioBtn = document.getElementById('btn-audio');
'''
text = text.replace(js_inject, js_replacement)

with open('templates/program_sace/presentation_ppp.html', 'w', encoding='utf-8') as f:
    f.write(text)

