import re

file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Add the new app-view for the Sign Language Game
new_app_view = '''
        <div class="app-view hidden" id="app-view-12">
            <h3 class="text-xl font-bold text-indigo-900 mb-4 border-b-2 border-indigo-100 pb-2">LITRE Sign Language Game</h3>
            <div class="bg-indigo-50 border border-indigo-200 p-6 rounded-lg shadow-inner mb-6">
                <i class="fas fa-sign-language text-4xl text-indigo-400 mb-4 flex justify-center"></i>
                <p class="font-bold text-indigo-800 mb-4 text-center">The "Tomato" Challenge</p>
                <div class="text-sm text-slate-700 space-y-4 text-left bg-white p-4 rounded-lg shadow-sm border border-indigo-100">
                    <p><strong class="text-indigo-800">1. Left Hand:</strong> Form the syllable <strong>"to"</strong> (4th finger to the middle of the palm).</p>
                    <p><strong class="text-indigo-800">2. Right Hand:</strong> Form the syllable <strong>"ma"</strong> (Thumb to the middle of the palm).</p>
                    <p><strong class="text-indigo-800">3. Team Up:</strong> Because "tomato" has 3 syllables (to-ma-to), you need a partner to provide the third hand!</p>
                    <div class="bg-yellow-50 border-l-4 border-yellow-400 p-3 mt-4 text-yellow-800 italic">
                        This kinesthetic game encourages spelling and memorizes the blending song using the palm as the meeting place.
                    </div>
                </div>
            </div>
            
            <label class="flex items-start space-x-3 mb-4 p-3 bg-white rounded border border-indigo-100 cursor-pointer hover:bg-indigo-50 transition">
                <input type="checkbox" id="check-game-sign" class="mt-1 h-5 w-5 text-indigo-600 rounded focus:ring-indigo-500">
                <span class="text-sm text-slate-700 font-medium">I confirm my group successfully blended "to-ma-to" using the LITRE sign language.</span>
            </label>
            <button onclick="if(document.getElementById('check-game-sign').checked) { submitLog('Completed Kinesthetic Drill: LITRE Sign Language'); } else { alert('Please check the confirmation box!'); }" class="w-full py-2.5 bg-indigo-600 text-white font-bold rounded-lg shadow hover:bg-indigo-700 transition">
                Submit Verification
            </button>
        </div>
'''

# Insert it before the last app-view (which is app-view-11 usually)
text = text.replace('<div class="app-view hidden overflow-y-auto" id="app-view-11">', new_app_view + '\n        <div class="app-view hidden overflow-y-auto" id="app-view-11">')

# 2. Update the JS mapping
js_mapping_old = '''if (currentSlide === 14) pIndex = 6; // Game 2
        if (currentSlide === 21) pIndex = 9; // Tactile Engagement (Folding)'''

js_mapping_new = '''if (currentSlide === 14) pIndex = 6; // Game 2
        if (currentSlide === 18) pIndex = 12; // LITRE Sign Language Game (tomato)
        if (currentSlide === 21) pIndex = 9; // Tactile Engagement (Folding)'''

text = text.replace(js_mapping_old, js_mapping_new)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
