import re

file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

old_game_text = '''<p class="font-bold text-indigo-800 mb-4 text-center">The "Tomato" Challenge</p>
                <div class="text-sm text-slate-700 space-y-4 text-left bg-white p-4 rounded-lg shadow-sm border border-indigo-100">
                    <p><strong class="text-indigo-800">1. Left Hand:</strong> Form the syllable <strong>"to"</strong> (4th finger to the middle of the palm).</p>
                    <p><strong class="text-indigo-800">2. Right Hand:</strong> Form the syllable <strong>"ma"</strong> (Thumb to the middle of the palm).</p>
                    <p><strong class="text-indigo-800">3. Team Up:</strong> Because "tomato" has 3 syllables (to-ma-to), you need a partner to provide the third hand!</p>
                    <div class="bg-yellow-50 border-l-4 border-yellow-400 p-3 mt-4 text-yellow-800 italic">
                        This kinesthetic game encourages spelling and memorizes the blending song using the palm as the meeting place.
                    </div>
                </div>'''

new_game_text = '''<p class="font-bold text-indigo-800 mb-4 text-center">"Who is leaving home?"</p>
                <div class="text-sm text-slate-700 space-y-4 text-left bg-white p-4 rounded-lg shadow-sm border border-indigo-100">
                    <p><strong class="text-indigo-800">1. The Prompt:</strong> The teacher asks, <em>"Who is leaving home?"</em></p>
                    <p><strong class="text-indigo-800">2. Assign Consonants:</strong> Participant 1 takes 'P', Participant 2 takes 'T', Participant 3 takes 'T'.</p>
                    <p><strong class="text-indigo-800">3. The Meeting Place:</strong> Each participant moves their assigned consonant finger to the middle of their palm to blend with the vowels.</p>
                    <p><strong class="text-indigo-800">4. Team Up:</strong> Together, the three hands combine to spell <strong>po - ta - to</strong>!</p>
                    <div class="bg-yellow-50 border-l-4 border-yellow-400 p-3 mt-4 text-yellow-800 italic">
                        This kinesthetic game personifies the letters leaving home, encouraging spelling and collaboration using the palm as the meeting place!
                    </div>
                </div>'''

text = text.replace(old_game_text, new_game_text)

# Also update the confirmation checkbox text
text = text.replace('blended "to-ma-to" using', 'blended "po-ta-to" and "to-ma-to" using')

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
