import re
import os

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

modal_html = '''
  <!-- Attendees Modal -->
  <div id="attendees-modal" class="fixed inset-0 bg-slate-900 bg-opacity-75 z-[100] hidden flex items-center justify-center p-4">
      <div class="bg-white rounded-xl shadow-2xl max-w-lg w-full max-h-[80vh] flex flex-col">
          <div class="p-6 border-b border-slate-200 flex justify-between items-center bg-indigo-50 rounded-t-xl">
              <h3 class="text-2xl font-bold text-indigo-900"><i class="fas fa-clipboard-list mr-2"></i> Live Digital Register</h3>
              <button onclick="document.getElementById('attendees-modal').classList.add('hidden')" class="text-slate-500 hover:text-red-500 transition text-2xl font-bold">&times;</button>
          </div>
          <div class="p-6 overflow-y-auto flex-grow bg-slate-50">
              <p class="text-sm text-slate-500 mb-4">The following 20 participants have successfully checked in via the Participant App.</p>
              <ul class="space-y-3">
                  <li class="flex items-center p-3 bg-white border border-slate-200 rounded-lg shadow-sm"><div class="h-8 w-8 rounded-full bg-indigo-100 text-indigo-600 flex items-center justify-center font-bold mr-3">SN</div><span class="font-medium text-slate-700">Siyabonga Ndlovu</span></li>
                  <li class="flex items-center p-3 bg-white border border-slate-200 rounded-lg shadow-sm"><div class="h-8 w-8 rounded-full bg-teal-100 text-teal-600 flex items-center justify-center font-bold mr-3">JM</div><span class="font-medium text-slate-700">Johan van der Merwe</span></li>
                  <li class="flex items-center p-3 bg-white border border-slate-200 rounded-lg shadow-sm"><div class="h-8 w-8 rounded-full bg-rose-100 text-rose-600 flex items-center justify-center font-bold mr-3">FP</div><span class="font-medium text-slate-700">Fatima Patel</span></li>
                  <li class="flex items-center p-3 bg-white border border-slate-200 rounded-lg shadow-sm"><div class="h-8 w-8 rounded-full bg-blue-100 text-blue-600 flex items-center justify-center font-bold mr-3">TN</div><span class="font-medium text-slate-700">Thandiwe Nkosi</span></li>
                  <li class="flex items-center p-3 bg-white border border-slate-200 rounded-lg shadow-sm"><div class="h-8 w-8 rounded-full bg-purple-100 text-purple-600 flex items-center justify-center font-bold mr-3">LM</div><span class="font-medium text-slate-700">Lerato Mokoena</span></li>
                  <li class="flex items-center p-3 bg-white border border-slate-200 rounded-lg shadow-sm"><div class="h-8 w-8 rounded-full bg-amber-100 text-amber-600 flex items-center justify-center font-bold mr-3">DN</div><span class="font-medium text-slate-700">David Naidoo</span></li>
                  <li class="flex items-center p-3 bg-white border border-slate-200 rounded-lg shadow-sm"><div class="h-8 w-8 rounded-full bg-indigo-100 text-indigo-600 flex items-center justify-center font-bold mr-3">SS</div><span class="font-medium text-slate-700">Sarah Smith</span></li>
                  <li class="flex items-center p-3 bg-white border border-slate-200 rounded-lg shadow-sm"><div class="h-8 w-8 rounded-full bg-teal-100 text-teal-600 flex items-center justify-center font-bold mr-3">MM</div><span class="font-medium text-slate-700">Mpho Mathata</span></li>
                  <li class="flex items-center p-3 bg-white border border-slate-200 rounded-lg shadow-sm"><div class="h-8 w-8 rounded-full bg-rose-100 text-rose-600 flex items-center justify-center font-bold mr-3">CG</div><span class="font-medium text-slate-700">Chloe Govender</span></li>
                  <li class="flex items-center p-3 bg-white border border-slate-200 rounded-lg shadow-sm"><div class="h-8 w-8 rounded-full bg-blue-100 text-blue-600 flex items-center justify-center font-bold mr-3">PJ</div><span class="font-medium text-slate-700">Pieter Joubert</span></li>
                  <li class="flex items-center p-3 bg-white border border-slate-200 rounded-lg shadow-sm"><div class="h-8 w-8 rounded-full bg-purple-100 text-purple-600 flex items-center justify-center font-bold mr-3">ZN</div><span class="font-medium text-slate-700">Zanele Ndaba</span></li>
                  <li class="flex items-center p-3 bg-white border border-slate-200 rounded-lg shadow-sm"><div class="h-8 w-8 rounded-full bg-amber-100 text-amber-600 flex items-center justify-center font-bold mr-3">AA</div><span class="font-medium text-slate-700">Aisha Adams</span></li>
                  <li class="flex items-center p-3 bg-white border border-slate-200 rounded-lg shadow-sm"><div class="h-8 w-8 rounded-full bg-indigo-100 text-indigo-600 flex items-center justify-center font-bold mr-3">KR</div><span class="font-medium text-slate-700">Kagiso Radebe</span></li>
                  <li class="flex items-center p-3 bg-white border border-slate-200 rounded-lg shadow-sm"><div class="h-8 w-8 rounded-full bg-teal-100 text-teal-600 flex items-center justify-center font-bold mr-3">MB</div><span class="font-medium text-slate-700">Megan Botha</span></li>
                  <li class="flex items-center p-3 bg-white border border-slate-200 rounded-lg shadow-sm"><div class="h-8 w-8 rounded-full bg-rose-100 text-rose-600 flex items-center justify-center font-bold mr-3">SD</div><span class="font-medium text-slate-700">Sipho Dlamini</span></li>
                  <li class="flex items-center p-3 bg-white border border-slate-200 rounded-lg shadow-sm"><div class="h-8 w-8 rounded-full bg-blue-100 text-blue-600 flex items-center justify-center font-bold mr-3">RC</div><span class="font-medium text-slate-700">Ravi Chetty</span></li>
                  <li class="flex items-center p-3 bg-white border border-slate-200 rounded-lg shadow-sm"><div class="h-8 w-8 rounded-full bg-purple-100 text-purple-600 flex items-center justify-center font-bold mr-3">NS</div><span class="font-medium text-slate-700">Nomsa Sithole</span></li>
                  <li class="flex items-center p-3 bg-white border border-slate-200 rounded-lg shadow-sm"><div class="h-8 w-8 rounded-full bg-amber-100 text-amber-600 flex items-center justify-center font-bold mr-3">HS</div><span class="font-medium text-slate-700">Heinrich Steyn</span></li>
                  <li class="flex items-center p-3 bg-white border border-slate-200 rounded-lg shadow-sm"><div class="h-8 w-8 rounded-full bg-indigo-100 text-indigo-600 flex items-center justify-center font-bold mr-3">NM</div><span class="font-medium text-slate-700">Nadia Moodley</span></li>
                  <li class="flex items-center p-3 bg-white border border-slate-200 rounded-lg shadow-sm"><div class="h-8 w-8 rounded-full bg-teal-100 text-teal-600 flex items-center justify-center font-bold mr-3">PM</div><span class="font-medium text-slate-700">Palesa Mokoena</span></li>
              </ul>
          </div>
      </div>
  </div>
'''

text = text.replace('<div class="min-h-screen bg-slate-900 flex flex-col font-sans">', '<div class="min-h-screen bg-slate-900 flex flex-col font-sans">\n' + modal_html)

lobby_btn = '<button onclick="document.getElementById(\'attendees-modal\').classList.remove(\'hidden\')" class="mt-4 px-6 py-2 bg-slate-200 hover:bg-slate-300 text-slate-700 font-bold rounded-lg transition"><i class="fas fa-users mr-2"></i> View Live Register</button>'
text = text.replace('<div class="text-2xl font-bold text-green-400">Participants Connected: <span id="attendance-counter">0</span></div>', '<div class="text-2xl font-bold text-green-400">Participants Connected: <span id="attendance-counter">0</span></div>\n  ' + lobby_btn)

fboard_header_btn = '<button onclick="document.getElementById(\'attendees-modal\').classList.remove(\'hidden\')" class="ml-4 px-4 py-2 bg-indigo-700 hover:bg-indigo-600 text-white font-bold rounded shadow-sm text-sm"><i class="fas fa-users mr-2"></i>Register (20)</button>'
text = text.replace('<button class="px-4 py-2 bg-indigo-700 hover:bg-indigo-600 text-white font-bold rounded shadow-sm flex items-center text-sm" id="global-audio-btn"', fboard_header_btn + '\n          <button class="px-4 py-2 bg-indigo-700 hover:bg-indigo-600 text-white font-bold rounded shadow-sm flex items-center text-sm" id="global-audio-btn"')

slides_html = '''
          <div class="slide-container absolute inset-0 flex flex-col overflow-y-auto items-center justify-center p-8 bg-white text-slate-800 rounded-xl text-center z-20" id="slide-lobby">
              <h2 class="text-4xl font-bold text-indigo-600 mb-4">Welcome</h2>
              <p class="text-xl text-slate-500 mb-8">Waiting for participants to check in...</p>
              <div class="text-2xl font-bold text-green-400">Participants Connected: <span id="attendance-counter">20</span></div>
              <button onclick="document.getElementById('attendees-modal').classList.remove('hidden')" class="mt-4 px-6 py-2 bg-slate-200 hover:bg-slate-300 text-slate-700 font-bold rounded-lg transition"><i class="fas fa-users mr-2"></i> View Live Register</button>
          </div>
          <div class="slide-container absolute inset-0 flex flex-col overflow-y-auto items-center justify-center" id="slide-0">
              <img alt="Slide 1: Program" class="w-full h-full p-2 object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/1Program.png') }}"/>
          </div>
          <div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center" id="slide-1">
              <img alt="Slide 2: Crisis" class="w-full h-full p-2 object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/2Crisis.png') }}"/>
          </div>
          <div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center" id="slide-2">
              <img alt="Slide 3: Survey" class="w-full h-full p-2 object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/3Survey.png') }}"/>
          </div>
          <div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center" id="slide-3">
              <img alt="Slide 4: Study" class="w-full h-full p-2 object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/4Study.png') }}"/>
          </div>
          <div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center" id="slide-4">
              <img alt="Slide 5: The Problem" class="w-full h-full p-2 object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/5The Problem.png') }}"/>
          </div>
          <div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center" id="slide-5">
              <img alt="Slide 6: Root Cause" class="w-full h-full p-2 object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/6Root Cause.png') }}"/>
          </div>
          <div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center" id="slide-6">
              <img alt="Slide 7: Litre" class="w-full h-full p-2 object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/7Litre.png') }}"/>
          </div>
          <div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center" id="slide-7">
              <img alt="Slide 8: Why Litre" class="w-full h-full p-2 object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/8Why Litre.png') }}"/>
          </div>
          <div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center" id="slide-8">
              <img alt="Slide 9: What is Litre" class="w-full h-full p-2 object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/9What is Litre.png') }}"/>
          </div>
          <!-- Keep slide 9 to 15 unchanged, but make sure they don't break JS -->
          <div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center p-8 bg-white text-slate-800 rounded-xl" id="slide-9">
              <h2 class="text-3xl font-bold text-indigo-900 mb-6 uppercase tracking-wider">Slide 10: Placeholder</h2>
          </div>
          <div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center p-8 bg-white text-slate-800 rounded-xl" id="slide-10">
              <h2 class="text-3xl font-bold text-indigo-900 mb-6 uppercase tracking-wider">Slide 11: Placeholder</h2>
          </div>
          <div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center p-8 bg-white text-slate-800 rounded-xl" id="slide-11">
              <h2 class="text-3xl font-bold text-indigo-900 mb-6 uppercase tracking-wider">Slide 12: Placeholder</h2>
          </div>
          <div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center p-8 bg-white text-slate-800 rounded-xl" id="slide-12">
              <h2 class="text-3xl font-bold text-indigo-900 mb-6 uppercase tracking-wider">Slide 13: Placeholder</h2>
          </div>
          <div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center p-8 bg-white text-slate-800 rounded-xl border-4 border-rose-500" id="slide-13">
              <h2 class="text-3xl font-bold text-rose-400 mb-2 uppercase tracking-wider">Slide 14: Game 1</h2>
          </div>
          <div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center p-8 bg-indigo-900 text-white rounded-xl border-4 border-indigo-500" id="slide-14">
              <h2 class="text-3xl font-bold text-indigo-900 mb-2 uppercase tracking-wider">Slide 15: Game 2</h2>
          </div>
          <div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center p-8 bg-white text-slate-800 rounded-xl border-4 border-emerald-500" id="slide-15">
              <h2 class="text-3xl font-bold text-emerald-400 mb-2 uppercase tracking-wider">Slide 16: Assessment</h2>
          </div>
'''

# Use regex to find and replace the slides block
import re

pattern = re.compile(r'<div class="slide-container absolute inset-0 flex flex-col overflow-y-auto items-center justify-center p-8 bg-white.*?</audio>\n          </div>', re.DOTALL)
# Wait, this regex is too fragile. Let's just find slide-lobby and the end of slide-15.
idx1 = text.find('id="slide-lobby"')
if idx1 != -1:
    idx1 = text.rfind('<div class="slide-container', 0, idx1)
    
idx2 = text.find('id="slide-15"')
if idx2 != -1:
    idx2 = text.find('</div>', idx2) + 6

if idx1 != -1 and idx2 != -1:
    text = text[:idx1] + slides_html + text[idx2:]

# Remove triggerRandomDice(currentSlide) from finishActivityAndNext()
text = text.replace('triggerRandomDice(currentSlide);', '')

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
