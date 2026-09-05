import re
file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

# The corrupted string
stray_html = ''' Facilitator (F)
              </button>
  <button class="flex items-center px-6 py-3 bg-slate-200 text-slate-600 font-bold rounded-t-lg transition border-b-2 border-slate-300 hover:bg-slate-300" id="btn-tab-p" onclick="showTab('p')">
  <div class="w-3 h-3 rounded-full bg-red-600 shadow-[0_0_8px_rgba(220,38,38,0.5)] mr-3" id="light-p"></div> Participant (P)
              </button>
  </div>'''

if stray_html in text:
    text = text.replace(stray_html, '')
    print("SUCCESS: Removed stray HTML block!")
else:
    print("ERROR: Could not find exact stray HTML string. Trying regex...")
    pattern = r' Facilitator \(F\)\s*</button>\s*<button.*?id="btn-tab-p".*?</button>\s*</div>'
    text = re.sub(pattern, '', text, flags=re.DOTALL)
    print("SUCCESS: Removed stray HTML using regex!")

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
