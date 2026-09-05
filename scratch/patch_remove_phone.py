import re

# 1. REMOVE TACTILE OVERLAY FROM PPP
with open('templates/program_sace/presentation_ppp.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Remove HTML overlay
overlay_pattern = re.compile(r'<!-- Tactile Engagement Overlay -->.*?</div>\s*<!-- Image Container -->', re.DOTALL)
text = overlay_pattern.sub('<!-- Image Container -->', text)

# Remove JS toggle
js_remove = '''        // Handle Tactile Engagement Overlay (Slide 22 / Index 21)
        if (currentIndex === 21) {
            document.getElementById('tactile-overlay').classList.remove('hidden');
            document.getElementById('tactile-overlay').classList.add('flex');
        } else {
            document.getElementById('tactile-overlay').classList.add('hidden');
            document.getElementById('tactile-overlay').classList.remove('flex');
        }'''
text = text.replace(js_remove, '')

with open('templates/program_sace/presentation_ppp.html', 'w', encoding='utf-8') as f:
    f.write(text)


# 2. REMOVE FAKE MOBILE PHONE FROM SIMULATOR P-BOARD
with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Replace the mobile phone frame with a clean wide container
old_frame = '<div class="w-full max-w-sm bg-white rounded-[2rem] border-[12px] border-slate-800 shadow-2xl overflow-hidden h-[550px] md:h-[600px] flex flex-col relative">'
new_frame = '<div class="w-full max-w-3xl bg-white rounded-xl border border-slate-200 shadow-xl overflow-hidden h-[550px] md:h-[600px] flex flex-col relative">'
text = text.replace(old_frame, new_frame)

# Also remove the "AIT App" header that looked like a mobile phone header, replace with a clean Participant View header
old_header = '''<div class="bg-indigo-600 p-4 text-center text-white font-bold shadow-md z-10 flex justify-between items-center">
                    <span>AIT App</span>
                    <span class="text-xs bg-green-400 text-green-900 px-2 py-1 rounded-full"><i class="fas fa-link mr-1"></i>Synced</span>
                </div>'''
new_header = '''<div class="bg-indigo-600 p-4 text-center text-white font-bold shadow-md z-10 flex justify-between items-center">
                    <span><i class="fas fa-laptop text-indigo-300 mr-2"></i> Participant Device View</span>
                    <span class="text-xs bg-green-400 text-green-900 px-2 py-1 rounded-full"><i class="fas fa-link mr-1"></i>Live Sync Active</span>
                </div>'''
text = text.replace(old_header, new_header)

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
