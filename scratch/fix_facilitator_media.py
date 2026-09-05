import re

with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Fix black background -> white background for the projector/slide area
content = content.replace('bg-slate-900 border-[8px] border-slate-800', 'bg-white border-[8px] border-slate-300')
content = content.replace('bg-slate-900 text-white rounded-xl', 'bg-white text-slate-800 rounded-xl') # Lobby slide
content = content.replace('bg-slate-800 rounded-xl p-6 border border-slate-700 space-y-6 flex-grow text-white', 'bg-white rounded-xl p-6 border border-slate-200 space-y-6 flex-grow text-slate-800 shadow-sm')

# 2. Make Audio visible and manual
content = content.replace('<audio class="hidden">', '<audio controls class="mt-4 mb-2 w-3/4 max-w-md shadow-sm rounded-full">')
# some might just be <audio> if they were modified
content = content.replace('<audio>', '<audio controls class="mt-4 mb-2 w-3/4 max-w-md shadow-sm rounded-full">')

# 3. Remove Javascript auto-play logic
# We need to remove the play logic but KEEP the pause logic (so when you change slide, previous audio stops playing)
content = re.sub(r'// Play audio 1 second after slide reveals.*?if \(audioEl\) \{.*?\}.*?\}', '', content, flags=re.DOTALL)

with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated Facilitator Dashboard visual and audio")
