import re

with open('templates/program_culturefire/watch_show.html', 'r', encoding='utf-8') as f:
    html = f.read()

# 1. Remove the Controls from above the player
html = re.sub(
    r'<!-- Bottom: Controls \(Moved up\) -->.*?</div>\s*<!-- Player -->',
    '<!-- Player -->',
    html,
    flags=re.DOTALL
)

# 2. Add Controls BELOW the player (before closing div for max-w-3xl)
controls_html = '''
        <!-- Controls (Moved to bottom) -->
        <div class="bg-gray-100 border-t border-gray-700 px-6 py-4 flex justify-between items-center shadow-inner mt-4 rounded-b-lg">
          <button id="btnPrev" onclick="prevItem()" class="bg-gray-500 hover:bg-gray-600 text-white px-4 py-2 rounded disabled:opacity-40 transition font-semibold">&larr; Previous</button>
          <button id="btnStart" onclick="startShow()" class="bg-green-600 hover:bg-green-700 text-white px-6 py-2 rounded transition font-bold shadow">Start / Resume</button>
          <button id="btnFlagVideo" onclick="flagVideo()" class="bg-red-600 hover:bg-red-700 text-white px-4 py-2 rounded transition font-semibold flex items-center gap-2 text-sm shadow">
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 21v-4m0 0V5a2 2 0 012-2h6.5l1 1H21l-3 6 3 6h-8.5l-1-1H5a2 2 0 00-2 2zm9-13.5V9"></path></svg>
            Flag Video
          </button>
          <button id="btnNext" onclick="nextItem()" class="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded disabled:opacity-40 transition font-semibold">Next &rarr;</button>
        </div>
      </div>
'''
html = re.sub(
    r'</video>\s*</div>',
    '</video>\n' + controls_html,
    html,
    flags=re.DOTALL
)

# 3. Expand Player Width
html = html.replace('max-w-3xl mx-auto', 'w-full max-w-5xl mx-auto')

# 4. Remove fullscreen hiding CSS
html = html.replace('video::-webkit-media-controls-fullscreen-button { display: none !important; }', '')

with open('templates/program_culturefire/watch_show.html', 'w', encoding='utf-8') as f:
    f.write(html)
print("Done fixing HTML layout")
