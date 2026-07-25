with open('templates/program_culturefire/watch_show.html', 'r') as f:
    content = f.read()

btn_html = '''
      <button id="btnFlagVideo" onclick="flagVideo()" class="bg-red-600 hover:bg-red-700 text-white px-4 py-2 rounded transition font-semibold flex items-center gap-2 text-sm shadow">
        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 21v-4m0 0V5a2 2 0 012-2h6.5l1 1H21l-3 6 3 6h-8.5l-1-1H5a2 2 0 00-2 2zm9-13.5V9"></path></svg>
        Flag Video
      </button>
'''

content = content.replace(
    '''<button id="btnStart" onclick="startShow()" class="bg-green-600 hover:bg-green-700 text-white px-6 py-2 rounded transition font-bold shadow">Start / Resume</button>''',
    f'''<button id="btnStart" onclick="startShow()" class="bg-green-600 hover:bg-green-700 text-white px-6 py-2 rounded transition font-bold shadow">Start / Resume</button>{btn_html}'''
)

js_html = '''
  function flagVideo() {
    if(currentIndex < 0 || currentIndex >= submissions.length) return;
    const currentVid = submissions[currentIndex];
    
    if(!confirm("Are you sure you want to flag this video for inappropriate content?")) return;
    
    fetch('/flag_video/' + currentVid.id, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-CSRFToken': '{{ csrf_token() }}'
      }
    })
    .then(res => res.json())
    .then(data => {
      alert(data.message);
      if(data.success) {
        // Optionally skip to next video
        nextItem();
      }
    })
    .catch(err => console.error(err));
  }
'''

content = content.replace('function startShow() {', f'{js_html}\n\nfunction startShow() {{')

with open('templates/program_culturefire/watch_show.html', 'w') as f:
    f.write(content)
print("Done")
