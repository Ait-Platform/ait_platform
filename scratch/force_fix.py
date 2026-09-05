import re

with open('templates/program_sace/interactive_workshop.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Properly delete app-view-lobby entirely.
lobby_pattern = re.compile(r'<div id="app-view-lobby".*?</div>\s*<!-- WAITING VIEW', re.DOTALL)
content = lobby_pattern.sub('<!-- WAITING VIEW', content)

# 2. Add hidden to app-view-0 so it doesn't flash.
content = content.replace('<div id="app-view-0" class="app-view">', '<div id="app-view-0" class="app-view hidden">')
content = content.replace('<div id="app-view-waiting" class="app-view hidden text-center">', '<div id="app-view-waiting" class="app-view hidden text-center">')

# 3. Update JS to handle sessionState properly.
# If they come to interactive without joining, send to join.
# BUT wait for the first fetch!
js_fix = """
    let sessionState = 'unknown'; // don't default to lobby or it redirects instantly
    
    function updateView() {
        if (!hasJoinedLocally) {
            window.location.href = '/sace/participant/join';
            return;
        }

        if (sessionState === 'lobby') {
            document.querySelectorAll('.app-view').forEach(el => el.classList.add('hidden'));
            document.getElementById('app-view-waiting').classList.remove('hidden');
            return;
        }
        
        if (sessionState === 'unknown') return; // wait for fetch

        document.querySelectorAll('.app-view').forEach(el => el.classList.add('hidden'));

        // Active State logic
"""

content = re.sub(r'let sessionState = \'lobby\';', 'let sessionState = \'unknown\';', content)

# Rewrite updateView
content = re.sub(r'function updateView\(\) \{.*?// Active State logic', js_fix.strip(), content, flags=re.DOTALL)

with open('templates/program_sace/interactive_workshop.html', 'w', encoding='utf-8') as f:
    f.write(content)
