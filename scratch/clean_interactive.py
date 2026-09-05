import re

with open('templates/program_sace/interactive_workshop.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Remove app-view-lobby completely
pattern_lobby = re.compile(r'<!-- LOBBY VIEW \(0\) -->.*?</div>\s*<!-- WAITING VIEW', re.DOTALL)
content = pattern_lobby.sub('<!-- WAITING VIEW', content)

# 2. Update Javascript logic
js_replacement = """
    function updateView() {
        if (!hasJoinedLocally || sessionState === 'lobby') {
            window.location.href = '/sace/participant/join';
            return;
        }

        document.querySelectorAll('.app-view').forEach(el => el.classList.add('hidden'));

        // Active State logic
        let appViewIndex = 1; // Default to Look at projector
"""

content = re.sub(r'function updateView\(\) \{.*?// Active State logic\s*let appViewIndex = 1; // Default to Look at projector', js_replacement.strip(), content, flags=re.DOTALL)

# 3. Remove joinRoom function from JS since it's in the other page
content = re.sub(r'function joinRoom\(\) \{.*?\n    \}', '', content, flags=re.DOTALL)

with open('templates/program_sace/interactive_workshop.html', 'w', encoding='utf-8') as f:
    f.write(content)
