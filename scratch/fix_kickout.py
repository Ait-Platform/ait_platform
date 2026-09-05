import re

with open('templates/program_sace/interactive_workshop.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix the kick-out logic
old_js = """                  if(data.status !== sessionState) {
                      sessionState = data.status;
                      if(sessionState === 'lobby') { hasJoinedLocally = false; sessionStorage.removeItem('sace_joined'); }
                      changed = true;
                  }"""

new_js = """                  if(data.status !== sessionState) {
                      // Only kick them out if the room was explicitly reset FROM active BACK TO lobby
                      if(sessionState === 'active' && data.status === 'lobby') { 
                          hasJoinedLocally = false; 
                          sessionStorage.removeItem('sace_joined'); 
                      }
                      sessionState = data.status;
                      changed = true;
                  }"""

if old_js in content:
    content = content.replace(old_js, new_js)
    with open('templates/program_sace/interactive_workshop.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Fixed Participant App JS")
else:
    print("Could not find the target JS block.")
