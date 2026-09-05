import re

# 1. Update Facilitator Dashboard
with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    f_content = f.read()

# Add server sync to startWorkshop
old_start = """    function startWorkshop() {
        sessionState = 'active'; 
        currentSlide = 1; 
        updateView();
    }"""
new_start = """    function startWorkshop() {
        sessionState = 'active'; 
        currentSlide = 1; 
        updateView();
        fetch('/sace/workshop/start', {method: 'POST', headers: {'X-CSRFToken': document.querySelector('meta[name="csrf-token"]').getAttribute('content')}});
    }"""
f_content = f_content.replace(old_start, new_start)

# Add server sync to nextSlide / prevSlide
old_prev = "function prevSlide() { if(currentSlide > 0) { currentSlide--; updateView(); } }"
new_prev = "function prevSlide() { if(currentSlide > 0) { currentSlide--; updateView(); fetch('/sace/workshop/set_slide', {method: 'POST', headers: {'Content-Type': 'application/json', 'X-CSRFToken': document.querySelector('meta[name=\"csrf-token\"]').getAttribute('content')}, body: JSON.stringify({slide: currentSlide})}); } }"
f_content = f_content.replace(old_prev, new_prev)

old_next = "function nextSlide() { if(currentSlide < totalSlides) { currentSlide++; updateView(); } }"
new_next = "function nextSlide() { if(currentSlide < totalSlides) { currentSlide++; updateView(); fetch('/sace/workshop/set_slide', {method: 'POST', headers: {'Content-Type': 'application/json', 'X-CSRFToken': document.querySelector('meta[name=\"csrf-token\"]').getAttribute('content')}, body: JSON.stringify({slide: currentSlide})}); } }"
f_content = f_content.replace(old_next, new_next)

with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(f_content)

# 2. Update Participant App
with open('templates/program_sace/interactive_workshop.html', 'r', encoding='utf-8') as f:
    p_content = f.read()

# Remove the navigation buttons
p_content = re.sub(r'<!-- Self-Paced Navigation Controls -->.*?</div>', '', p_content, flags=re.DOTALL)
p_content = re.sub(r'function changeSlide\(dir\).*?\}', '', p_content, flags=re.DOTALL)

# Add the setInterval sync logic
sync_logic = """
    // Magic Sync: Listen to Facilitator
    setInterval(() => {
        if (evaluatorMode) return;
        fetch('/sace/workshop/get_state')
            .then(res => res.json())
            .then(data => {
                // If facilitator has started, lock to their slide
                if (data.status === 'active' && currentSlide !== data.slide) {
                    currentSlide = data.slide;
                    sessionState = 'active';
                    updateView();
                }
            })
            .catch(e => console.log('Sync error:', e));
    }, 2000);
"""

# Insert sync logic before initialize
p_content = p_content.replace("// Initialize", sync_logic + "\n    // Initialize")

with open('templates/program_sace/interactive_workshop.html', 'w', encoding='utf-8') as f:
    f.write(p_content)

print("Updated both dashboards for Bulletproof Sync")
