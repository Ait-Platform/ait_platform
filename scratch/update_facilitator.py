import re

with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Remove fetchState setInterval
content = re.sub(r'setInterval\(fetchState, \d+\);', '', content)

# Remove fetchState definition
content = re.sub(r'function fetchState\(\) \{.*?\n    \}\n', '', content, flags=re.DOTALL)
content = re.sub(r'fetchState\(\);', '', content)

# Fix prevSlide and nextSlide to be local only
content = re.sub(
    r"function prevSlide\(\) \{ if\(currentSlide > 0\) \{ currentSlide--; fetch\('/sace/workshop/set_slide'.*?\); updateView\(\); \}\n    \}",
    "function prevSlide() { if(currentSlide > 0) { currentSlide--; updateView(); } }",
    content
)
content = re.sub(
    r"function nextSlide\(\) \{ if\(currentSlide < totalSlides\) \{ currentSlide\+\+; fetch\('/sace/workshop/set_slide'.*?\); updateView\(\); \}\n    \}",
    "function nextSlide() { if(currentSlide < totalSlides) { currentSlide++; updateView(); } }",
    content
)

# Fix startWorkshop to be local only
old_start = """    function startWorkshop() {
        fetch('/sace/workshop/start', {
            method: 'POST',
            headers: {'X-CSRFToken': document.querySelector('meta[name="csrf-token"]').getAttribute('content')}
        })
        .then(() => { sessionState = 'active'; currentSlide = 1; updateView(); });
    }"""
new_start = """    function startWorkshop() {
        sessionState = 'active'; 
        currentSlide = 1; 
        updateView();
    }"""
content = content.replace(old_start, new_start)

# Fix resetWorkshop to be local only
old_reset = """    function resetWorkshop() {
        if(confirm("End workshop and clear all participant data?")) {
            fetch('/sace/workshop/reset', {
                method: 'POST',
                headers: {'X-CSRFToken': document.querySelector('meta[name="csrf-token"]').getAttribute('content')}
            }).then(() => { sessionState = 'lobby'; currentSlide = 0; updateView(); });
        }
    }"""
new_reset = """    function resetWorkshop() {
        if(confirm("End workshop and clear all participant data?")) {
            sessionState = 'lobby'; 
            currentSlide = 0; 
            updateView();
        }
    }"""
content = content.replace(old_reset, new_reset)

# Set evaluatorMode = true permanently? No, we just removed the backend logic so it's intrinsically local.
with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated facilitator dashboard")
