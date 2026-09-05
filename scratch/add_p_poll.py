import re

with open('templates/program_sace/interactive_workshop.html', 'r', encoding='utf-8') as f:
    text = f.read()

poll_code = """
    // REAL SERVER POLLING (Replaces Magic Sync)
    setInterval(() => {
        if (!hasJoinedLocally) return;
        
        const csrfToken = document.querySelector('meta[name="csrf-token"]').getAttribute('content');
        fetch('/sace/workshop/get_state', {
            headers: {'X-CSRFToken': csrfToken}
        })
        .then(res => res.json())
        .then(data => {
            if (data.status === 'success') {
                if (data.state && sessionState !== data.state) {
                    sessionState = data.state;
                }
                if (data.slide !== undefined && currentSlide !== data.slide) {
                    currentSlide = data.slide;
                    updateView();
                }
            }
        });
    }, 2000);
"""

text = text.replace('// Simulator Sync: Listen to Parent Window instead of polling Server', poll_code + '\n    // Simulator Sync: Listen to Parent Window instead of polling Server')

with open('templates/program_sace/interactive_workshop.html', 'w', encoding='utf-8') as f:
    f.write(text)
