import re

with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Add message listener to the bottom of the script
script_end = """    // Initialize
    updateView();
    
</script>"""

new_listener = """    // Listen for incoming participant answers (Simulator Mode)
    window.addEventListener('message', function(event) {
        if (event.data && event.data.action === 'participantAnswer') {
            // Animate progress bars on the current slide to simulate live survey responses
            const activeSlide = document.getElementById('slide-' + currentSlide);
            if (activeSlide) {
                const bars = activeSlide.querySelectorAll('.bg-slate-700 > div');
                if (bars.length > 0) {
                    // Pick a random bar to increase
                    const randomBar = bars[Math.floor(Math.random() * bars.length)];
                    let currentWidth = parseInt(randomBar.style.width) || 0;
                    currentWidth += Math.floor(Math.random() * 15) + 5; // jump 5-20%
                    if (currentWidth > 100) currentWidth = 100;
                    randomBar.style.width = currentWidth + '%';
                }
            }
        }
    });

    // Initialize
    updateView();
    
</script>"""

text = text.replace(script_end, new_listener)

with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(text)
