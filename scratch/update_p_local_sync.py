import re

with open('templates/program_sace/interactive_workshop.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Replace the Magic Sync setInterval block to check if NOT embedded before polling
old_sync = r"""    // Magic Sync: Listen to Facilitator
    setInterval\(\(\) => \{
        if \(evaluatorMode\) return;
        fetch\('/sace/workshop/get_state'\)
            \.then\(res => res\.json\(\)\)
            \.then\(data => \{
                // If facilitator has started, lock to their slide
                if \(data\.status === 'active' && currentSlide !== data\.slide\) \{
                    currentSlide = data\.slide;
                    sessionState = 'active';
                    updateView\(\);
                \}
            \}\)
            \.catch\(e => console\.log\('Sync error:', e\)\);
    \}, 2000\);"""

# The query parameter embed is passed from Flask in request.args.get('embed')
# Let's use a JS variable at the top
new_sync = """    const isEmbedded = new URLSearchParams(window.location.search).get('embed') === '1';

    // Magic Sync: Listen to Facilitator (ONLY if in Live Room, NOT Simulator)
    if (!isEmbedded) {
        setInterval(() => {
            if (evaluatorMode) return;
            fetch('/sace/workshop/get_state')
                .then(res => res.json())
                .then(data => {
                    if (data.status === 'active' && currentSlide !== data.slide) {
                        currentSlide = data.slide;
                        sessionState = 'active';
                        updateView();
                    }
                })
                .catch(e => console.log('Sync error:', e));
        }, 2000);
    }
    
    // Simulator Sync: Listen to Parent Window instead of polling Server
    window.addEventListener('message', function(event) {
        if (event.data && event.data.action === 'syncSlide') {
            if (evaluatorMode) return;
            
            if (event.data.state === 'active') {
                hasJoinedLocally = true;
                sessionState = 'active';
                currentSlide = event.data.slide;
                updateView();
            }
        }
    });"""

text = re.sub(old_sync, new_sync, text)

with open('templates/program_sace/interactive_workshop.html', 'w', encoding='utf-8') as f:
    f.write(text)
