import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Replace the entire window.addEventListener block
pattern = r"window\.addEventListener\('message', function\(event\) \{.*?(?=\n\s*// Ensure A is open on load)"
replacement = """window.addEventListener('message', function(event) {
    if (event.data) {
        if (event.data.action === 'switchToParticipant') {
            switchTab('p');
        } else if (event.data.action === 'slideChanged') {
            // Update auditor banner locally
            updateAuditorBanner(event.data.slide);
            
            // Relay to participant board instantly
            const pIframe = document.getElementById('participantIframe');
            if (pIframe && pIframe.contentWindow) {
                pIframe.contentWindow.postMessage({action: 'syncSlide', slide: event.data.slide, state: event.data.state}, '*');
            }
        } else if (event.data.action === 'participantAnswer') {
            // Relay participant's answer to the F board
            const fIframe = document.getElementById('facilitatorIframe');
            if (fIframe && fIframe.contentWindow) {
                fIframe.contentWindow.postMessage(event.data, '*');
            }
        }
    }
});"""

text = re.sub(pattern, replacement, text, flags=re.DOTALL)

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
