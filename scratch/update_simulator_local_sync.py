import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Fix cache buster in iframe
text = text.replace('cb=1788072626', 'cb={{ range(1, 999999) | random }}')

# Remove the setInterval
pattern_interval = r'// Sync loop.*?1000\);'
text = re.sub(pattern_interval, '', text, flags=re.DOTALL)

# Update the window message listener
old_listener = r"""window\.addEventListener\('message', function\(event\) \{
    if \(event\.data && event\.data\.action === 'switchToParticipant'\) \{
        switchTab\('p'\);
    \}
\}\);"""

new_listener = """window.addEventListener('message', function(event) {
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
        }
    }
});"""
text = text.replace(old_listener, new_listener)

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
