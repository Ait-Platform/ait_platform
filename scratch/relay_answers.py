import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

old_listener = r"""        } else if \(event\.data\.action === 'slideChanged'\) \{
            // Update auditor banner locally
            updateAuditorBanner\(event\.data\.slide\);
            
            // Relay to participant board instantly
            const pIframe = document\.getElementById\('participantIframe'\);
            if \(pIframe && pIframe\.contentWindow\) \{
                pIframe\.contentWindow\.postMessage\(\{action: 'syncSlide', slide: event\.data\.slide, state: event\.data\.state\}, '\*'\);
            \}
        \}
    \}
\}\);"""

new_listener = """        } else if (event.data.action === 'slideChanged') {
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

text = re.sub(old_listener, new_listener, text)

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
