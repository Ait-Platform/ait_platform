import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Wrap fetch in try catch
old_fetch = """    fetch('/sace/log_event', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': document.querySelector('meta[name="csrf-token"]').getAttribute('content')
        },
        body: JSON.stringify({
            action: 'AUDITOR_VIEWED_TAB_' + tabId.toUpperCase(),
            details: 'Auditor switched to ' + (tabId === 'a' ? 'Guide' : tabId === 'f' ? 'Facilitator Board' : 'Participant Board')
        })
    });"""

new_fetch = """    try {
        const csrfMeta = document.querySelector('meta[name="csrf-token"]');
        const csrfToken = csrfMeta ? csrfMeta.getAttribute('content') : '';
        fetch('/sace/log_event', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': csrfToken
            },
            body: JSON.stringify({
                action: 'AUDITOR_VIEWED_TAB_' + tabId.toUpperCase(),
                details: 'Auditor switched to ' + (tabId === 'a' ? 'Guide' : tabId === 'f' ? 'Facilitator Board' : 'Participant Board')
            })
        });
    } catch(e) {
        console.error("Audit log failed", e);
    }"""

text = text.replace(old_fetch, new_fetch)

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
