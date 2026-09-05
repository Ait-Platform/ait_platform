import re

with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# The bad second definition looks like:
bad_func = """    function resetWorkshop() {
        if(confirm("Are you sure? This resets all connected devices back to the lobby.")) {
            fetch('/sace/workshop/reset', {method: 'POST', headers: {'X-CSRFToken': document.querySelector('meta[name="csrf-token"]').getAttribute('content')}})
                .then(() => fetchState());
        }
    }"""

if bad_func in content:
    content = content.replace(bad_func, "")

with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
