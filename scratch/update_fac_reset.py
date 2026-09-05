import re

with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Add button to the top header area
old_header = '<button onclick="location.href=\'{{ url_for(\\\'sace.reading_hub\\\') }}\'" class="px-4 py-2 bg-slate-800 hover:bg-slate-700 rounded font-bold shadow transition">Exit</button>'
new_header = """<button onclick="resetWorkshop()" class="px-4 py-2 bg-red-600 hover:bg-red-700 text-white rounded font-bold shadow transition mr-2"><i class="fas fa-trash-alt mr-2"></i>Reset Room</button>
                <button onclick="location.href='{{ url_for('sace.reading_hub') }}'" class="px-4 py-2 bg-slate-800 hover:bg-slate-700 rounded font-bold shadow transition">Exit</button>"""

if old_header in content:
    content = content.replace(old_header, new_header)

# Add Javascript
js_code = """
        function resetWorkshop() {
            if(confirm("Are you sure you want to completely reset the room? This will clear all attendees, erase all poll data, and kick everyone back to the PIN screen.")) {
                const csrfToken = document.querySelector('meta[name="csrf-token"]').getAttribute('content'); 
                fetch('/sace/workshop/reset', {
                    method: 'POST',
                    headers: {'X-CSRFToken': csrfToken}
                })
                .then(() => location.reload());
            }
        }
"""

content = content.replace("function startWorkshop() {", js_code + "\n        function startWorkshop() {")

with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
