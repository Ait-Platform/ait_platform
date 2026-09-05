import re

file_path = 'templates/program_sace/presentation_ppp.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# Add logic to ping an endpoint to log viewed_ppp when they reach slide 30
new_btn_update = '''
        if (currentIndex === slides.length - 1) {
            document.getElementById('btn-next').classList.add('hidden');
            document.getElementById('btn-finish').classList.remove('hidden');
            document.getElementById('btn-finish').classList.add('flex');
            
            // Log that they completed the PPP
            fetch("{{ url_for('sace_bp.log_ppp_view') }}", {
                method: "POST",
                headers: { "X-CSRFToken": "{{ csrf_token() }}" }
            });
        } else {
'''

text = text.replace('''
        if (currentIndex === slides.length - 1) {
            document.getElementById('btn-next').classList.add('hidden');
            document.getElementById('btn-finish').classList.remove('hidden');
            document.getElementById('btn-finish').classList.add('flex');
        } else {
''', new_btn_update)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
