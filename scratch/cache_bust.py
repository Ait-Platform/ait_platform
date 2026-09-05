import re
import time

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Add cache buster to iframe src
cache_buster = str(int(time.time()))
text = text.replace(
    "{{ url_for('sace_bp.facilitator_dashboard') }}?embed=1",
    f"{{{{ url_for('sace_bp.facilitator_dashboard') }}}}?embed=1&cb={cache_buster}"
)

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
