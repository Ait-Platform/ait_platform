with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Replace iframe src
text = text.replace("src=\"{{ url_for('sace_bp.interactive_workshop') }}\"", "src=\"{{ url_for('sace_bp.interactive_workshop') }}?embed=1\"")
text = text.replace("src=\"{{ url_for('sace_bp.facilitator_dashboard') }}\"", "src=\"{{ url_for('sace_bp.facilitator_dashboard') }}?embed=1\"")

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
print("Added embed=1 to simulator IFrames")
