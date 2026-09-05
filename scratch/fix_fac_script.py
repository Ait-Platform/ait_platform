import re

with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Close the script tag properly after fetchState()
content = content.replace("    // Initialize\n    updateView();\n    fetchState();\n<!-- Roster Modal -->", "    // Initialize\n    updateView();\n    fetchState();\n</script>\n<!-- Roster Modal -->")

# 2. Remove the trailing </script> at the very end of the file
content = re.sub(r'</script>\s*{% endblock %}', '{% endblock %}', content)

with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
