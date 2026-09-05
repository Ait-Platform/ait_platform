import re

with open('templates/program_sace/interactive_workshop.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Close the script tag properly after updateView()
content = content.replace("    // Initialize\n    updateView();\n<!-- Floating SACE Guide Button -->", "    // Initialize\n    updateView();\n</script>\n<!-- Floating SACE Guide Button -->")

# 2. Remove the trailing </script> at the very end of the file
content = re.sub(r'</script>\s*$', '', content)

with open('templates/program_sace/interactive_workshop.html', 'w', encoding='utf-8') as f:
    f.write(content)
