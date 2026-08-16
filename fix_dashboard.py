import re

with open('templates/program_mechanic/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Find the first script block that ends just before {% endblock %} {% block content %}
start_str = "  <script>\n    // AI Business Card Upload\n    const cardUploadInput = document.getElementById('ajax_card_upload');"
end_str = "  </script>\n  \n  {% endblock %}\n  {% block content %}"

start_idx = content.find(start_str)
end_idx = content.find(end_str)

if start_idx != -1 and end_idx != -1:
    # Remove it
    content = content[:start_idx] + content[end_idx + len("  </script>\n  \n"):]
    with open('templates/program_mechanic/dashboard.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Replaced successfully")
else:
    print(f"Could not find block. start: {start_idx}, end: {end_idx}")
