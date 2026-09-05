import re

with open('templates/layout.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Add a check for the footer
if "{% if not hide_navbar %}" not in text.split("<!-- Footer")[1]:
    # Replace the footer start
    text = text.replace("<footer class=\"fixed inset-x-0", "{% if not hide_navbar %}\n  <footer class=\"fixed inset-x-0")
    # Replace the footer end
    text = text.replace("</footer>", "</footer>\n  {% endif %}")
    
    with open('templates/layout.html', 'w', encoding='utf-8') as f:
        f.write(text)
    print("Wrapped footer in condition")
else:
    print("Footer already wrapped")
