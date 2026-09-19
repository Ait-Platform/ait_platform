with open("templates/program_uip/provisioning.html", "r", encoding="utf-8") as f:
    text = f.read()

style_block = """
<style>
    .ui-sidebar { display: none !important; }
    .ui-shell { grid-template-columns: 1fr !important; display: block !important; }
    .ui-workspace { padding-left: 0 !important; margin-left: 0 !important; max-width: 1200px; margin: 0 auto !important; width: 100%; }
</style>
"""

if "<style>" not in text and "{% block content %}" in text:
    text = text.replace("{% block content %}", "{% block content %}\n" + style_block)
    with open("templates/program_uip/provisioning.html", "w", encoding="utf-8") as f:
        f.write(text)
    print("Patched provisioning.html")
else:
    print("Could not patch provisioning.html")
