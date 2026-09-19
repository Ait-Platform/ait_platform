with open("templates/program_uip/dashboards/secretary_organogram.html", "r", encoding="utf-8") as f:
    text = f.read()

# 1. Add CSRF Tokens
if 'name="csrf_token"' not in text:
    text = text.replace(
        '<input type="hidden" name="action" value="add_seat">',
        '<input type="hidden" name="action" value="add_seat">\n            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">'
    )
    text = text.replace(
        '<input type="hidden" name="action" value="upload_photo">',
        '<input type="hidden" name="action" value="upload_photo">\n            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">'
    )

# 2. Hide Sidebar
style_block = """
<style>
    .ui-sidebar { display: none !important; }
    .ui-shell { grid-template-columns: 1fr !important; display: block !important; }
    .ui-workspace { padding-left: 0 !important; margin-left: 0 !important; max-width: 1200px; margin: 0 auto !important; width: 100%; }
</style>
"""
if "ui-sidebar { display: none !important; }" not in text:
    text = text.replace("{% block content %}", "{% block content %}\n" + style_block)

with open("templates/program_uip/dashboards/secretary_organogram.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Patched CSRF and sidebar")
