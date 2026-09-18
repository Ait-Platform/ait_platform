import re
with open("templates/program_uip/base.html", "r", encoding="utf-8") as f:
    text = f.read()

hide_sidebar_css = """{% if is_secretary %}
  <style>
      .ui-sidebar { display: none !important; }
      .ui-shell { grid-template-columns: 1fr !important; display: block !important; }
      .ui-workspace { padding-left: 0 !important; margin-left: 0 !important; max-width: 1200px; margin: 0 auto !important; width: 100%; }
      .ui-topbar { border-radius: 0 0 12px 12px; box-shadow: 0 1px 3px rgba(0,0,0,0.05); }
  </style>
  {% endif %}
  </head>"""

text = text.replace("</head>", hide_sidebar_css)

with open("templates/program_uip/base.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated base.html with Secretary full-screen override")
