import re
with open("templates/program_uip/ui.css", "r", encoding="utf-8") as f:
    text = f.read()

# Add the new specific color blocks at the very end
new_css = """
/* Distinct Navigation Header Colors */
details.ui-nav-purple>summary { color: #5b21b6; }
details.ui-nav-purple>summary:hover, details.ui-nav-purple[open]>summary { background: #ede9fe; }

details.ui-nav-emerald>summary { color: #065f46; }
details.ui-nav-emerald>summary:hover, details.ui-nav-emerald[open]>summary { background: #d1fae5; }

details.ui-nav-amber>summary { color: #92400e; }
details.ui-nav-amber>summary:hover, details.ui-nav-amber[open]>summary { background: #fef3c7; }

details.ui-nav-rose>summary { color: #9f1239; }
details.ui-nav-rose>summary:hover, details.ui-nav-rose[open]>summary { background: #ffe4e6; }

details.ui-nav-blue>summary { color: #1e40af; }
details.ui-nav-blue>summary:hover, details.ui-nav-blue[open]>summary { background: #dbeafe; }

details.ui-nav-teal>summary { color: #115e59; }
details.ui-nav-teal>summary:hover, details.ui-nav-teal[open]>summary { background: #ccfbf1; }

details.ui-nav-fuchsia>summary { color: #86198f; }
details.ui-nav-fuchsia>summary:hover, details.ui-nav-fuchsia[open]>summary { background: #fae8ff; }

details.ui-nav-slate>summary { color: #334155; }
details.ui-nav-slate>summary:hover, details.ui-nav-slate[open]>summary { background: #f1f5f9; }
"""
if "Distinct Navigation Header Colors" not in text:
    text = text + new_css

with open("templates/program_uip/ui.css", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated ui.css with nav colors")
