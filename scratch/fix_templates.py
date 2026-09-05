with open('templates/program_sace/interactive_workshop.html', 'r', encoding='utf-8') as f:
    text = f.read()
if "{% set hide_navbar" not in text:
    text = text.replace("{% extends \"layout.html\" %}", "{% extends \"layout.html\" %}\n{% set hide_navbar = request.args.get('embed') == '1' %}")
    with open('templates/program_sace/interactive_workshop.html', 'w', encoding='utf-8') as f:
        f.write(text)

with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    text = f.read()
if "{% set hide_navbar" not in text:
    text = text.replace("{% extends \"layout.html\" %}", "{% extends \"layout.html\" %}\n{% set hide_navbar = request.args.get('embed') == '1' %}")
    with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
        f.write(text)

print("Added hide_navbar flags to templates")
