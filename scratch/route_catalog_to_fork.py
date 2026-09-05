import re

with open('templates/program_sace/sace_catalog.html', 'r', encoding='utf-8') as f:
    text = f.read()

old_link = "{{ url_for('quote_bp.quote', subject='sace_' ~ activity.slug) }}"
new_link = "{{ url_for('quote_bp.franchise_fork', subject='sace_' ~ activity.slug) }}"

if old_link in text:
    text = text.replace(old_link, new_link)
    with open('templates/program_sace/sace_catalog.html', 'w', encoding='utf-8') as f:
        f.write(text)
    print("Updated sace_catalog.html to route to fork")
else:
    print("Could not find link in sace_catalog")
