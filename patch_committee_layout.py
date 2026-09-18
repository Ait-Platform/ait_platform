import re
with open("templates/program_uip/dashboards/committee.html", "r", encoding="utf-8") as f:
    text = f.read()

# Remove the sidebar part
start_str = '<div class="flex flex-col md:flex-row gap-8 mb-16">'
end_str = '<!-- MAIN DASHBOARD -->'
start_idx = text.find(start_str)
end_idx = text.find(end_str)

if start_idx != -1 and end_idx != -1:
    text = text[:start_idx] + '<!-- MAIN DASHBOARD -->' + text[end_idx + len(end_str):]

# Remove the classes from MAIN DASHBOARD div
text = text.replace('<div class="w-full md:w-3/4">', '<div class="w-full mb-16">')

# Remove the final closing div from the flex layout
text = text.replace('    </div>\n</div>\n\n{% endblock %}', '    </div>\n\n{% endblock %}')

with open("templates/program_uip/dashboards/committee.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated committee dashboard layout")
