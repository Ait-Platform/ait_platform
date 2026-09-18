import re
with open("templates/program_uip/dashboards/resolution_view.html", "r", encoding="utf-8") as f:
    text = f.read()

old_header = """    <div class="mb-8 flex justify-between items-start">
        <div class="w-full">"""

new_header = """    <div class="mb-8 flex justify-between items-start">
        <div class="flex-grow pr-8">"""
text = text.replace(old_header, new_header)

with open("templates/program_uip/dashboards/resolution_view.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Fixed header flex layout")
