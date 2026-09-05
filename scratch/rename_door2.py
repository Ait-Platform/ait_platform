with open('templates/program_sace/sace_selection_hub.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace("3. Teacher / Participant", "3. Participant")

with open('templates/program_sace/sace_selection_hub.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Renamed door")
