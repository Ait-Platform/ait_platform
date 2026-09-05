import re

# Update simulator.html
sim_path = 'templates/program_sace/simulator.html'
with open(sim_path, 'r', encoding='utf-8') as f: sim_text = f.read()

sim_text = sim_text.replace('Provider Interactive Simulator', 'Provider Interactive Demo')

with open(sim_path, 'w', encoding='utf-8') as f: f.write(sim_text)

# Update reading_hub.html
hub_path = 'templates/program_sace/reading_hub.html'
with open(hub_path, 'r', encoding='utf-8') as f: hub_text = f.read()

hub_text = hub_text.replace('Interactive Demo</span>', 'Provider Interactive Demo</span>')

with open(hub_path, 'w', encoding='utf-8') as f: f.write(hub_text)
