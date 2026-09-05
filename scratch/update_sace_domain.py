import re

# Update Hub Title
file_path = 'templates/program_sace/reading_hub.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

text = text.replace('Provider Litre Reading Activity', "Provider's SACE Activities")
text = text.replace('SACE Activity Evaluation Hub', "Provider's SACE Activities")

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)

# Update AGENT.md
agent_path = 'AGENT.md'
with open(agent_path, 'r', encoding='utf-8') as f: agent_text = f.read()

domain_note = '''## 5. System Architecture (SACE Module)
- **Provider vs SACE Relationship (Critical Domain Context):** AIT is the *Provider*. We are giving SACE an activity for approval. The activities belong to the Provider (AIT), NOT SACE. SACE is merely an endorsement entity. When naming pages or UI elements, do not frame them as if SACE owns the activity (e.g., use "Provider's SACE Activities" rather than "SACE's Evaluation Hub").'''

agent_text = agent_text.replace('## 5. System Architecture (SACE Module)', domain_note)

with open(agent_path, 'w', encoding='utf-8') as f: f.write(agent_text)
