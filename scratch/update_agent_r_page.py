import re

file_path = 'AGENT.md'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

domain_note = '''## 5. System Architecture (SACE Module)
- **R Page (Reneilwe / SACE Admin Page):** We will design an "R page" for the SACE Admin later. This page will host the Audit Logs (/sace/audit_report). Do not place the Audit Logs button on the participant or evaluator hubs.
- **Provider vs SACE Relationship (Critical Domain Context):** AIT is the *Provider*. We are giving SACE an activity for approval. The activities belong to the Provider (AIT), NOT SACE. SACE is merely an endorsement entity. When naming pages or UI elements, do not frame them as if SACE owns the activity (e.g., use "Provider's SACE Activities" rather than "SACE's Evaluation Hub").'''

text = text.replace('## 5. System Architecture (SACE Module)\n- **Provider vs SACE Relationship', domain_note)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
