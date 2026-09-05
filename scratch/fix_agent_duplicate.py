import re

file_path = 'AGENT.md'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# Fix the duplicate text
bad_duplicate = 'than "SACE\\'s Evaluation Hub"). (Critical Domain Context):** AIT is the *Provider*. We are giving SACE an activity for approval. The activities belong to the Provider (AIT), NOT SACE. SACE is merely an endorsement entity. When naming pages or UI elements, do not frame them as if SACE owns the activity (e.g., use "Provider\\'s SACE Activities" rather than "SACE\\'s Evaluation Hub").'
text = text.replace(bad_duplicate, 'than "SACE\\'s Evaluation Hub").')

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
