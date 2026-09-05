import re

agent_path = 'agent.md'
with open(agent_path, 'r', encoding='utf-8') as f:
    text = f.read()

# Update terminology
old_term = '''- **Terminology (Critical):** The physical machine/hardware is the "LITRE Blending Machine". However, the official SACE Provider Activity name is **"I Learn to Read English Using the LITRE Method"**. NEVER refer to the activity as "LITRE Blending Machine" or "Litre Reading" in the UI. Always use the full correct activity name.'''

new_term = '''- **Terminology (Critical):** The physical machine/hardware is the "LITRE Blending Machine". However, the official SACE Provider Activity name is **"I Learn to Read English Using the LITRE Method"**. NEVER refer to the activity as "LITRE Blending Machine" or "Litre Reading" in the UI. Always use the full correct activity name.
- **Auditors vs Evaluators:** When dealing with SACE's endorsement process, always refer to the individuals as "Auditors" (e.g. use "Provisioned Auditors"). You can provide examples of Auditors acting as "evaluators", but the official noun must be Auditors.'''

text = text.replace(old_term, new_term)

with open(agent_path, 'w', encoding='utf-8') as f:
    f.write(text)
