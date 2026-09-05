import re

with open('app/models/core.py', 'r', encoding='utf-8') as f:
    text = f.read()

old_task_rels = """    # Relationships
    interaction = db.relationship("CoreInteraction", back_populates="tasks")"""

new_task_rels = """    # Relationships
    interaction = db.relationship("CoreInteraction", back_populates="tasks")
    assignee = db.relationship("User", foreign_keys=[assignee_id])"""

text = text.replace(old_task_rels, new_task_rels)

with open('app/models/core.py', 'w', encoding='utf-8') as f:
    f.write(text)
