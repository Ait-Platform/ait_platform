import re

with open("app/models/core.py", "r", encoding="utf-8") as f:
    text = f.read()

if "parent_id = db.Column" not in text:
    old_col = 'closed_by = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)'
    new_col = 'closed_by = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)\n    parent_id = db.Column(db.Integer, db.ForeignKey("core_interaction.id"), nullable=True) # Master Ticket Link'
    text = text.replace(old_col, new_col)
    
    old_rel = 'tasks = db.relationship("CoreTask", back_populates="interaction", cascade="all, delete-orphan")'
    new_rel = 'tasks = db.relationship("CoreTask", back_populates="interaction", cascade="all, delete-orphan")\n    children = db.relationship("CoreInteraction", backref=db.backref("parent", remote_side="CoreInteraction.id"))'
    text = text.replace(old_rel, new_rel)

    with open("app/models/core.py", "w", encoding="utf-8") as f:
        f.write(text)
    print("Added parent_id to CoreInteraction")
else:
    print("parent_id already exists")
