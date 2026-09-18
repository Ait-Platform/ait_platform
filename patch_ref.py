import re
with open("app/models/uip.py", "r", encoding="utf-8") as f:
    text = f.read()

# I want to add a @property reference to UipResolution
old_res = """class UipResolution(db.Model):
    __tablename__ = "uip_resolution"
    id = db.Column(db.Integer, primary_key=True)"""

new_res = """class UipResolution(db.Model):
    __tablename__ = "uip_resolution"
    id = db.Column(db.Integer, primary_key=True)
    
    @property
    def reference(self):
        from datetime import datetime
        year = self.created_at.year if self.created_at else datetime.utcnow().year
        return f"RES-{year}-{self.id:03d}" if self.id else "RES-DRAFT"
"""

text = text.replace(old_res, new_res)

with open("app/models/uip.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Added reference property to UipResolution")
