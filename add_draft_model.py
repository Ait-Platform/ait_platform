with open('app/models/billing.py', 'r', encoding='utf-8') as f:
    content = f.read()

new_model = """class BilArchitectureDraft(db.Model):
    __tablename__ = 'bil_architecture_draft'
    id = db.Column(db.Integer, primary_key=True)
    property_id = db.Column(db.Integer, db.ForeignKey('bil_property.id', ondelete='CASCADE'), nullable=False, unique=True)
    draft_json = db.Column(db.JSON, nullable=True)
    updated_at = db.Column(db.DateTime, default=func.now(), onupdate=func.now())
"""

if 'class BilArchitectureDraft' not in content:
    # Append it to the end of the file
    content += "\n" + new_model
    with open('app/models/billing.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Added BilArchitectureDraft model.")
else:
    print("BilArchitectureDraft model already exists.")
