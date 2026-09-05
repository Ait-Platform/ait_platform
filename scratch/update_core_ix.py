import re

with open('app/models/core.py', 'r', encoding='utf-8') as f:
    text = f.read()

old_ix = """class CoreInteraction(db.Model):
    __tablename__ = "core_interaction"
    id = db.Column(db.Integer, primary_key=True)
    reference = db.Column(db.String(50), unique=True, index=True) # e.g. "MG-00482"
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    creator_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False) # e.g., the Resident
    
    interaction_type = db.Column(db.String(50), nullable=False) # enquiry, complaint, request, etc.
    title = db.Column(db.String(255), nullable=False)
    description = db.Column(db.Text)
    
    status = db.Column(db.String(50), default="open", index=True)
    priority = db.Column(db.String(50), default="normal")
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    organization = db.relationship("CoreOrganization", backref="interactions")
    tasks = db.relationship("CoreTask", back_populates="interaction", cascade="all, delete-orphan")"""

new_ix = """class CoreInteraction(db.Model):
    __tablename__ = "core_interaction"
    id = db.Column(db.Integer, primary_key=True)
    reference = db.Column(db.String(50), unique=True, index=True) # e.g. "MG-00482"
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    
    # Audit Actors
    creator_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    assigned_to = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)
    closed_by = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)
    
    # Core Data
    channel = db.Column(db.String(50)) # e.g., Telephone, Web, Walk-in
    category = db.Column(db.String(100)) # e.g., Security, Maintenance
    interaction_type = db.Column(db.String(50), nullable=False) 
    
    title = db.Column(db.String(255), nullable=False)
    description = db.Column(db.Text)
    
    status = db.Column(db.String(50), default="NEW", index=True)
    priority = db.Column(db.String(50), default="NORMAL")
    
    # Timestamps
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    closed_at = db.Column(db.DateTime, nullable=True)

    # Relationships
    organization = db.relationship("CoreOrganization", backref="interactions")
    tasks = db.relationship("CoreTask", back_populates="interaction", cascade="all, delete-orphan")
    
    # User Relationships (using foreign_keys to disambiguate)
    creator = db.relationship("User", foreign_keys=[creator_id], backref="created_interactions")
    assignee = db.relationship("User", foreign_keys=[assigned_to], backref="assigned_interactions")
    closer = db.relationship("User", foreign_keys=[closed_by], backref="closed_interactions")"""

if old_ix in text:
    text = text.replace(old_ix, new_ix)
    with open('app/models/core.py', 'w', encoding='utf-8') as f:
        f.write(text)
    print("Updated CoreInteraction with detailed Functional Spec fields.")
else:
    print("Could not find the CoreInteraction block exactly.")
