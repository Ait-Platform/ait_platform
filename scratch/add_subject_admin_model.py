import re

with open('app/models/auth.py', 'r', encoding='utf-8') as f:
    text = f.read()

model_code = """
class AuthSubjectAdmin(db.Model):
    __tablename__ = "auth_subject_admin"

    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String, nullable=False, index=True)
    subject_id = db.Column(db.Integer, db.ForeignKey('auth_subject.id'), nullable=False)
    
    subject = db.relationship("AuthSubject", backref="admins")

"""

# Insert it before AuthSubject or after ApprovedAdmin
if "class ApprovedAdmin" in text:
    text = text.replace("class ApprovedAdmin(db.Model):", model_code + "class ApprovedAdmin(db.Model):")

with open('app/models/auth.py', 'w', encoding='utf-8') as f:
    f.write(text)
print("Added AuthSubjectAdmin model")
