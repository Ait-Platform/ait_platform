import re

with open('app/models/auth.py', 'r', encoding='utf-8') as f:
    text = f.read()

model_code = """
class FranchiseLicense(db.Model):
    __tablename__ = "franchise_license"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    subject_id = db.Column(db.Integer, db.ForeignKey('auth_subject.id'), nullable=False)
    total_seats = db.Column(db.Integer, nullable=False, default=0)
    used_seats = db.Column(db.Integer, nullable=False, default=0)
    created_at = db.Column(db.DateTime, default=db.func.now())
    
    user = db.relationship("User", backref="franchise_licenses")
    subject = db.relationship("AuthSubject", backref="franchise_licenses")

"""

if "class FranchiseLicense" not in text:
    text = text.replace("class UserEnrollment(db.Model):", model_code + "class UserEnrollment(db.Model):")
    with open('app/models/auth.py', 'w', encoding='utf-8') as f:
        f.write(text)
    print("Added FranchiseLicense model")
else:
    print("FranchiseLicense already exists")
