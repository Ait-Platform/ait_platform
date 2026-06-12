from app import create_app
from app.extensions import db
from app.models.auth import AuthSubject

app = create_app()
app.app_context().push()

# Fix HOME (Base Course)
s = AuthSubject.query.filter_by(slug='home').first()
if s:
    s.commercial_mode = 'free'
    s.program_type = 'free'

# Fix HOME Premium
s2 = AuthSubject.query.filter_by(slug='home_premium').first()
if s2:
    s2.commercial_mode = 'paid'
    s2.program_type = 'course'

# Hide HOME2 entirely
s3 = AuthSubject.query.filter_by(slug='home2').first()
if s3:
    s3.is_active = 0

db.session.commit()
print("Fixed HOME subject commercial modes and deactivated home2.")
