from app import create_app
from app.extensions import db
from app.models.auth import AuthSubject

app = create_app()
app.app_context().push()

s = AuthSubject.query.filter_by(slug='home').first()
if s:
    s.name = 'Hands On Math Education (HOME)'

s2 = AuthSubject.query.filter_by(slug='home_premium').first()
if s2:
    s2.name = 'HOME Premium Upgrade'

db.session.commit()
print('Renamed successfully!')
