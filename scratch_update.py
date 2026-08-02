from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
app.app_context().push()

db.session.execute(text("UPDATE auth_subject SET show_on_welcome = true WHERE slug IN ('mechanic', 'practice_crm')"))
db.session.commit()
print('Set show_on_welcome locally via script')
