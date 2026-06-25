from wsgi import app
from app.extensions import db
from app.models.auth import User

with app.app_context():
    users = User.query.all()
    print(f"Total users in DB: {len(users)}")
    for u in users:
        print(f"ID={u.id}, Email={u.email}, Name={u.name}")
