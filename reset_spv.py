from wsgi import app
from app.extensions import db
from app.models.auth import User

with app.app_context():
    user = User.query.filter_by(email="spv@gmail.com").first()
    if user:
        user.set_password("12345678")
        db.session.commit()
        print("Password for spv@gmail.com has been successfully reset to: 12345678")
    else:
        print("User spv@gmail.com not found!")
