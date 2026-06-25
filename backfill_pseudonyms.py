from wsgi import app
from app.extensions import db
from app.models.spv import SpvParticipation
from app.models.auth import User

with app.app_context():
    participations = SpvParticipation.query.all()
    count = 0
    for p in participations:
        if not p.pseudonym:
            user = User.query.get(p.user_id)
            if user:
                name_part = (user.name or user.email.split('@')[0])[:3].capitalize()
                id_part = str(user.id)[-3:].zfill(3)
                p.pseudonym = f"{name_part}{id_part}"
                count += 1
    db.session.commit()
    print(f"Successfully backfilled {count} empty pseudonyms.")
