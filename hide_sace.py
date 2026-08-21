import os
from app import create_app, db
from app.models.auth import AuthSubject

app = create_app()
render_url = 'postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db'
app.config['SQLALCHEMY_DATABASE_URI'] = render_url

with app.app_context():
    subjects = AuthSubject.query.all()
    for s in subjects:
        if 'sace' in s.name.lower() or 'cptd' in s.name.lower() or 'sace' in s.slug.lower():
            print(f"Hiding: SLUG: {s.slug}, NAME: {s.name}")
            s.show_on_welcome = False
    
    db.session.commit()
    print("Done hiding subjects.")
