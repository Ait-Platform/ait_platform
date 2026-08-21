import os
from app import create_app, db
from app.models.auth import AuthSubject

app = create_app()

def update_db(url, is_local=False):
    app.config['SQLALCHEMY_DATABASE_URI'] = url
    with app.app_context():
        try:
            subjects = AuthSubject.query.all()
            for s in subjects:
                # Hide CPTD and SACE from welcome
                if 'sace' in s.name.lower() or 'cptd' in s.name.lower() or 'sace' in s.slug.lower():
                    s.show_on_welcome = False
                
                # Rename SACE Evaluation & Endorsement to SACE Activity Approval Hub
                if 'sace evaluation' in s.name.lower() or s.slug == 'sace':
                    s.name = 'SACE Activity Approval Hub'
                    
            db.session.commit()
            print(f"Successfully updated {'local' if is_local else 'Render'} database.")
        except Exception as e:
            print(f"Failed to update {'local' if is_local else 'Render'} DB: {e}")

# Update Local
update_db('postgresql+psycopg2://ait_local:temp1234@localhost:5432/ait_local_db', is_local=True)

# Update Render
update_db('postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db', is_local=False)

