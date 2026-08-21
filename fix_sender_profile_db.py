from app import create_app
from app.extensions import db
from sqlalchemy import text
import traceback

app = create_app()

with app.app_context():
    try:
        db.session.execute(text('ALTER TABLE sender_profile ADD COLUMN letterhead_url VARCHAR(255)'))
        print('Added letterhead_url')
    except Exception as e:
        print('Could not add letterhead_url (maybe it exists):', str(e))
        db.session.rollback()

    try:
        db.session.execute(text('ALTER TABLE sender_profile ADD COLUMN use_custom_letterhead BOOLEAN DEFAULT FALSE'))
        print('Added use_custom_letterhead')
    except Exception as e:
        print('Could not add use_custom_letterhead (maybe it exists):', str(e))
        db.session.rollback()
        
    db.session.commit()
    print('Done.')
