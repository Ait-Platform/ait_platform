
from app import create_app, db
from app.models.culturalfire import CfiTalentSubmission
import sqlalchemy as sa
app = create_app()
with app.app_context():
    inspector = sa.inspect(db.engine)
    columns = [c['name'] for c in inspector.get_columns('cfi_talent_submission')]
    print('Columns in cfi_talent_submission:', columns)

