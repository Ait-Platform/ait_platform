
from app import create_app, db
import sqlalchemy as sa
app = create_app()
with app.app_context():
    inspector = sa.inspect(db.engine)
    indices = inspector.get_indexes('cfi_talent_submission')
    for idx in indices:
        print(idx)

