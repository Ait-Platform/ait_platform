
from app import create_app, db
import sqlalchemy as sa
app = create_app()
with app.app_context():
    engine = db.engine
    with engine.begin() as conn:
        try:
            conn.execute(sa.text('ALTER TABLE cfi_talent_submission ADD COLUMN question_id INTEGER REFERENCES cfi_pageant_question(id)'))
            print('Column added successfully.')
        except Exception as e:
            print(f'Error: {e}')

