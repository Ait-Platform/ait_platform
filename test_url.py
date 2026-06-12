from app import create_app
from flask import url_for

app = create_app()
with app.test_request_context():
    print(url_for('yoco_bp.yoco_start'))
