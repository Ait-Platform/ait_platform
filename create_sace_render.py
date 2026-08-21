import os
from app import create_app, db
from app.models.auth import AuthSubject

app = create_app()
render_url = 'postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db'
app.config['SQLALCHEMY_DATABASE_URI'] = render_url

with app.app_context():
    sace = AuthSubject.query.filter_by(slug='sace').first()
    if not sace:
        print("Creating SACE subject in Render DB...")
        sace = AuthSubject(
            slug='sace',
            name='SACE Evaluation & Endorsement',
            program_type='system',
            commercial_mode='free',
            is_active=1,
            show_on_welcome=0,
            is_hidden_on_bridge=1,
            about_endpoint='sace_bp.sace_about',
            bypass_dashboard_endpoint='sace_bp.dashboard'
        )
        db.session.add(sace)
        db.session.commit()
        print("Created!")
    else:
        print("SACE subject already exists.")
