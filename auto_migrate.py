import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

regex = r'(@mechanic_bp\.route\("/mechanic/dashboard"\)\s*@login_required\s*def mechanic_dashboard\(\):)'

migration_logic = '''\\1
    # Auto-migrate payment_method column if it doesn't exist
    from sqlalchemy import inspect, text
    try:
        inspector = inspect(db.engine)
        columns = [col['name'] for col in inspector.get_columns('mech_job_cards')]
        if 'payment_method' not in columns:
            # Handle SQLite vs Postgres
            if db.engine.dialect.name == 'sqlite':
                db.session.execute(text("ALTER TABLE mech_job_cards ADD COLUMN payment_method VARCHAR(50) DEFAULT 'EFT'"))
            else:
                db.session.execute(text("ALTER TABLE mech_job_cards ADD COLUMN payment_method VARCHAR(50) DEFAULT 'EFT'"))
            db.session.commit()
    except Exception as e:
        current_app.logger.error(f"Migration error: {e}")
'''

content = re.sub(regex, migration_logic, content)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
