import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

migration_logic = '''
    # Auto-migrate payment_method column if it doesn't exist
    from sqlalchemy import inspect, text
    try:
        inspector = inspect(db.engine)
        columns = [col['name'] for col in inspector.get_columns('mech_job_cards')]
        if 'payment_method' not in columns:
            db.session.execute(text("ALTER TABLE mech_job_cards ADD COLUMN payment_method VARCHAR(50) DEFAULT 'EFT'"))
            db.session.commit()
    except Exception as e:
        current_app.logger.error(f"Migration error: {e}")
'''

# Inject right after def job_cards_list():
regex = r'(@mechanic_bp\.route\("/mechanic/jobs"\)\s*@login_required\s*def job_cards_list\(\):)'

content = re.sub(regex, r'\1' + migration_logic, content)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
