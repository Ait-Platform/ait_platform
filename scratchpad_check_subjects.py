from app import create_app
from app.extensions import db
from app.models.auth import AuthSubject

app = create_app()
with app.app_context():
    subjects = AuthSubject.query.all()
    print(f"{'SLUG':<15} | {'ACTIVE':<6} | {'HIDDEN':<6} | {'PROGRAM TYPE':<15}")
    print("-" * 50)
    for s in subjects:
        print(f"{s.slug:<15} | {s.is_active:<6} | {s.is_hidden_on_bridge:<6} | {s.program_type:<15}")
