from app import create_app
from app.models.auth import AuthSubject

app = create_app()
with app.app_context():
    cfi = AuthSubject.query.filter_by(slug="cultural_fire").first()
    if cfi:
        print(f"CFI: commercial_mode={cfi.commercial_mode}, requires_price={cfi.requires_price}, trial_days={cfi.trial_days}, program_type={cfi.program_type}")
