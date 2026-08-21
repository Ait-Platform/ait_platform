from app import create_app, db
from app.models.auth import AuthSubject
from sqlalchemy import text

app = create_app()

with app.app_context():
    # Find old ones
    old_subjects = AuthSubject.query.filter(
        AuthSubject.slug.in_(['sace', 'cptd', 'sace_hub']) |
        AuthSubject.name.ilike('%cptd%') |
        AuthSubject.name.ilike('%sace%')
    ).all()
    
    for s in old_subjects:
        print(f"Deactivating old subject: {s.name} (ID {s.id})")
        s.is_active = 0
        s.slug = f"{s.slug}_old_{s.id}"
        s.show_on_welcome = False
        s.is_hidden_on_bridge = True
        
    db.session.commit()
    
    # Create the new SACE Hub
    new_sace = AuthSubject(
        slug="sace_hub",
        name="SACE Activity Approval Hub",
        is_active=1,
        sort_order=100,
        commercial_mode="free",
        program_type="free",
        enroll_policy="auto_enroll",
        show_on_welcome=True,
        about_endpoint="auth_bp.login",
        bypass_dashboard_endpoint="sace_bp.dashboard",
        start_endpoint="sace_bp.dashboard",
        is_hidden_on_bridge=False
    )
    db.session.add(new_sace)
    db.session.commit()
    print(f"Created new SACE Hub with ID {new_sace.id}")
