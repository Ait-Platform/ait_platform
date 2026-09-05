from app import create_app
app = create_app()
with app.app_context():
    from app.models.auth import AuthSubject
    sace = AuthSubject.query.filter_by(slug='sace').first()
    if sace:
        print({k: v for k, v in sace.__dict__.items() if not k.startswith('_')})
    else:
        print('SACE not found')
