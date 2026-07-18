from app import create_app
from app.extensions import db
from app.models.auth import AuthSubject

def main():
    app = create_app()
    with app.app_context():
        # Check if exists
        secure_admin = AuthSubject.query.filter_by(slug='admin_secure').first()
        if not secure_admin:
            secure_admin = AuthSubject(
                name='Secure Setup Admin',
                slug='admin_secure',
                program_type='admin',
                is_hidden_on_bridge=0,
                requires_price=0
            )
            db.session.add(secure_admin)
            db.session.commit()
            print("Successfully inserted AuthSubject for admin_secure.")
        else:
            print("AuthSubject admin_secure already exists.")
            
if __name__ == "__main__":
    main()
