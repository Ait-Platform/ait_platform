from app import create_app
from app.models.billing import BilTenant
try:
    app = create_app()
    with app.app_context():
        print("Flask booted successfully.")
except Exception as e:
    print(f"Error: {e}")
