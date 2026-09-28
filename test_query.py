from app import create_app
from app.extensions import db
from app.models.uip import UipProviderAccount, UipProvider
from app.models.auth import User
from app.models.core import CoreOrganization

app = create_app()
with app.app_context():
    try:
        user = User.query.first()
        org = CoreOrganization.query.first()
        if user and org:
            account = UipProviderAccount.query.filter_by(user_id=user.id).join(UipProvider).filter(UipProvider.organization_id == org.id).first()
            print("Query succeeded:", account)
        else:
            print("No user or org found to test")
    except Exception as e:
        print("ERROR:", type(e).__name__, e)
