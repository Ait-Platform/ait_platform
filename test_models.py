from app import create_app
from app.extensions import db
from app.models.uip import UipCommitteeMember, UipSubcommitteeMember

app = create_app()
with app.app_context():
    pass
