"""Isolated imports: never import or call the shared application factory."""
import sys
import types
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
for name, path in (("app", ROOT / "app"), ("app.models", ROOT / "app/models")):
    package = types.ModuleType(name)
    package.__path__ = [str(path)]
    sys.modules[name] = package
from app.extensions import db
from flask_login import UserMixin
class User(UserMixin, db.Model):
    __tablename__ = "user"
    id = db.Column(db.Integer, primary_key=True)
    is_active = db.Column(db.Integer, nullable=False, server_default="1")
    name = db.Column(db.String(255))
    email = db.Column(db.String(255), unique=True)
auth = types.ModuleType("app.models.auth")
auth.User = User
sys.modules["app.models.auth"] = auth
db.Table("ait_token_transaction", db.metadata, db.Column("id", db.Integer, primary_key=True))
from app.models import core, uip
NEW_TABLES = ("uip_member_profile", "uip_property", "uip_property_member",
              "uip_member_representative", "uip_communication_preference", "uip_audit_event")
