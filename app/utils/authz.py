# app/utils/authz.py
from app.models import ApprovedAdmin

def is_admin_email(email: str) -> bool:
    if not email:
        return False
    return ApprovedAdmin.query.filter(
        ApprovedAdmin.email == (email or "").strip().lower()
    ).first() is not None
