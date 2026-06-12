from flask import session
from flask_login import current_user

from app.models.auth import AuthSubject

'''
def get_cfi_baton():
    return {
        "subject_slug": session.get("subject_slug"),   # always 'cultural_fire'
        "subject_id": session.get("subject_id"),
        "user_id": current_user.id if current_user.is_authenticated else None,
        "role": session.get("role"),                   # participant, sponsor, etc.
    }
''' 

def get_cfi_baton():
    baton = session.get("baton")

    if not baton:
        subj = AuthSubject.query.filter_by(slug="cultural_fire").first()
        if not subj:
            return None

        baton = {
            "subject_id": subj.id,
            "subject_slug": subj.slug
        }
        session["baton"] = baton

    return baton

