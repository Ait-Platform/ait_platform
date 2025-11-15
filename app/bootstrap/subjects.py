# app/bootstrap/subjects.py

from app.extensions import db
from app.models.auth import AuthSubject

CORE_SUBJECTS = [
    {
        "slug": "loss",
        "name": "Loss & Adaptation",
        "is_active": 1,
        "sort_order": 30,
        "start_endpoint": "loss_bp.about_loss",  # or None / "" if not used
    },
    # You can add more later in ONE place, e.g.:
    # {
    #     "slug": "reading",
    #     "name": "Reading Development Program",
    #     "is_active": 1,
    #     "sort_order": 10,
    #     "start_endpoint": "reading_bp.about_reading",
    # },
]

def ensure_core_subjects() -> None:
    """
    Idempotent: makes sure auth_subject has the core AIT subjects.
    - Safe on SQLite and Postgres
    - Safe to call on every startup
    """
    created = 0
    updated = 0

    for cfg in CORE_SUBJECTS:
        slug = cfg["slug"]
        subj = AuthSubject.query.filter_by(slug=slug).first()
        if subj is None:
            subj = AuthSubject(slug=slug)
            created += 1
        else:
            updated += 1

        subj.name          = cfg["name"]
        subj.is_active     = cfg.get("is_active", 1)
        subj.sort_order    = cfg.get("sort_order", 0)
        subj.start_endpoint = cfg.get("start_endpoint")

        db.session.add(subj)

    db.session.commit()
    # optional: you can drop the prints if you want
    print(f"[bootstrap] auth_subject: created={created}, updated={updated}")
