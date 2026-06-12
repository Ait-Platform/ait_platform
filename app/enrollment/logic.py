from app.extensions import db
from app.models.payment import SubjectCountryPrice


def get_quote_for_subject_country(subject_id, country_code, price_id=None):
    q = SubjectCountryPrice.query.filter_by(subject_id=subject_id, country_code=country_code)
    if price_id:
        q = q.filter_by(id=price_id)
    return q.first()


