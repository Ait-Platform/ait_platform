from app import create_app
from app.models.auth import AuthSubject
from app.models.payment import SubjectCountryPrice

def export_prices():
    app = create_app()
    with app.app_context():
        prices = SubjectCountryPrice.query.filter_by(country_code='ZA').all()
        sql_statements = []
        for p in prices:
            subj = AuthSubject.query.get(p.subject_id)
            if subj:
                slug = subj.slug
                stmt = f"""INSERT INTO subject_country_price (subject_id, country_code, local_amount_cents, zar_amount_cents, local_currency, is_active)
SELECT id, '{p.country_code}', {p.local_amount_cents}, {p.zar_amount_cents}, '{p.local_currency}', {str(p.is_active).lower()} 
FROM auth_subject WHERE slug = '{slug}'
AND NOT EXISTS (SELECT 1 FROM subject_country_price WHERE subject_id = auth_subject.id AND country_code = '{p.country_code}');"""
                sql_statements.append(stmt)
                
        with open('final_dump.sql', 'w', encoding='utf-8') as f:
            f.write('\n\n'.join(sql_statements))

if __name__ == '__main__':
    export_prices()
