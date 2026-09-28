from app.factory import create_app
from app.extensions import db
from tests.uip.bootstrap import data
def test_import_error():
    app = create_app()
    with app.app_context():
        d = data()
        for user in d.users.values():
            db.session.add(user)
        from tests.uip.test_register import import_records
        row = dict(reference="M1", name="Test Person", member_type="person",
            email="person@example.invalid", phone="0111234567", is_active="true", eligibility_status="eligible")
        from app.program_uip.services import register
        from datetime import date
        batch, summary = register.process_import_batch(d.org.id, d.users["manager"].id, "members", [row],
            dict(source_identifier="Synthetic municipal register", batch_reference="test-fixture",
                 date_received=date(2026,1,1), effective_date=date(2026,1,1)))
        from app.models.uip import UipRegisterException
        exs = UipRegisterException.query.filter_by(batch_id=batch.id).all()
        for ex in exs:
            print(f"EXCEPTION: {ex.error_category} - {ex.description}")
        
test_import_error()
