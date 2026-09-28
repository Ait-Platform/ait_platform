import json
from app import create_app, db
from app.models.uip import UipMemberProfile, UipRegisterImport

app = create_app()
with app.app_context():
    imports = [{"id": i.id, "status": i.status, "date": str(i.effective_date), "notes": i.notes} for i in UipRegisterImport.query.all()]
    members = [{"id": m.id, "email": m.email, "name": m.name, "source": m.record_source, "active": m.is_active, "eligibility": m.eligibility_status, "last_import_id": m.last_import_id} for m in UipMemberProfile.query.all()]
    
    with open('db_dump.json', 'w') as f:
        json.dump({"imports": imports, "members": members}, f, indent=2)
