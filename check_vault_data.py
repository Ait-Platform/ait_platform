from app import create_app, db
from app.models.uip import UipMemberProfile, UipRegisterImport

app = create_app()
with app.app_context():
    imports = UipRegisterImport.query.all()
    print("IMPORTS:")
    for i in imports:
        print(f"ID {i.id} | Status: {i.status} | Date: {i.effective_date} | Notes: {i.notes}")
        
    members = UipMemberProfile.query.all()
    print("\nMEMBERS:")
    for m in members:
        print(f"ID {m.id} | Email: {m.email} | Name: {m.name} | Source: {m.record_source} | Last Import ID: {m.last_import_id}")
