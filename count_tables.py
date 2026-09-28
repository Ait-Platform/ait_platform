import sys
try:
    from app import create_app, db
    from app.models.uip import UipRegisterImport, UipRegisterImportException, UipMemberProfile, UipProperty, UipPropertyMember, UipCommunicationPreference, UipAuditEvent
    from app.models.core import CoreOrganizationMember

    app = create_app()
    with app.app_context():
        print(f"UipRegisterImport: {UipRegisterImport.query.count()}")
        print(f"UipRegisterImportException: {UipRegisterImportException.query.count()}")
        print(f"UipMemberProfile: {UipMemberProfile.query.count()}")
        print(f"UipProperty: {UipProperty.query.count()}")
        print(f"UipPropertyMember: {UipPropertyMember.query.count()}")
        print(f"CoreOrganizationMember: {CoreOrganizationMember.query.count()}")
        print(f"UipCommunicationPreference: {UipCommunicationPreference.query.count()}")
        print(f"UipAuditEvent: {UipAuditEvent.query.count()}")
except Exception as e:
    print(f"Error: {e}")
