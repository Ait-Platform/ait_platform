with open('app/models/__init__.py', 'a', encoding='utf-8') as f:
    f.write("\nfrom .uip import UipProvider, UipWorkOrder, UipMunicipalReferral, UipCommitteeMeeting, UipResolution\n")
