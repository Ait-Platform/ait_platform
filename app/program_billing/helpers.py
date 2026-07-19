from flask_login import current_user
from app.extensions import db
from app.models.billing import BilProperty

def get_dashboard_data(show_hidden=False):
    """
    Fetches property data for the logged-in owner/manager.
    Returns 1 row per property to avoid dashboard duplication.
    """
    if show_hidden:
        props = BilProperty.query.filter_by(manager_id=current_user.id).all()
    else:
        props = BilProperty.query.filter_by(manager_id=current_user.id, is_archived=False).all()
    data = []
    
    for p in props:
        tenant_name = None
        tenant_id = None
        meter_summary = None
        utility_summary = None
        
        if p.tenants:
            tenant_name = p.tenants[0].name
            tenant_id = p.tenants[0].id
        if p.meters:
            meter_summary = f"{len(p.meters)} Meters Linked"
            types = list(set([m.utility_type for m in p.meters if m.utility_type]))
            utility_summary = ", ".join(types)
                
        data.append({
            "property_id": p.id,
            "property_name": p.name,
            "address": p.address if p.address and p.address != "None" else "",
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "meter_number": meter_summary,
            "utility_type": utility_summary,
            "is_archived": p.is_archived
        })

    return data

def sync_muni_accounts():
    """
    Syncs bil_meter.municipal_bill_number into bil_muni_account.
    This ensures that the ledger has a matching account record for every unique municipal bill number.
    """
    from app.models.billing import BilMeter, BilMuniAccount, RefMuniOwner
    
    # Get all distinct municipal_bill_numbers that are not null/empty
    sql = """
        SELECT DISTINCT municipal_bill_number 
        FROM bil_meter 
        WHERE municipal_bill_number IS NOT NULL AND municipal_bill_number != ''
    """
    bill_numbers = db.session.execute(db.text(sql)).scalars().all()
    
    for bill_no in bill_numbers:
        # Check if an account already exists for this bill number
        acc = BilMuniAccount.query.filter_by(account_number=bill_no).first()
        
        # Find the meters for this bill number to link them
        meters = BilMeter.query.filter_by(municipal_bill_number=bill_no).all()
        water_meter_id = None
        elec_meter_id = None
        
        for m in meters:
            if m.utility_type and m.utility_type.lower() == 'water':
                water_meter_id = m.id
            elif m.utility_type and m.utility_type.lower() == 'electricity':
                elec_meter_id = m.id
                
        if not acc:
            # We need a default owner just to satisfy the schema, or we can look up the manager's name
            owner_name = "Unknown Owner"
            if meters:
                first_meter = meters[0]
                if first_meter.property_id:
                    from app.models.billing import BilProperty, BilProperty
                    from app.models.auth import User
                    unit = BilProperty.query.get(first_meter.property_id)
                    if unit and unit.property_id:
                        prop = BilProperty.query.get(unit.property_id)
                        if prop and prop.manager_id:
                            manager = User.query.get(prop.manager_id)
                            if manager:
                                owner_name = manager.name or manager.email
            
            # Find or create RefMuniOwner
            ref_owner = RefMuniOwner.query.filter_by(name=owner_name).first()
            if not ref_owner:
                ref_owner = RefMuniOwner(name=owner_name)
                db.session.add(ref_owner)
                db.session.flush()
                
            acc = BilMuniAccount(
                account_number=bill_no,
                owner_id=ref_owner.id,
                water_meter_id=water_meter_id,
                elec_meter_id=elec_meter_id
            )
            db.session.add(acc)
        else:
            # Update existing account's meters if they changed
            if water_meter_id and not acc.water_meter_id:
                acc.water_meter_id = water_meter_id
            if elec_meter_id and not acc.elec_meter_id:
                acc.elec_meter_id = elec_meter_id
                
    db.session.commit()
