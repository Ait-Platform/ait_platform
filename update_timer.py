import re

filepath = 'app/program_uip/secretary_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

search_block = '''    processed_claims = CoreInteraction.query.filter(
        CoreInteraction.organization_id == org.id,
        CoreInteraction.status.in_(["VERIFIED", "PENDING_RESOLUTION"]),
        CoreInteraction.interaction_type.in_([
            "committee_claim", "ratepayer_claim", "subcommittee_claim", "mo_claim", "staff_claim", "unknown_claim"
        ])
    ).order_by(CoreInteraction.id.desc()).limit(15).all()
    
    enriched_processed = []
    for pc in processed_claims:
        creator = User.query.get(pc.creator_id)
        enriched_processed.append({
            "id": pc.id,
            "type": pc.interaction_type,
            "title": pc.title,
            "status": pc.status,
            "user_name": creator.name or "User",
            "user_email": creator.email,
        })'''

replace_block = '''    from datetime import datetime, timedelta
    processed_claims = CoreInteraction.query.filter(
        CoreInteraction.organization_id == org.id,
        CoreInteraction.status.in_(["VERIFIED", "PENDING_RESOLUTION"]),
        CoreInteraction.interaction_type.in_([
            "committee_claim", "ratepayer_claim", "subcommittee_claim", "mo_claim", "staff_claim", "unknown_claim"
        ])
    ).order_by(CoreInteraction.updated_at.desc()).limit(15).all()
    
    now = datetime.utcnow()
    enriched_processed = []
    for pc in processed_claims:
        creator = User.query.get(pc.creator_id)
        
        can_undo = False
        minutes_left = 0
        if pc.updated_at:
            # Drop timezone info if it exists so we can subtract naive datetimes
            pc_time = pc.updated_at.replace(tzinfo=None)
            delta = now - pc_time
            if delta < timedelta(minutes=15):
                can_undo = True
                minutes_left = max(0, 15 - int(delta.total_seconds() / 60))
                
        enriched_processed.append({
            "id": pc.id,
            "type": pc.interaction_type,
            "title": pc.title,
            "status": pc.status,
            "user_name": creator.name or "User",
            "user_email": creator.email,
            "can_undo": can_undo,
            "minutes_left": minutes_left
        })'''

content = content.replace(search_block, replace_block)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated route with 15 minute timer")
