from app import create_app
from app.extensions import db
from datetime import date
import re

app = create_app()

with app.app_context():
    from app.models.culturalfire import CfiShow, CfiSegmentItem, CfiPageantSegment
    from app.models.auth import UserEnrollment
    from app.models.culturalfire import CfiBiodata

    # 1. Find generic pageant shows (title doesn't contain " - ")
    pageant_shows = CfiShow.query.filter(
        CfiShow.title.notlike("% - %"),
        CfiShow.title.like("%Pageant%")
    ).all()

    for show in pageant_shows:
        print(f"Processing Show {show.id}: {show.title}")
        
        # Get all segments attached to this show
        items = CfiSegmentItem.query.filter_by(show_id=show.id).all()
        
        if not items:
            # If no items, just rename it to a generic Ramp Walk show to be safe
            show.title = f"{show.title} - Ramp Walk"
            db.session.commit()
            print(f"Renamed empty show to {show.title}")
            continue
            
        # Determine gender from the first item
        gender = "Female"
        first_item = items[0]
        enrollment = UserEnrollment.query.get(first_item.enrollment_id)
        if enrollment:
            biodata = CfiBiodata.query.filter_by(user_id=enrollment.user_id).first()
            if biodata and biodata.gender:
                gender = biodata.gender
                
        # Parse show number
        match = re.search(r"Show (\d+)", show.title)
        show_num = match.group(1) if match else "1"
        base_title = f"Pageant ({gender}) Show {show_num}"
        
        print(f"Base title for splitting: {base_title}")
        
        # We will reuse the original show for "Ramp Walk"
        original_show_id = show.id
        show.title = f"{base_title} - Ramp Walk"
        
        # For each distinct segment in items, if it's not Ramp Walk, create a show
        # Actually, let's create shows for ALL PageantSegments so they exist
        segments = [s for s in CfiPageantSegment.query.all() if s.name.lower() not in ('sponsor', 'supporter')]
        
        segment_to_show_map = {}
        for seg in segments:
            if seg.name.lower() == "ramp walk" or seg.name.lower() == "ramp_walk":
                segment_to_show_map[seg.name] = original_show_id
            else:
                new_show = CfiShow(
                    title=f"{base_title} - {seg.name}",
                    description=f"{show.description} for {seg.name}",
                    start_date=show.start_date,
                    location=show.location,
                    status=show.status,
                    category_item_id=show.category_item_id
                )
                db.session.add(new_show)
                db.session.commit() # commit to get ID
                segment_to_show_map[seg.name] = new_show.id
                print(f"Created new show: {new_show.title} (ID {new_show.id})")
                
        # Update existing items to point to correct show
        for item in items:
            # Map item.segment_type (e.g. ramp_walk, talent, qna) to seg.name
            normalized_type = item.segment_type.replace("_", " ").title()
            if normalized_type == "Qna": normalized_type = "Q&A"
            if normalized_type == "Q And A": normalized_type = "Q&A"
            
            # Find matching segment key
            matched_key = next((k for k in segment_to_show_map.keys() if k.lower() == normalized_type.lower()), None)
            
            if matched_key:
                old_show = item.show_id
                item.show_id = segment_to_show_map[matched_key]
                print(f"Moved item {item.id} ({item.segment_type}) from show {old_show} to {item.show_id}")
            else:
                print(f"Warning: Could not match segment type '{item.segment_type}' for item {item.id}")
                
        db.session.commit()
    print("Migration complete!")
