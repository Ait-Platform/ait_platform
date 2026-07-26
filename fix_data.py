from app import create_app
from app.extensions import db
from app.models.culturalfire import CfiTalentSubmission, CfiShow, CfiSegmentItem, CfiPageantSegment
import os

# Temporarily override the DATABASE_URL to Render DB
os.environ["DATABASE_URL"] = "postgresql://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

app = create_app()

with app.app_context():
    print("Starting data fix on Render DB...")
    
    pageant_shows = CfiShow.query.filter(CfiShow.title.like("%Pageant%")).all()
    pageant_show_ids = [s.id for s in pageant_shows]
    
    submissions = CfiTalentSubmission.query.filter(CfiTalentSubmission.show_id.in_(pageant_show_ids)).all()
    
    created_count = 0
    synced_count = 0
    shows_created = 0
    
    for sub in submissions:
        if not sub.talent_name: continue
        
        normalized_name = sub.talent_name.replace("_", " ").title()
        if normalized_name in ["Qna", "Q And A"]: normalized_name = "Q&A"
        
        orig_show = CfiShow.query.get(sub.show_id)
        if not orig_show: continue
        
        base_title = orig_show.title.split(" - ")[0]
        
        target_show = None
        if normalized_name.lower() == "ramp walk":
            target_show = orig_show
        else:
            target_show = CfiShow.query.filter_by(title=f"{base_title} - {normalized_name}").first()
            if not target_show:
                target_show = CfiShow(
                    title=f"{base_title} - {normalized_name}",
                    description=f"{orig_show.description} for {normalized_name}",
                    start_date=orig_show.start_date,
                    location=orig_show.location,
                    status=orig_show.status,
                    category_item_id=orig_show.category_item_id
                )
                db.session.add(target_show)
                db.session.commit()
                shows_created += 1
                
        segment_type_norm = normalized_name.lower().replace(" ", "_").replace("&", "n")
        existing_item = CfiSegmentItem.query.filter_by(
            enrollment_id=sub.user_enrollment_id,
            show_id=target_show.id,
            segment_type=segment_type_norm
        ).first()
        
        if not existing_item:
            new_item = CfiSegmentItem(
                enrollment_id=sub.user_enrollment_id,
                show_id=target_show.id,
                segment_type=segment_type_norm,
                title=normalized_name,
                status="uploaded" if sub.video_url else "pending",
                video_url=sub.video_url
            )
            db.session.add(new_item)
            created_count += 1
        else:
            if not existing_item.video_url and sub.video_url:
                existing_item.video_url = sub.video_url
                existing_item.status = "uploaded"
                synced_count += 1
                
    db.session.commit()
    print(f"Data Fix Complete: Created {shows_created} missing shows, {created_count} segment items, synced {synced_count} videos.")
