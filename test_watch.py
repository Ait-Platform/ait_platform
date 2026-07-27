from app import create_app, db
from app.models.culturalfire import CfiShow, CfiPrivateShowGroup, CfiTalentCategoryItem, CfiMcAssignment, CfiJudgeAssignment, CfiShowAd, CfiTalentSubmission, CfiMcRecording
from app.models.auth import User
from flask import render_template

app = create_app()
with app.app_context():
    # Simulate current_user via a mock
    class MockUser:
        id = 1
        is_authenticated = True
    
    app.jinja_env.globals['current_user'] = MockUser()
    
    # Just grab any show
    show = CfiShow.query.first()
    if not show:
        print("No shows in DB.")
        exit(0)
        
    print(f"Testing with show: {show.title}")
    
    def get_url(url):
        return url

    submissions = (
        CfiTalentSubmission.query
        .filter_by(show_id=show.id)
        .all()
    )
    submissions_data = [
        {
            "id": sub.id,
            "title": sub.talent_name or sub.custom_talent or "Untitled",
            "segment_type": "all",
            "src": get_url(file.filename),
            "user_id": sub.user_id
        }
        for sub in submissions
        for file in (sub.files or [])
        if file and file.filename
    ]

    for sub in submissions_data:
        sub['has_voted'] = False

    available_segments = []

    recordings = CfiMcRecording.query.filter_by(show_id=show.id).order_by(CfiMcRecording.id.desc()).all()
    ads = CfiShowAd.query.filter_by(show_id=show.id).all()
    
    show_intro = next((r for r in recordings if r.recording_type == 'show_intro'), None)
    show_outro = next((r for r in recordings if r.recording_type == 'show_outro'), None)

    for s in submissions_data:
        s['item_type'] = 'act'
        
    unified_playlist = []
    
    first_segment = 'all'
    last_segment = 'all'
    
    middle_index = max(1, len(submissions_data) // 2)
    
    pre_show_ads = [ad for ad in ads if ad.position_index == 0]
    for ad in pre_show_ads:
        unified_playlist.append({
            "id": f"ad_{ad.id}",
            "title": "Sponsor Message",
            "segment_type": first_segment,
            "src": get_url(ad.video_url),
            "item_type": "ad",
            "has_voted": False,
            "user_id": ad.user_id
        })
    
    if show_intro:
        unified_playlist.append({
            "id": f"mc_intro_{show_intro.id}",
            "title": "Welcome to the Show!",
            "segment_type": first_segment,
            "src": get_url(show_intro.media_url),
            "item_type": "mc",
            "has_voted": False,
            "user_id": None
        })
        
    for idx, act in enumerate(submissions_data):
        unified_playlist.append(act)
        
    outro_ads = [ad for ad in ads if ad.position_index == 99]
    for ad in outro_ads:
        unified_playlist.append({
            "id": f"ad_{ad.id}",
            "title": "Sponsor Message",
            "segment_type": last_segment,
            "src": get_url(ad.video_url),
            "item_type": "ad",
            "has_voted": False,
            "user_id": ad.user_id
        })
        
    submissions_data = unified_playlist

    origin = "test"
    enrollment_id = 1

    is_judge = False
    is_mc = False
    
    cat_name = show.category_item.name if show.category_item else "Unknown"
    cat_lower = cat_name.lower()
    judge_criteria = [{"id": "test"}]

    mc_assignments = CfiMcAssignment.query.filter_by(show_id=show.id).all()
    judge_assignments = CfiJudgeAssignment.query.filter_by(show_id=show.id).all()
    
    show_mcs = []
    show_judges = []
    show_advertisers = []
    mc_criteria = []

    try:
        ctx = {
            'is_judge': is_judge,
            'is_mc': is_mc,
            'judge_criteria': judge_criteria,
            'mc_criteria': mc_criteria,
            'show': show,
            'submissions_data': submissions_data,
            'available_segments': available_segments,
            'origin': origin,
            'enrollment_id': enrollment_id,
            'show_mcs': show_mcs,
            'show_judges': show_judges,
            'show_advertisers': show_advertisers
        }
        with app.test_request_context('/'):
            html = render_template("program_culturefire/watch_show.html", **ctx)
            print("Rendered successfully!")
    except Exception as e:
        import traceback
        traceback.print_exc()
