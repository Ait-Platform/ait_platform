#app/program_culturefire/helpers.py
from datetime import date, datetime, timedelta
from app.models.auth import UserEnrollment
from app.extensions import db
from app.models.culturalfire import CfiBiodata, CfiJudgeAssignment, CfiParent, CfiShow, CfiSponsorship, CfiSupporter, CfiTalentFile, CfiTalentSubmission
import os
from werkzeug.utils import secure_filename
from flask import current_app
import uuid


def cultural_fire_post_register(user):
    """
    After a user registers for Cultural Fire, create enrollment
    and send them to Bridge.
    """
    # Create enrollment for Cultural Fire
    enrollment = UserEnrollment(
        user_id=user.id,
        subject_id=12,  # Cultural Fire
        status="started",
        started_at=datetime.utcnow()
    )
    db.session.add(enrollment)
    db.session.commit()

    # Always send to Bridge
    #return redirect(url_for("auth_bp.bridge"))

def calculate_age_from_id(id_number: str) -> int | None:
    """
    Calculate age from a South African ID number (YYMMDD...).
    Returns None if parsing fails.
    """
    try:
        dob_str = id_number[:6]  # YYMMDD
        year = int(dob_str[:2])
        month = int(dob_str[2:4])
        day = int(dob_str[4:6])

        # Determine century
        current_year_two_digits = int(str(datetime.now().year)[2:])
        if year <= current_year_two_digits:
            year += 2000
        else:
            year += 1900

        dob = date(year, month, day)
        today = date.today()
        age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
        return age
    except Exception:
        return None

def next_step(record):
    # Step 1 checks
    if not record.full_name or not record.id_number or not record.dob or not record.phone:
        return 1

    # Step 2 checks
    if not record.gender or not record.city or not record.province \
       or not record.address_line or not record.occupation or not record.highest_qualification:
        return 2

    # Step 3 checks (pledge + role)
    if not record.pledge_agreed or not record.role:
        return 3

    # All complete
    return None

def check_biodata_complete(record):
    return next_step(record) is None

def biodata_complete(record):
    return all([
        record.gender,
        record.city,
        record.province,
        record.address_line,
        record.occupation,
        record.highest_qualification,
        record.pledge_agreed,   # ✅ must be True
        record.role             # ✅ must be set
    ])

def calculate_age_from_dob(dob: date) -> int | None:
    """
    Calculate age from a date of birth.
    Returns None if dob is missing.
    """
    if not dob:
        return None
    today = date.today()
    return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))

def auto_generate_show_from_submissions(submissions):
    if not submissions:
        return None

    start_date = min(sub.created_at for sub in submissions)
    month_year = start_date.strftime("%B %Y")  # e.g. "April 2026"

    sponsor_name = "CFI Sponsors"  # or query your sponsorship table for actual sponsor
    new_show = CfiShow(
        title=f"CFI {month_year} Variety Show – Sponsored by {sponsor_name}",
        description=f"Showcase generated from {len(submissions)} submissions",
        start_date=start_date,
        end_date=max(sub.created_at for sub in submissions),
        location="TBD"
    )
    db.session.add(new_show)
    db.session.commit()

    for sub in submissions:
        sub.show_id = new_show.id
    db.session.commit()

    return new_show

def handle_talent_files(submission, files):
    upload_folder = current_app.config.get("UPLOAD_FOLDER", os.path.join(current_app.root_path, "static", "uploads", "cfi"))
    os.makedirs(upload_folder, exist_ok=True)

    # Only replace if new files were uploaded
    if files and any(f.filename.strip() for f in files):
        # Clear old DB records
        submission.files.clear()

        for file in files:
            if file and file.filename.strip():
                filename = secure_filename(file.filename)
                file_path = os.path.join(upload_folder, filename)
                file.save(file_path)
                submission.files.append(CfiTalentFile(filename=filename))

def moderate_video_with_gemini(filepath):
    import os
    import time
    try:
        import google.generativeai as genai
    except ImportError:
        return True # Fail open if library not installed
        
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        return True
        
    try:
        genai.configure(api_key=api_key)
        video_file = genai.upload_file(path=filepath)
        
        # Wait for processing
        while video_file.state.name == "PROCESSING":
            time.sleep(2)
            video_file = genai.get_file(video_file.name)
            
        if video_file.state.name == "FAILED":
            genai.delete_file(video_file.name)
            return True
            
        model = genai.GenerativeModel(model_name="gemini-1.5-flash")
        prompt = "Analyze this video. Does it contain explicit content (pornography, severe violence, hate symbols)? Reply ONLY with 'SAFE' or 'EXPLICIT'."
        response = model.generate_content([video_file, prompt])
        
        is_safe = 'EXPLICIT' not in response.text.upper()
        
        genai.delete_file(video_file.name)
        return is_safe
    except Exception as e:
        print(f"Gemini Moderation Error: {e}")
        return True

def curate_shows(cutoff=10):
    submissions = CfiTalentSubmission.query.filter(CfiTalentSubmission.show_id == None).all()

    # Group submissions by rules
    pageant_subs = [s for s in submissions if s.category_item and s.category_item.name.lower() in ["pageant", "pageants"]]

    context_groups = {}
    for sub in submissions:
        if not sub.category_item or sub.category_item.name.lower() not in ["pageant", "pageants"]:
            context = sub.context.name if sub.context else "General"
            context_groups.setdefault(context, []).append(sub)

    # Create pageant show
    if pageant_subs:
        pageant_show = CfiShow(title="Pageant Showcase", description="Pageant participants", status="active")
        db.session.add(pageant_show)
        db.session.commit()
        for sub in pageant_subs:
            sub.show_id = pageant_show.id
        db.session.commit()

    # Create context shows
    for context, subs in context_groups.items():
        if len(subs) > cutoff:
            song_items = [s for s in subs if s.category_item and s.category_item.name == "Singing"]
            dance_items = [s for s in subs if s.category_item and s.category_item.name == "Dance"]

            if song_items:
                song_show = CfiShow(title=f"{context} Song Showcase", status="active")
                db.session.add(song_show)
                db.session.commit()
                for s in song_items:
                    s.show_id = song_show.id
                db.session.commit()

            if dance_items:
                dance_show = CfiShow(title=f"{context} Dance Showcase", status="active")
                db.session.add(dance_show)
                db.session.commit()
                for s in dance_items:
                    s.show_id = dance_show.id
                db.session.commit()
        else:
            show = CfiShow(title=f"{context} Showcase", status="active")
            db.session.add(show)
            db.session.commit()
            for s in subs:
                s.show_id = show.id
            db.session.commit()

def create_show(title, submissions, context=None, category=None):
    """
    Create a new CfiShow and attach submissions.
    """
    new_show = CfiShow(
        title=title,
        description=f"{context or ''} {category or ''} Showcase".strip(),
        start_date=datetime.utcnow(),
        end_date=datetime.utcnow() + timedelta(days=1),
        context=context,
        category=category
    )
    db.session.add(new_show)
    db.session.commit()

    # Attach submissions to this show
    for sub in submissions:
        sub.show_id = new_show.id
        db.session.add(sub)

    db.session.commit()
    return new_show

def assign_judges(show):
    # Parents (exclude own children)
    parent_judges = CfiParent.query.filter(CfiParent.child_id.notin_(
        [sub.user_id for sub in CfiTalentSubmission.query.filter_by(show_id=show.id)]
    )).limit(2).all()

    # Sponsors + Supporters
    sponsor_judges = CfiSponsorship.query.limit(1).all()
    supporter_judges = CfiSupporter.query.limit(1).all()

    for judge in parent_judges + sponsor_judges + supporter_judges:
        assignment = CfiJudgeAssignment(judge_id=judge.user_id, show_id=show.id, role=judge.__class__.__name__)
        db.session.add(assignment)

    db.session.commit()

def calculate_age(dob):
    today = date.today()
    return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))



def build_filename(talent_name, original_filename, submission_id=None):
    safe_name = talent_name or "untitled"
    base = secure_filename(safe_name).replace(" ", "_").lower()

    ext = os.path.splitext(original_filename)[1].lower()

    # ALWAYS guarantee uniqueness
    unique_id = submission_id or uuid.uuid4().hex

    return f"{base}_{unique_id}{ext}"

def all_segments_filled(show):
    """
    Check if all required segments for a show are filled.
    Returns True if complete, False otherwise.
    """
    # Define the required segment types for a Pageant flow
    required_segments = {
        "ramp_walk",
        "talent",
        "eastern",
        "western",
        "qna",
        "sponsor",
        "supporter"
    }

    # Collect submitted segment types from linked submissions
    submitted_segments = {s.segment.name.lower() for s in show.submissions if s.segment}

    # Compare sets
    return required_segments.issubset(submitted_segments)

def get_token_cost(action_name: str, default_cost: int = 10) -> int:
    from app.models.billing import TokenTariff
    from app.extensions import db
    tariff = TokenTariff.query.filter_by(program_slug='culturalfire', action_name=action_name).first()
    if not tariff:
        tariff = TokenTariff(program_slug='culturalfire', action_name=action_name, base_token_cost=default_cost)
        db.session.add(tariff)
        db.session.commit()
    return tariff.base_token_cost

def charge_tokens(user_id, amount, description):
    from app.models.auth import AitTokenWallet, AitTokenTransaction
    from app.models.culturalfire import CfiAward
    from app.extensions import db
    wallet = AitTokenWallet.query.filter_by(user_id=user_id).first()
    if not wallet or wallet.balance < amount:
        return False
    wallet.balance -= amount
    txn = AitTokenTransaction(wallet_id=wallet.id, amount=-amount, description=description)
    db.session.add(txn)
    db.session.flush()
    
    # Milestone Award logic based on cumulative tokens spent
    total_spent_val = db.session.query(db.func.sum(AitTokenTransaction.amount)).filter(
        AitTokenTransaction.wallet_id == wallet.id, 
        AitTokenTransaction.amount < 0
    ).scalar() or 0
    total_spent = abs(total_spent_val)
    
    thresholds = {
        1000: ('Silver Award', 'Reached 1,000 tokens spent in the Culture Fire community!'),
        2500: ('Gold Award', 'Reached 2,500 tokens spent in the Culture Fire community!'),
        5000: ('Platinum Award', 'Reached 5,000 tokens spent in the Culture Fire community!')
    }
    
    existing_milestones = CfiAward.query.filter_by(user_id=user_id, award_type='Milestone').all()
    existing_titles = [a.title for a in existing_milestones]
    
    for threshold, (title, desc) in thresholds.items():
        if total_spent >= threshold and title not in existing_titles:
            award = CfiAward(user_id=user_id, award_type='Milestone', title=title, description=desc)
            db.session.add(award)
            
    db.session.commit()
    return True


def assign_questions_for_show(show_id):
    from app.models.culturalfire import CfiShow, CfiSegmentItem, CfiPageantQuestion, CfiQuestionAssignment
    from app.extensions import db
    import random
    
    show = CfiShow.query.get(show_id)
    if not show or not show.category_item or show.category_item.name != 'Pageant':
        return
        
    # Get all contestants (segment items)
    items = CfiSegmentItem.query.filter_by(show_id=show_id).all()
    if not items:
        return
        
    # Get already assigned questions for this show
    assignments = CfiQuestionAssignment.query.filter_by(show_id=show_id).all()
    assigned_item_ids = {a.segment_item_id for a in assignments}
    assigned_q_ids = {a.question_id for a in assignments}
    
    unassigned_items = [i for i in items if i.id not in assigned_item_ids]
    if not unassigned_items:
        return # All assigned
        
    all_questions = CfiPageantQuestion.query.all()
    available_qs = [q for q in all_questions if q.id not in assigned_q_ids]
    
    # If we run out of questions, just recycle them (though 20 should be enough for a single show)
    if len(available_qs) < len(unassigned_items):
        available_qs = list(all_questions)
        random.shuffle(available_qs)
    else:
        random.shuffle(available_qs)
        
    for item in unassigned_items:
        q = available_qs.pop(0)
        assignment = CfiQuestionAssignment(show_id=show_id, segment_item_id=item.id, question_id=q.id)
        db.session.add(assignment)
        
    db.session.commit()
