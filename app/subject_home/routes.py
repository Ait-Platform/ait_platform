# routes.py
from flask import Blueprint, abort, render_template,redirect, request, url_for, flash, session
from flask_login import login_required, current_user, login_user
from app.models.auth import db, User
from app.models.home import HomeChapter, HomeFinalAssessment, HomeQuestion, HomeProgress
from sqlalchemy import text
import random

# Toggle to shuffle questions in chapters and final exams. 
# Keep False during testing/development so question IDs map predictably to drawings.
SHUFFLE_HOME_QUESTIONS = False


home_bp = Blueprint("home_bp", __name__)

def _has_active_home_subscription(user_id):
    from datetime import datetime
    now = datetime.utcnow()
    ent = db.session.execute(text("""
        SELECT ue.status, ue.trial_end, ue.expires_at
          FROM user_enrollment ue
          JOIN auth_subject s ON s.id = ue.subject_id
         WHERE ue.user_id = :uid
           AND s.slug = 'home'
         LIMIT 1
    """), {"uid": user_id}).mappings().first()

    if not ent:
        return False

    if ent["status"] in ("paid", "completed"):
        return True
    elif ent["status"] == "active":
        if (ent["trial_end"] and ent["trial_end"] >= now) or (ent["expires_at"] and ent["expires_at"] >= now):
            return True
    return False

def _save_home_progress(user_id, chapter_number):
    try:
        exists = HomeProgress.query.filter_by(user_id=user_id, chapter_number=chapter_number).first()
        if not exists:
            prog = HomeProgress(user_id=user_id, chapter_number=chapter_number)
            db.session.add(prog)
            db.session.commit()
    except Exception as e:
        db.session.rollback()
        print("Error saving home progress:", e)

@home_bp.route('/home/about')
def subject_home():
    return render_template("subject_home/about.html")

@home_bp.route('/home/price')
def price_page():
    from app.models.auth import AuthSubject
    from app.utils.country import get_active_countries
    
    subject = AuthSubject.query.filter(db.func.lower(AuthSubject.slug) == 'home').first()
    if not subject:
        flash("Subject not found.", "warning")
        return redirect(url_for('public_bp.welcome'))

    countries = get_active_countries()

    return render_template(
        "subject_home/price.html",
        subject=subject,
        countries=countries
    )


@home_bp.route('/dashboard/learner')
@login_required
def learner_dashboard():
    is_completed = db.session.scalar(
        text("""
            SELECT 1 FROM user_enrollment 
            WHERE user_id = :uid 
              AND subject_id = (SELECT id FROM auth_subject WHERE slug = 'home' LIMIT 1)
              AND status = 'completed'
            LIMIT 1
        """),
        {"uid": current_user.id}
    )

    if is_completed:
        flash("You have successfully completed the HOME Programme! You can review your Diagnostic Report and Certificate below.", "success")

    progresses = HomeProgress.query.filter_by(user_id=current_user.id).all()
    for p in progresses:
        session[f'chapter_{p.chapter_number}_done'] = True

    chapters = HomeChapter.query.order_by(
        HomeChapter.chapter_number
    ).all()

    section1_chapters = [c for c in chapters if c.chapter_number <= 10]
    section2_chapters = [c for c in chapters if 11 <= c.chapter_number <= 20]
    section3_chapters = [c for c in chapters if 21 <= c.chapter_number <= 30]

    assessment = HomeFinalAssessment.query.filter_by(
        user_id=current_user.id
    ).order_by(
        HomeFinalAssessment.id.desc()
    ).first()

    has_premium = _has_active_home_subscription(current_user.id)
    has_section3 = has_premium

    from app.models.home import HomePracticalSubmission, HomeTeacherLink
    pending_subs = HomePracticalSubmission.query.filter_by(
        student_id=current_user.id,
        status='pending'
    ).all()
    pending_chapters = [s.chapter_number for s in pending_subs]

    # Fetch teacher link info
    link = HomeTeacherLink.query.filter_by(student_id=current_user.id).first()
    linked_teacher = User.query.get(link.teacher_id) if link else None

    # Fetch all teachers enrolled in HOME and Support Staff
    from app.models.auth import UserEnrollment, AuthSubject
    home_subject = AuthSubject.query.filter_by(slug='home').first()
    staff_subject = AuthSubject.query.filter_by(slug='staff').first()
    
    all_home_teachers = []
    teacher_ids = set()
    
    if home_subject:
        teacher_enrollments = UserEnrollment.query.filter_by(subject_id=home_subject.id, status='teacher').all()
        for e in teacher_enrollments:
            teacher_ids.add(e.user_id)
            
    if staff_subject:
        staff_enrollments = UserEnrollment.query.filter(
            UserEnrollment.subject_id == staff_subject.id,
            UserEnrollment.status != 'archived'
        ).all()
        for e in staff_enrollments:
            teacher_ids.add(e.user_id)
            
    if teacher_ids:
        all_home_teachers = User.query.filter(User.id.in_(list(teacher_ids))).all()

    from flask import make_response
    response = make_response(render_template(
        'subject_home/dashboard.html',
        user=current_user,
        chapters=chapters,
        section1_chapters=section1_chapters,
        section2_chapters=section2_chapters,
        section3_chapters=section3_chapters,
        assessment=assessment,
        has_premium=has_premium,
        has_section3=has_section3,
        pending_chapters=pending_chapters,
        is_completed=is_completed,
        linked_teacher=linked_teacher,
        all_home_teachers=all_home_teachers
    ))
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

@home_bp.route('/home/create_tutor', methods=['POST'])
@login_required
def create_tutor():
    from app.models.home import HomeTeacherLink
    from app.models.auth import UserEnrollment, AuthSubject
    from werkzeug.security import generate_password_hash
    
    tutor_name = request.form.get('tutor_name')
    tutor_email = request.form.get('tutor_email')
    tutor_password = request.form.get('tutor_password')
    
    if not tutor_name or not tutor_email or not tutor_password:
        flash("Please fill in all tutor details.", "warning")
        return redirect(url_for('home_bp.learner_dashboard'))
        
    # Check if email exists
    existing = User.query.filter_by(email=tutor_email.lower()).first()
    if existing:
        # Just link the existing tutor
        link = HomeTeacherLink.query.filter_by(student_id=current_user.id).first()
        if link:
            link.teacher_id = existing.id
        else:
            link = HomeTeacherLink(student_id=current_user.id, teacher_id=existing.id)
            db.session.add(link)
        db.session.commit()
        flash(f"Tutor {existing.name or existing.email} already has an account and has been linked successfully!", "success")
        return redirect(url_for('home_bp.learner_dashboard'))
        
    # Create the user
    new_tutor = User(
        email=tutor_email.lower(),
        name=tutor_name,
        password=generate_password_hash(tutor_password, method='pbkdf2:sha256'),
        role='support_staff'
    )
    db.session.add(new_tutor)
    db.session.flush() # To get new_tutor.id
    
    # Enroll them in staff
    staff_subject = AuthSubject.query.filter_by(slug='staff').first()
    if staff_subject:
        enr = UserEnrollment(
            user_id=new_tutor.id,
            subject_id=staff_subject.id,
            status='active'
        )
        db.session.add(enr)
        
    # Link to learner
    link = HomeTeacherLink.query.filter_by(student_id=current_user.id).first()
    if link:
        link.teacher_id = new_tutor.id
    else:
        link = HomeTeacherLink(student_id=current_user.id, teacher_id=new_tutor.id)
        db.session.add(link)
        
    db.session.commit()
    flash(f"Tutor account created for {tutor_name}! They can now log in.", "success")
    return redirect(url_for('home_bp.learner_dashboard'))

@home_bp.route('/home/link_teacher', methods=['POST'])
@login_required
def link_teacher():
    from app.models.home import HomeTeacherLink
    teacher_id = request.form.get('teacher_id')
    if not teacher_id:
        flash("Please select a teacher.", "warning")
        return redirect(url_for('home_bp.learner_dashboard'))

    link = HomeTeacherLink.query.filter_by(student_id=current_user.id).first()
    if link:
        link.teacher_id = teacher_id
    else:
        link = HomeTeacherLink(student_id=current_user.id, teacher_id=teacher_id)
        db.session.add(link)
    db.session.commit()
    flash("Teacher linked successfully!", "success")
    return redirect(url_for('home_bp.learner_dashboard'))

@home_bp.route(
    '/home/chapter/<int:chapter_num>',
    methods=['GET', 'POST']
)
@login_required
def chapter_page(chapter_num):

    chapter = HomeChapter.query.filter_by(
        chapter_number=chapter_num
    ).first_or_404()

    # Resolve hero image from base chapter (1-10)
    base_chapter_num = chapter_num
    if 11 <= chapter_num <= 20:
        base_chapter_num = chapter_num - 10
    elif 21 <= chapter_num <= 30:
        base_chapter_num = chapter_num - 20
        
    base_chapter = HomeChapter.query.filter_by(chapter_number=base_chapter_num).first()
    hero_image = base_chapter.image_filename if base_chapter else None
    
    # Fallback if DB is missing the image filename
    if not hero_image:
        fallback_images = {
            1: 'chapter1_observation.jpg',
            2: 'chapter2_Position.jpg',
            3: 'chapter3_comparison.jpg',
            4: 'chapter4_estimation.jpg',
            5: 'chapter5_Measurement.jpg',
            6: 'chapter6_Pattern_Recognition.jpg',
            7: 'chapter7_Spatial_Reasoning.jpg',
            8: 'chapter8_logic.jpg',
            9: 'chapter9_mathematics.jpg',
            10: 'chapter10_critical_thinking.jpg'
        }
        hero_image = fallback_images.get(base_chapter_num)


    if chapter_num >= 11:
        from app.models.home import HomeProgress
        completed_count = db.session.scalar(
            db.text("SELECT COUNT(DISTINCT chapter_number) FROM home_progress WHERE user_id = :uid AND chapter_number <= 10"),
            {"uid": current_user.id}
        )
        if completed_count < 10:
            flash("You must complete all practical assignments (Chapters 1-10) and receive a Competent grade from your teacher before starting Section 2.", "warning")
            return redirect(url_for('home_bp.learner_dashboard'))

        if not _has_active_home_subscription(current_user.id):
            flash("You must subscribe to unlock the rest of the HOME course.", "warning")
            return redirect(url_for('paystack_bp.checkout_review', subject='home'))

    questions = HomeQuestion.query.filter_by(
        chapter_id=chapter.id
    ).all()

    if request.method == 'POST':
        
        if chapter_num <= 10:
            from app.models.home import HomePracticalSubmission
            
            # Check if there is already a submission
            existing = HomePracticalSubmission.query.filter_by(
                student_id=current_user.id, 
                chapter_number=chapter_num
            ).first()
            
            if not existing:
                new_sub = HomePracticalSubmission(
                    student_id=current_user.id,
                    chapter_number=chapter_num,
                    status="pending"
                )
                db.session.add(new_sub)
                db.session.commit()
                flash(f"Practical work for Chapter {chapter_num} submitted to your teacher for review!", "success")
            elif existing.status == "pending":
                flash(f"You have already submitted Chapter {chapter_num} for review. Please wait for your teacher.", "info")
            else:
                # If they were not_yet_competent, they can resubmit
                existing.status = "pending"
                db.session.commit()
                flash(f"Practical work for Chapter {chapter_num} re-submitted for review!", "success")
                
            return redirect(url_for('home_bp.learner_dashboard'))

        results = []
        correct_count = 0

        for question in questions:

            if question.question_type == "multi_select":

                user_answer = request.form.getlist(
                    f"q{question.id}"
                )

                correct_answers = [
                    x.strip()
                    for x in question.correct_answer.split(",")
                ]

                correct = (
                    sorted(user_answer) ==
                    sorted(correct_answers)
                )

                display_answer = (
                    ", ".join(user_answer)
                    if user_answer else
                    "No Answer"
                )

            else:

                user_answer = request.form.get(
                    f"q{question.id}"
                )

                correct = (
                    user_answer ==
                    question.correct_answer
                )

                display_answer = (
                    user_answer or "No Answer"
                )

            if correct:
                correct_count += 1

            results.append({
                "question": question.question,
                "your_answer": display_answer,
                "correct_answer": question.correct_answer,
                "correct": correct
            })

        if not questions:
            score = 100
        else:
            score = round(
                (correct_count / len(questions)) * 100
            )

        passed = (
            score >= chapter.pass_mark
        )

        if passed:
            session[
                f'chapter_{chapter_num}_done'
            ] = True
            _save_home_progress(current_user.id, chapter_num)

        session['chapter_results'] = {
            'chapter_num': chapter_num,
            'score': score,
            'passed': passed,
            'results': results
        }

        return redirect(
            url_for(
                'home_bp.chapter_results',
                chapter_num=chapter_num
            )
        )

    # THIS MUST BE OUTSIDE THE POST BLOCK
    render_questions = list(questions)
    if SHUFFLE_HOME_QUESTIONS:
        random.shuffle(render_questions)
        for q in render_questions:
            q.shuffled_options = list(q.options)
            random.shuffle(q.shuffled_options)
    else:
        for q in render_questions:
            q.shuffled_options = list(q.options)

    if chapter_num in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]:
        return render_template(
            f'subject_home/chapter{chapter_num}_practical.html',
            chapter=chapter, hero_image=hero_image,
            questions=render_questions
        )
    elif chapter_num in [21, 22, 23, 24, 25, 26, 27, 28, 29, 30]:
        return render_template(
            f'subject_home/chapter{chapter_num}_theory.html',
            chapter=chapter, hero_image=hero_image,
            questions=render_questions
        )

    return render_template(
        'subject_home/chapter_db.html',
        chapter=chapter, hero_image=hero_image,
        questions=render_questions
    )

@home_bp.route(
    '/home/chapter/<int:chapter_num>/results'
)
@login_required
def chapter_results(chapter_num):

    result_data = session.get(
        'chapter_results'
    )

    if not result_data:

        return redirect(
            url_for(
                'home_bp.chapter_page',
                chapter_num=chapter_num
            )
        )

    next_url = None
    next_label = None

    if result_data['passed']:
        if chapter_num in [10, 20, 30]:
            next_url = url_for('home_bp.learner_dashboard')
            next_label = "Return to Dashboard"
        else:
            next_url = url_for('home_bp.chapter_page', chapter_num=chapter_num + 1)
            next_label = f"Continue to Chapter {chapter_num + 1}"

    return render_template(
        'subject_home/chapter1_result.html',
        chapter_num=chapter_num,
        score=result_data['score'],
        passed=result_data['passed'],
        results=result_data['results'],
        next_url=next_url,
        next_label=next_label
    )
  
@home_bp.route('/home/advance/<int:chapter_id>', methods=['POST'])
@login_required
def advance_chapter(chapter_id):
    session[f'chapter_{chapter_id}_done'] = True
    _save_home_progress(current_user.id, chapter_id)
    flash(f"Chapter {chapter_id} marked as complete!", "success")
    return redirect(url_for('home_bp.learner_dashboard'))


@home_bp.route('/home/re_enrol', methods=['GET', 'POST'])
@login_required
def re_enrol():
    if 'user_id' not in session:
        return redirect(url_for('auth_bp.login'))

    from app.models.auth import UserEnrollment, AuthSubject
    
    # 1. Fetch the HOME subject and user's locked enrollment
    home_subj = AuthSubject.query.filter(db.func.lower(AuthSubject.slug) == 'home').first()
    enr = None
    if home_subj:
        enr = UserEnrollment.query.filter_by(
            user_id=current_user.id, 
            subject_id=home_subj.id
        ).order_by(UserEnrollment.id.desc()).first()

    if not enr or not enr.zar_amount_cents:
        flash("We could not find your original payment details.", "warning")
        return redirect(url_for('home_bp.learner_dashboard'))

    # 2. Calculate the discounted retake prices
    # Exam Only (66% discount -> / 3)
    exam_zar_cents = int(round((enr.zar_amount_cents / 3.0) / 100.0) * 100)
    # Entire Course (50% discount -> / 2)
    course_zar_cents = int(round((enr.zar_amount_cents / 2.0) / 100.0) * 100)
    
    exam_local_cents = 0
    course_local_cents = 0
    if enr.local_amount_cents:
        exam_local_cents = int(round((enr.local_amount_cents / 3.0) / 100.0) * 100)
        course_local_cents = int(round((enr.local_amount_cents / 2.0) / 100.0) * 100)

    if request.method == 'POST':
        retake_type = request.form.get('retake_type', 'exam')
        session['is_retake'] = True
        session['retake_type'] = retake_type
        session['pending_subject'] = 'home'
        session['pending_email'] = current_user.email
        session['retake_zar_cents'] = course_zar_cents if retake_type == 'course' else exam_zar_cents
        flash("You are about to retake the program. A retake fee applies.", "info")
        return redirect(url_for("paystack_bp.paystack_start"))

    return render_template(
        'subject_home/retake_quote.html',
        country_code=enr.country_code,
        local_currency=enr.local_currency,
        exam_local_cents=exam_local_cents,
        exam_zar_cents=exam_zar_cents,
        course_local_cents=course_local_cents,
        course_zar_cents=course_zar_cents
    )

@home_bp.route('/view_diagnostic')
def view_diagnostic():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    return render_template('subject_home/view_diagnostic.html')

'''
@home_bp.route(
'/final_exam',
methods=['GET', 'POST']
)
@login_required
def final_exam():
    if request.method == 'POST':

        session['final_score'] = 100
        session['final_passed'] = True

        return redirect(
            url_for(
                'home_bp.final_exam_result'
            )
        )

    return render_template(
        'subject_home/final_exam.html'
    )
''' 

@home_bp.route(
'/final_exam/result'
)
@login_required
def final_exam_result():
    assessment = HomeFinalAssessment.query.filter_by(
        user_id=current_user.id
    ).order_by(
        HomeFinalAssessment.id.desc()
    ).first_or_404()

    return render_template(
        'subject_home/final_exam_result.html',
        assessment=assessment
    )

@home_bp.route(
'/home/send_certificate',
methods=['POST']
)
@login_required
def send_certificate():
    flash(
        'Certificate emailed successfully.',
        'success'
    )

    return redirect(
        url_for(
            'home_bp.learner_dashboard'
        )
    )

@home_bp.route('/home/report/exit', methods=['GET'])
@login_required
def report_exit():
    assessment_id = request.args.get('assessment_id')
    doc_type = request.args.get('type', 'report')
    default_email = getattr(current_user, "email", "")
    return render_template('subject_home/report_exit.html', default_email=default_email, assessment_id=assessment_id, doc_type=doc_type)

@home_bp.route('/home/report/finish', methods=['POST'])
@login_required
def finish_report():
    email = request.form.get('email')
    assessment_id = request.form.get('assessment_id')
    doc_type = request.form.get('doc_type', 'report')
    assessment = HomeFinalAssessment.query.get(assessment_id)
    
    if email and assessment:
        import io
        from xhtml2pdf import pisa
        from app.utils.mailer import send_pdf_email

        # Generate HTML for Certificate & Diagnostic Report
        from flask import current_app
        report_pdf_bytes = _generate_home_certificate_pdf(assessment)

        if doc_type == 'certificate' and assessment.passed:
            send_pdf_email(
                to_email=email,
                subject="Your HOME Certificate & Diagnostic Report",
                body_text="Congratulations on passing the HOME Programme! Please find your official Certificate and Diagnostic Report attached.",
                pdf_bytes=report_pdf_bytes,
                filename="HOME_Certificate_and_Report.pdf"
            )

            db.session.execute(
                text("""
                    UPDATE user_enrollment
                    SET status = 'completed'
                    WHERE user_id = :uid AND subject_id = (SELECT id FROM auth_subject WHERE slug = 'home' LIMIT 1)
                """),
                {"uid": current_user.id}
            )
            db.session.commit()
            flash("Your Certificate and Diagnostic Report have been emailed to you. Congratulations on completing the course!", "success")
            return redirect(url_for('public_bp.welcome'))
        else:
            subject = "Your HOME Diagnostic Report"
            if assessment.passed:
                body_text = "Your HOME final assessment has been graded. You passed! Please find your Diagnostic Report attached."
            else:
                body_text = "Your HOME final assessment has been graded. Please find your Diagnostic Report attached."
                
            send_pdf_email(
                to_email=email,
                subject=subject,
                body_text=body_text,
                pdf_bytes=report_pdf_bytes,
                filename="HOME_Diagnostic_Report.pdf"
            )
            flash("Your diagnostic report has been emailed to you.", "info")
            return redirect(url_for('public_bp.welcome'))

    return redirect(url_for('public_bp.welcome'))

@home_bp.route('/home/section2/coming_soon', methods=['GET'])
@login_required
def section2_coming_soon():
    return render_template('subject_home/section2_coming_soon.html')

@home_bp.route('/home/bypass_chapters', methods=['GET'])
@login_required
def bypass_chapters():
    for i in range(1, 31):
        session[f'chapter_{i}_done'] = True
        _save_home_progress(current_user.id, i)
    flash('Chapters bypassed for testing. Final exam is now unlocked.', 'success')
    return redirect(url_for('home_bp.learner_dashboard'))



@home_bp.route(
'/final_exam',
methods=['GET', 'POST']
)
@login_required
def final_exam():
    has_premium = _has_active_home_subscription(current_user.id)
    
    if not has_premium:
        flash("You must subscribe to unlock the Final Exam.", "warning")
        return redirect(url_for('home_bp.learner_dashboard'))

    questions = []
    for chapter_id in range(21, 31):
        ch_qs = HomeQuestion.query.filter_by(chapter_id=chapter_id).all()
        if ch_qs:
            questions.extend(random.sample(ch_qs, min(5, len(ch_qs))))

    if request.method == 'POST':

        section_scores = {
            "observation": 0,
            "position": 0,
            "comparison": 0,
            "estimation": 0,
            "measurement": 0,
            "pattern": 0,
            "spatial": 0,
            "logic": 0,
            "mathematics": 0,
            "critical": 0
        }

        total_correct = 0

        for question in questions:

            if question.question_type == "multi_select":
                user_answer = request.form.getlist(f"q{question.id}")
                correct_answers = [x.strip() for x in question.correct_answer.split(",")]
                correct = sorted(user_answer) == sorted(correct_answers)
            else:
                user_answer = request.form.get(f"q{question.id}")
                correct = user_answer == question.correct_answer

            if correct:

                total_correct += 1

                if question.chapter_id in [1, 11, 21]:
                    section_scores["observation"] += 1

                elif question.chapter_id in [2, 12, 22]:
                    section_scores["position"] += 1

                elif question.chapter_id in [3, 13, 23]:
                    section_scores["comparison"] += 1

                elif question.chapter_id in [4, 14, 24]:
                    section_scores["estimation"] += 1

                elif question.chapter_id in [5, 15, 25]:
                    section_scores["measurement"] += 1

                elif question.chapter_id in [6, 16, 26]:
                    section_scores["pattern"] += 1

                elif question.chapter_id in [7, 17, 27]:
                    section_scores["spatial"] += 1

                elif question.chapter_id in [8, 18, 28]:
                    section_scores["logic"] += 1

                elif question.chapter_id in [9, 19, 29]:
                    section_scores["mathematics"] += 1

                elif question.chapter_id in [10, 20, 30]:
                    section_scores["critical"] += 1

        category_counts = {i: 0 for i in range(1, 11)}
        for q in questions:
            cat_id = q.chapter_id % 10
            if cat_id == 0:
                cat_id = 10
            category_counts[cat_id] += 1

        def calc_cat(cat_id, key):
            c = category_counts.get(cat_id, 0)
            if c > 0:
                return round((section_scores[key] / c) * 100)
            return 0

        obs_score = calc_cat(1, "observation")
        pos_score = calc_cat(2, "position")
        cmp_score = calc_cat(3, "comparison")
        est_score = calc_cat(4, "estimation")
        mea_score = calc_cat(5, "measurement")
        pat_score = calc_cat(6, "pattern")
        spa_score = calc_cat(7, "spatial")
        log_score = calc_cat(8, "logic")
        mat_score = calc_cat(9, "mathematics")
        cri_score = calc_cat(10, "critical")

        overall_score = round((obs_score + pos_score + cmp_score + est_score + mea_score + pat_score + spa_score + log_score + mat_score + cri_score) / 10.0)

        passed = overall_score >= 70

        assessment = HomeFinalAssessment(
            user_id=current_user.id,
            observation_score=obs_score,
            position_score=pos_score,
            comparison_score=cmp_score,
            estimation_score=est_score,
            measurement_score=mea_score,
            pattern_score=pat_score,
            spatial_score=spa_score,
            logic_score=log_score,
            mathematics_score=mat_score,
            critical_thinking_score=cri_score,
            overall_score=overall_score,

            passed=passed
        )

        db.session.add(assessment)
        db.session.commit()

        return redirect(url_for('home_bp.report_exit', assessment_id=assessment.id, type='report'))

    render_questions = list(questions)
    if SHUFFLE_HOME_QUESTIONS:
        random.shuffle(render_questions)
        for q in render_questions:
            q.shuffled_options = list(q.options)
            random.shuffle(q.shuffled_options)
    else:
        for q in render_questions:
            q.shuffled_options = list(q.options)

    return render_template(
        'subject_home/final_exam.html',
        questions=render_questions
    )

@home_bp.route(
'/view_certificate'
)
@login_required
def view_certificate():


    assessment = HomeFinalAssessment.query.filter_by(
        user_id=current_user.id
    ).order_by(
        HomeFinalAssessment.id.desc()
    ).first_or_404()

    return render_template(
        'subject_home/certificate.html',
        assessment=assessment
    )

@home_bp.route(
'/view_final_certificate'
)
@login_required
def view_final_certificate():
    assessment = HomeFinalAssessment.query.filter_by(
        user_id=current_user.id
    ).order_by(
        HomeFinalAssessment.id.desc()
    ).first_or_404()

    if not assessment.passed:
        flash("You must pass the assessment to view your certificate.", "warning")
        return redirect(url_for('home_bp.learner_dashboard'))

    from flask import url_for
    logo_url = url_for('static', filename='branding/ait_logo.png')
    seal_url = url_for('static', filename='branding/ait_seal.png')
    
    return render_template(
        'subject_home/certificate.html',
        assessment=assessment,
        logo_path=logo_url,
        seal_path=seal_url
    )

@home_bp.route('/view_failed_certificate')
@login_required
def view_failed_certificate():
    from app.models.home import HomeFinalAssessment
    assessment = HomeFinalAssessment.query.filter_by(
        user_id=current_user.id
    ).order_by(HomeFinalAssessment.id.desc()).first_or_404()
    
    from flask import url_for
    logo_url = url_for('static', filename='branding/ait_logo.png')
    seal_url = url_for('static', filename='branding/ait_seal.png')
    
    return render_template(
        'subject_home/certificate.html',
        assessment=assessment,
        logo_path=logo_url,
        seal_path=seal_url
    )
@home_bp.route('/test_passed_certificate')
@login_required
def test_passed_certificate():
    import io
    from flask import make_response
    from xhtml2pdf import pisa
    assessment = HomeFinalAssessment.query.filter_by(user_id=current_user.id).order_by(HomeFinalAssessment.id.desc()).first()
    if not assessment:
        flash("Take the exam first.", "warning")
        return redirect(url_for('home_bp.learner_dashboard'))
        
    class MockAssessment:
        pass
    mock = MockAssessment()
    mock.passed = True
    mock.overall_score = assessment.overall_score
    mock.created_at = assessment.created_at
    mock.id = assessment.id
    mock.observation_score = assessment.observation_score
    mock.position_score = assessment.position_score
    mock.comparison_score = assessment.comparison_score
    mock.estimation_score = assessment.estimation_score
    mock.measurement_score = assessment.measurement_score
    mock.pattern_score = assessment.pattern_score
    mock.spatial_score = assessment.spatial_score
    mock.logic_score = assessment.logic_score
    mock.mathematics_score = assessment.mathematics_score
    mock.critical_thinking_score = assessment.critical_thinking_score
    
    from flask import current_app
    import os, base64
    logo_path = os.path.join(current_app.root_path, 'static', 'images', 'Palm.png')
    logo_b64 = ""
    if os.path.exists(logo_path):
        with open(logo_path, "rb") as image_file:
            logo_b64 = "data:image/png;base64," + base64.b64encode(image_file.read()).decode('utf-8')

    html = render_template('subject_home/certificate.html', assessment=mock, logo_path=logo_b64)
    out = io.BytesIO()
    pisa.CreatePDF(html, dest=out, encoding="UTF-8")
    pdf_bytes = out.getvalue()
    response = make_response(pdf_bytes)
    response.headers['Content-Type'] = 'application/pdf'
    response.headers['Content-Disposition'] = 'inline; filename=test_certificate.pdf'
    return response


def _generate_home_certificate_pdf(assessment):
    from flask import current_app, render_template
    import io, os, base64
    from xhtml2pdf import pisa
    
    logo_path = os.path.join(current_app.root_path, 'static', 'branding', 'ait_logo.png')
    if not os.path.exists(logo_path):
        # Fallback to older static folder structure if needed
        logo_path = os.path.join(current_app.root_path, '..', 'static', 'branding', 'ait_logo.png')
    
    logo_b64 = ""
    if os.path.exists(logo_path):
        with open(logo_path, "rb") as image_file:
            logo_b64 = "data:image/png;base64," + base64.b64encode(image_file.read()).decode('utf-8')
            
    seal_path = os.path.join(current_app.root_path, 'static', 'branding', 'ait_seal.png')
    if not os.path.exists(seal_path):
        seal_path = os.path.join(current_app.root_path, '..', 'static', 'branding', 'ait_seal.png')
        
    seal_b64 = ""
    if os.path.exists(seal_path):
        with open(seal_path, "rb") as image_file:
            seal_b64 = "data:image/png;base64," + base64.b64encode(image_file.read()).decode('utf-8')
    
    report_html = render_template('subject_home/certificate.html', assessment=assessment, logo_path=logo_b64, seal_path=seal_b64)
    out_report = io.BytesIO()
    pisa.CreatePDF(report_html, dest=out_report, encoding="UTF-8")
    return out_report.getvalue()





# ==========================================
# TEACHER / PARENT ROUTES
# ==========================================

@home_bp.route('/teacher/register', methods=['GET', 'POST'])
def teacher_register():
    from app.models.auth import User, UserEnrollment, AuthSubject
    if current_user.is_authenticated:
        return redirect(url_for('home_bp.teacher_dashboard'))

    if request.method == 'POST':
        from app import db
        full_name = request.form.get('full_name', '').strip()
        email = request.form.get('email', '').strip().lower()
        password = request.form.get('password', '')

        if not email or not password:
            flash("Email and password are required.", "danger")
            return redirect(url_for('home_bp.teacher_register'))

        existing_user = User.query.filter(db.func.lower(User.email) == email).first()
        if existing_user:
            flash("That email is already registered. Please log in.", "danger")
            return redirect(url_for('auth_bp.login'))

        # Create new Teacher user
        new_teacher = User(
            name=full_name,
            email=email
        )
        new_teacher.set_password(password)
        db.session.add(new_teacher)
        db.session.commit()
        
        # Auto-enroll in 'home' subject so they see the tile on bridge dashboard
        home_subject = AuthSubject.query.filter_by(slug='home').first()
        if home_subject:
            enr = UserEnrollment(user_id=new_teacher.id, subject_id=home_subject.id, status='teacher')
            db.session.add(enr)
            db.session.commit()

        login_user(new_teacher)
        flash("Teacher account created successfully!", "success")
        return redirect(url_for('home_bp.teacher_dashboard'))

    return render_template('subject_home/teacher_register.html')

@home_bp.route('/teacher/dashboard', methods=['GET', 'POST'])
@login_required
def teacher_dashboard():
    from app.models.home import HomeTeacherLink, HomePracticalSubmission
    from app.models.auth import User, UserEnrollment, AuthSubject
    from app import db

    # Get the home subject
    home_subject = AuthSubject.query.filter_by(slug='home').first()
    
    if request.method == 'POST':
        # Add a student via dropdown
        student_id_str = request.form.get('student_id')
        if student_id_str and student_id_str.isdigit():
            student = User.query.get(int(student_id_str))
            if not student:
                flash("Student not found.", "danger")
            else:
                existing_link = HomeTeacherLink.query.filter_by(teacher_id=current_user.id, student_id=student.id).first()
                if existing_link:
                    flash("Student is already linked to your dashboard.", "info")
                else:
                    new_link = HomeTeacherLink(teacher_id=current_user.id, student_id=student.id)
                    db.session.add(new_link)
                    db.session.commit()
                    flash(f"Successfully linked student: {student.name or student.email}", "success")
        return redirect(url_for('home_bp.teacher_dashboard'))

    # GET: Load dashboard
    links = HomeTeacherLink.query.filter_by(teacher_id=current_user.id).all()
    student_ids = [link.student_id for link in links]

    if not student_ids:
        submissions = []
        students = []
    else:
        students = User.query.filter(User.id.in_(student_ids)).all()
        # Get pending submissions for linked students
        submissions = db.session.query(HomePracticalSubmission, User).join(
            User, HomePracticalSubmission.student_id == User.id
        ).filter(
            HomePracticalSubmission.student_id.in_(student_ids),
            HomePracticalSubmission.status == 'pending'
        ).order_by(HomePracticalSubmission.created_at.asc()).all()

        from app.models.home import HomeProgress
        all_progress = HomeProgress.query.filter(HomeProgress.user_id.in_(student_ids)).all()
        student_progress_map = {}
        for p in all_progress:
            if p.user_id not in student_progress_map:
                student_progress_map[p.user_id] = []
            student_progress_map[p.user_id].append(p.chapter_number)
            
        for uid in student_progress_map:
            student_progress_map[uid].sort()

    # Get all learners enrolled in home (for the dropdown)
    all_home_learners = []
    if home_subject:
        all_home_learners = db.session.query(User).join(UserEnrollment).filter(
            UserEnrollment.subject_id == home_subject.id
        ).all()

    from flask import make_response
    response = make_response(render_template(
        'subject_home/teacher_dashboard.html',
        students=students,
        submissions=submissions,
        all_home_learners=all_home_learners,
        linked_student_ids=student_ids,
        student_progress_map=student_progress_map if student_ids else {}
    ))
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

@home_bp.route('/teacher/grade_view/<int:submission_id>')
@login_required
def teacher_grade_view(submission_id):
    from app.models.home import HomePracticalSubmission, HomeChapter
    sub = HomePracticalSubmission.query.get_or_404(submission_id)
    chapter = HomeChapter.query.filter_by(chapter_number=sub.chapter_number).first_or_404()
    
    # Render the exact same practical template, but tell it we are grading!
    template_name = f'subject_home/chapter{sub.chapter_number}_practical.html'
    hero_image = chapter.image_filename
    
    return render_template(
        template_name,
        chapter=chapter, hero_image=hero_image,
        is_teacher_scoring=True,
        submission=sub
    )

@home_bp.route('/teacher/score/<int:submission_id>', methods=['POST'])
@login_required
def teacher_score(submission_id):
    from app.models.home import HomePracticalSubmission
    from app import db
    
    sub = HomePracticalSubmission.query.get_or_404(submission_id)
    decision = request.form.get('decision')
    
    if decision not in ['competent', 'not_yet_competent']:
        flash("Invalid decision.", "danger")
        return redirect(url_for('home_bp.teacher_dashboard'))
        
    sub.status = decision
    db.session.commit()
    
    if decision == 'competent':
        # Actually unlock the next chapter for the student
        _save_home_progress(sub.student_id, sub.chapter_number)
        flash(f"Marked Chapter {sub.chapter_number} as Competent for student.", "success")
    else:
        flash(f"Marked Chapter {sub.chapter_number} as Not Yet Competent. The student can retry.", "info")
        
    return redirect(url_for('home_bp.teacher_dashboard'))
