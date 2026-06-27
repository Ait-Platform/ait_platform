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
    from app.models.payment import RefCountryCurrency
    from app.enrollment.logic import get_quote_for_subject_country
    
    subject = AuthSubject.query.filter(db.func.lower(AuthSubject.slug) == 'home').first()
    if not subject:
        flash("Subject not found.", "warning")
        return redirect(url_for('public_bp.welcome'))

    country_code = (request.args.get("country") or "").strip().upper()
    if not country_code and current_user.is_authenticated and hasattr(current_user, 'country_code'):
        country_code = (current_user.country_code or "").strip().upper()

    if not country_code:
        country_code = session.get("country_code", "")

    if country_code:
        session["country_code"] = country_code

    price_ctx = {
        "has_quote": False,
        "price_id": None,
        "country_code": None,
        "local_amount": None,
        "local_currency": None,
        "estimated_zar": None,
        "fx_rate": None,
        "is_discount": False,
    }

    if country_code:
        row = get_quote_for_subject_country(subject.id, country_code)
        if row:
            price_ctx.update({
                "price_id": row.id,
                "country_code": row.country_code,
                "local_amount": row.local_amount_cents,
                "local_currency": row.local_currency,
                "estimated_zar": row.zar_amount_cents,
                "fx_rate": getattr(row, "fx_rate", None),
                "is_discount": getattr(row, "is_discount", False),
            })
            price_ctx["has_quote"] = True
        else:
            flash("No pricing found for that country yet.", "warning")

    countries = db.session.execute(
        text("""
            SELECT r.alpha2 AS code, r.name
              FROM ref_country_currency r
             WHERE (r.is_active IS NULL OR r.is_active::text IN ('1','t','true','TRUE'))
             ORDER BY r.name
        """)
    ).mappings().all()

    return render_template(
        "subject_home/price.html",
        subject=subject,
        countries=countries,
        price=price_ctx,
        country_code=country_code
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

    # Fetch all teachers enrolled in HOME
    from app.models.auth import UserEnrollment, AuthSubject
    home_subject = AuthSubject.query.filter_by(slug='home').first()
    all_home_teachers = []
    if home_subject:
        teacher_enrollments = UserEnrollment.query.filter_by(subject_id=home_subject.id, status='teacher').all()
        teacher_ids = [e.user_id for e in teacher_enrollments]
        if teacher_ids:
            all_home_teachers = User.query.filter(User.id.in_(teacher_ids)).all()

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

    if chapter_num >= 11:
        if not _has_active_home_subscription(current_user.id):
            flash("You must subscribe to unlock the rest of the HOME course.", "warning")
            return redirect(url_for('yoco_bp.yoco_start', subject='home', email=current_user.email))

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
            chapter=chapter
        )
    elif chapter_num in [21, 22, 23, 24, 25, 26, 27, 28, 29, 30]:
        return render_template(
            f'subject_home/chapter{chapter_num}_theory.html',
            chapter=chapter,
            questions=render_questions
        )

    return render_template(
        'subject_home/chapter_db.html',
        chapter=chapter,
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
        return redirect(url_for("yoco_bp.yoco_start"))

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
        import os, base64
        logo_path = os.path.join(current_app.root_path, 'static', 'images', 'Palm.png')
        logo_b64 = ""
        if os.path.exists(logo_path):
            with open(logo_path, "rb") as image_file:
                logo_b64 = "data:image/png;base64," + base64.b64encode(image_file.read()).decode('utf-8')
        
        report_html = render_template('subject_home/certificate.html', assessment=assessment, logo_b64=logo_b64)
        out_report = io.BytesIO()
        pisa.CreatePDF(report_html, dest=out_report, encoding="UTF-8")
        report_pdf_bytes = out_report.getvalue()

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
            return redirect(url_for('auth_bp.bridge_dashboard'))
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
            return redirect(url_for('home_bp.learner_dashboard'))

    return redirect(url_for('home_bp.learner_dashboard'))

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

@home_bp.route('/home/reset_18_20', methods=['GET'])
@login_required
def reset_18_20():
    session.pop('chapter_18_done', None)
    session.pop('chapter_20_done', None)
    HomeProgress.query.filter_by(user_id=current_user.id, chapter_number=18).delete()
    HomeProgress.query.filter_by(user_id=current_user.id, chapter_number=20).delete()
    db.session.commit()
    flash('Progress for Chapters 18 and 20 has been reset. You may now retake them.', 'success')
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

    questions = HomeQuestion.query.order_by(
        HomeQuestion.id
    ).all()

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

                if question.chapter_id == 1:
                    section_scores["observation"] += 1

                elif question.chapter_id == 2:
                    section_scores["position"] += 1

                elif question.chapter_id == 3:
                    section_scores["comparison"] += 1

                elif question.chapter_id == 4:
                    section_scores["estimation"] += 1

                elif question.chapter_id == 5:
                    section_scores["measurement"] += 1

                elif question.chapter_id == 6:
                    section_scores["pattern"] += 1

                elif question.chapter_id == 7:
                    section_scores["spatial"] += 1

                elif question.chapter_id == 8:
                    section_scores["logic"] += 1

                elif question.chapter_id == 9:
                    section_scores["mathematics"] += 1

                elif question.chapter_id == 10:
                    section_scores["critical"] += 1

        overall_score = round(
            (total_correct / 50) * 100
        )

        passed = overall_score >= 70

        assessment = HomeFinalAssessment(

            user_id=current_user.id,

            observation_score=section_scores["observation"] * 20,

            position_score=section_scores["position"] * 20,

            comparison_score=section_scores["comparison"] * 20,

            estimation_score=section_scores["estimation"] * 20,

            measurement_score=section_scores["measurement"] * 20,

            pattern_score=section_scores["pattern"] * 20,

            spatial_score=section_scores["spatial"] * 20,

            logic_score=section_scores["logic"] * 20,

            mathematics_score=section_scores["mathematics"] * 20,

            critical_thinking_score=section_scores["critical"] * 20,

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

    return render_template(
        'subject_home/final_certificate.html',
        assessment=assessment
    )

@home_bp.route('/view_failed_certificate')
@login_required
def view_failed_certificate():
    return render_template('subject_home/failed_certificate.html')

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

    html = render_template('subject_home/certificate.html', assessment=mock, logo_b64=logo_b64)
    out = io.BytesIO()
    pisa.CreatePDF(html, dest=out, encoding="UTF-8")
    pdf_bytes = out.getvalue()
    response = make_response(pdf_bytes)
    response.headers['Content-Type'] = 'application/pdf'
    response.headers['Content-Disposition'] = 'inline; filename=test_certificate.pdf'
    return response








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
    
    return render_template(
        template_name,
        chapter=chapter,
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
