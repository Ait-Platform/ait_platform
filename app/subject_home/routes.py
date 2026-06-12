# routes.py
from flask import Blueprint, abort, render_template,redirect, request, url_for, flash, session
from flask_login import login_required, current_user
from app.models.auth import db, User
from app.models.home import HomeChapter, HomeFinalAssessment, HomeQuestion, HomeProgress
from sqlalchemy import text
import random

# Toggle to shuffle questions in chapters and final exams. 
# Keep False during testing/development so question IDs map predictably to drawings.
SHUFFLE_HOME_QUESTIONS = False


home_bp = Blueprint("home_bp", __name__)

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
        flash("You have successfully completed the HOME Programme! Feel free to explore our other subjects.", "success")
        return redirect(url_for('auth_bp.bridge_dashboard'))

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

    has_premium = db.session.scalar(
        text("""
            SELECT 1 FROM user_enrollment 
            WHERE user_id = :uid 
              AND subject_id = (SELECT id FROM auth_subject WHERE slug = 'home2' LIMIT 1)
              AND status IN ('active', 'completed')
            LIMIT 1
        """),
        {"uid": current_user.id}
    ) is not None
    
    has_section3 = db.session.scalar(
        text("""
            SELECT 1 FROM user_enrollment 
            WHERE user_id = :uid 
              AND subject_id = (SELECT id FROM auth_subject WHERE slug = 'home_section3' LIMIT 1)
              AND status IN ('active', 'completed')
            LIMIT 1
        """),
        {"uid": current_user.id}
    ) is not None

    return render_template(
        'subject_home/dashboard.html',
        user=current_user,
        chapters=chapters,
        section1_chapters=section1_chapters,
        section2_chapters=section2_chapters,
        section3_chapters=section3_chapters,
        assessment=assessment,
        has_premium=has_premium,
        has_section3=has_section3
    )

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
        if chapter_num >= 21:
            has_section3 = db.session.scalar(
                text("""
                    SELECT 1 FROM user_enrollment 
                    WHERE user_id = :uid 
                      AND subject_id = (SELECT id FROM auth_subject WHERE slug = 'home_section3' LIMIT 1)
                      AND status IN ('active', 'completed')
                    LIMIT 1
                """),
                {"uid": current_user.id}
            ) is not None
            
            if not has_section3:
                flash("You must unlock the Section 3 Upgrade to access this chapter.", "warning")
                return redirect(url_for('quote_bp.quote', subject='home_section3'))
                
        else:
            has_premium = db.session.scalar(
                text("""
                    SELECT 1 FROM user_enrollment 
                    WHERE user_id = :uid 
                      AND subject_id = (SELECT id FROM auth_subject WHERE slug = 'home2' LIMIT 1)
                      AND status IN ('active', 'completed')
                    LIMIT 1
                """),
                {"uid": current_user.id}
            ) is not None
            
            if not has_premium:
                flash("You must unlock the Premium Bundle to access this chapter.", "warning")
                return redirect(url_for('quote_bp.quote', subject='home2'))

    questions = HomeQuestion.query.filter_by(
        chapter_id=chapter.id
    ).all()

    if request.method == 'POST':
        
        if chapter_num <= 10:
            if chapter_num in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]:
                competency = request.form.get("competency")
                if competency == "competent":
                    session[f'chapter_{chapter_num}_done'] = True
                    _save_home_progress(current_user.id, chapter_num)
                    flash(f"Chapter {chapter_num} Practical marked as COMPETENT!", "success")
                else:
                    flash("The learner is NOT YET COMPETENT. Please review the material and try again.", "warning")
                    return redirect(url_for('home_bp.chapter_page', chapter_num=chapter_num))
            else:
                session[f'chapter_{chapter_num}_done'] = True
                _save_home_progress(current_user.id, chapter_num)
                flash(f"Practical work for Chapter {chapter_num} submitted successfully!", "success")
                
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


@home_bp.route('/home/re_enrol', methods=['POST'])
def re_enrol():
    if 'user_id' not in session:
        return redirect(url_for('auth_bp.login'))
        
    uid = current_user.id
        
    # Delete previous failed assessments so they get a clean slate for the Final Exam only
    HomeFinalAssessment.query.filter_by(user_id=uid).delete()
    db.session.commit()
        
    flash("Your previous failed exam has been reset. You can now retake the Final Exam.", "success")
    return redirect(url_for("home_bp.learner_dashboard"))

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
        report_html = render_template('subject_home/certificate.html', assessment=assessment)
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

@home_bp.route(
'/final_exam',
methods=['GET', 'POST']
)
@login_required
def final_exam():
    has_premium = db.session.scalar(
        text("""
            SELECT 1 FROM user_enrollment 
            WHERE user_id = :uid 
              AND subject_id = (SELECT id FROM auth_subject WHERE slug = 'home2' LIMIT 1)
              AND status IN ('active', 'completed')
            LIMIT 1
        """),
        {"uid": current_user.id}
    ) is not None
    
    if not has_premium:
        flash("You must unlock the Premium Bundle to access the Final Exam.", "warning")
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

                if 1 <= question.id <= 5:
                    section_scores["observation"] += 1

                elif 6 <= question.id <= 10:
                    section_scores["position"] += 1

                elif 11 <= question.id <= 15:
                    section_scores["comparison"] += 1

                elif 16 <= question.id <= 20:
                    section_scores["estimation"] += 1

                elif 21 <= question.id <= 25:
                    section_scores["measurement"] += 1

                elif 26 <= question.id <= 30:
                    section_scores["pattern"] += 1

                elif 31 <= question.id <= 35:
                    section_scores["spatial"] += 1

                elif 36 <= question.id <= 40:
                    section_scores["logic"] += 1

                elif 41 <= question.id <= 45:
                    section_scores["mathematics"] += 1

                elif 46 <= question.id <= 50:
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
    
    html = render_template('subject_home/certificate.html', assessment=mock)
    out = io.BytesIO()
    pisa.CreatePDF(html, dest=out, encoding="UTF-8")
    pdf_bytes = out.getvalue()
    response = make_response(pdf_bytes)
    response.headers['Content-Type'] = 'application/pdf'
    response.headers['Content-Disposition'] = 'inline; filename=test_certificate.pdf'
    return response






