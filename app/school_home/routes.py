# routes.py
from flask import Blueprint, render_template,redirect, request, url_for, flash, session
from flask_login import login_required, current_user
from app.models.auth import db, User

home_bp = Blueprint("home_bp", __name__)
#home_bp = Blueprint('home_bp', __name__, url_prefix='/home')
#print("✅ school.home.routes loaded")

@home_bp.route('/home/about')
def home_about():
    return render_template("school_home/about.html")

@home_bp.route("/dashboard/learner")
@login_required
def learner_dashboard():
    return render_template("school_home/learner_dashboard.html", user=current_user)

@home_bp.route("/dashboard/tutor")
@login_required
def tutor_dashboard():
    if current_user.role != "home_tutor":
        flash("Access denied", "danger")
        return redirect(url_for("public_bp.welcome"))
    return render_template("school_home/tutor_dashboard.html")

@home_bp.route('/home/chapter/1')
def chapter_1():
    return render_template('school_home/chapter_1.html')

@home_bp.route('/test-mark')
def test_mark_route():
    return "✅ mark_section_done route is visible"

@home_bp.route('/mark_section_done/<int:section>', methods=['POST'])
def mark_section_done(section):
    print(f"📘 Section {section} marked done")
    session[f'section_{section}_done'] = True  # Example logic — store in session
    return redirect(url_for('home_bp.home_learner_dashboard'))

@home_bp.route('/test/<int:test_number>')
def test_page(test_number):
    if 'user_id' not in session:
        flash("Please log in to access tests.", "warning")
        return redirect(url_for('login'))
    return render_template(f'school_home/test_{test_number}.html')

@home_bp.route('/submit_test_<int:test_number>', methods=['POST'])
def submit_test(test_number):
    if 'user_id' not in session:
        flash("Please log in to submit your test.", "warning")
        return redirect(url_for('login'))

    user = User.query.get(session['user_id'])
    question1 = request.form.get('question1', '').strip()
    score = 100 if question1 == '4' else 0
    test_name = f"Test {test_number}"

    #existing = TestResult.query.filter_by(user_id=user.id, test_name=test_name).first()
    #if existing:
        #flash(f"You have already submitted {test_name}.", "info")
        #return redirect(url_for('home_bp.home_dashboard'))

    #result = TestResult(user_id=user.id, test_name=test_name, score=score)
    #db.session.add(result)
    #db.session.commit()

    flash(f"{test_name} submitted successfully. Your score: {score}%", "success")
    return redirect(url_for('home_bp.learner_dashboard'))

@home_bp.route('/final_exam')
def final_exam():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    return render_template('school_home/final_exam.html')

@home_bp.route('/view_certificate')
def view_certificate():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    return render_template('school_home/certificate.html')

@home_bp.route('/view_diagnostic')
def view_diagnostic():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    return render_template('school_home/view_diagnostic.html')
