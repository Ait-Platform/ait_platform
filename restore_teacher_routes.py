import os

filepath = r'D:\Users\yeshk\Documents\ait_platform\app\subject_home\routes.py'

routes_content = """

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
            enr = UserEnrollment(user_id=new_teacher.id, subject_id=home_subject.id, status='active')
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

    # Get all learners enrolled in home (for the dropdown)
    all_home_learners = []
    if home_subject:
        all_home_learners = db.session.query(User).join(UserEnrollment).filter(
            UserEnrollment.subject_id == home_subject.id
        ).all()

    return render_template(
        'subject_home/teacher_dashboard.html',
        students=students,
        submissions=submissions,
        all_home_learners=all_home_learners,
        linked_student_ids=student_ids
    )

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
"""

with open(filepath, 'a', encoding='utf-8') as f:
    f.write(routes_content)

print("Routes appended!")
