from flask import flash, redirect, render_template, request, url_for, abort
from app.extensions import db
from app.admin import admin_bp
from app.models.home import HomeChapter, HomeQuestion, HomeQuestionOption

@admin_bp.route("/<subject>/questions", endpoint="manage_questions")
def manage_questions(subject):
    subject = subject.lower().strip()
    if subject == "home":
        questions = HomeQuestion.query.order_by(HomeQuestion.chapter_id, HomeQuestion.id).all()
        return render_template("admin/programs/home/questions.html", questions=questions, subject=subject)
    abort(404)

@admin_bp.route("/<subject>/questions/add", methods=["GET", "POST"], endpoint="add_question")
def add_question(subject):
    subject = subject.lower().strip()
    if subject != "home":
        abort(404)
        
    chapters = HomeChapter.query.order_by(HomeChapter.chapter_number).all()
    if request.method == "POST":
        question = HomeQuestion(
            chapter_id=request.form.get("chapter_id", type=int),
            question=request.form.get("question"),
            question_type=request.form.get("question_type"),
            correct_answer=request.form.get("correct_answer")
        )
        db.session.add(question)
        db.session.commit()
        flash("Question created.", "success")
        return redirect(url_for("admin_bp.manage_questions", subject=subject))
        
    return render_template("admin/programs/home/question_form.html", chapters=chapters, question=None, subject=subject)

@admin_bp.route("/<subject>/questions/edit/<int:question_id>", methods=["GET", "POST"], endpoint="edit_question")
def edit_question(subject, question_id):
    subject = subject.lower().strip()
    if subject != "home":
        abort(404)
        
    question = HomeQuestion.query.get_or_404(question_id)
    chapters = HomeChapter.query.order_by(HomeChapter.chapter_number).all()
    
    if request.method == "POST":
        question.chapter_id = request.form.get("chapter_id", type=int)
        question.question = request.form.get("question")
        question.question_type = request.form.get("question_type")
        question.correct_answer = request.form.get("correct_answer")
        db.session.commit()
        flash("Question updated.", "success")
        return redirect(url_for("admin_bp.manage_questions", subject=subject))
        
    return render_template("admin/programs/home/question_form.html", question=question, chapters=chapters, subject=subject)

@admin_bp.route("/<subject>/questions/delete/<int:question_id>", methods=["POST"], endpoint="delete_question")
def delete_question(subject, question_id):
    subject = subject.lower().strip()
    if subject != "home":
        abort(404)
        
    question = HomeQuestion.query.get_or_404(question_id)
    db.session.delete(question)
    db.session.commit()
    flash("Question deleted.", "success")
    return redirect(url_for("admin_bp.manage_questions", subject=subject))

@admin_bp.route("/<subject>/options", endpoint="manage_options")
def manage_options(subject):
    subject = subject.lower().strip()
    if subject == "home":
        options = HomeQuestionOption.query.order_by(HomeQuestionOption.question_id, HomeQuestionOption.sort_order).all()
        return render_template("admin/programs/home/options.html", options=options, subject=subject)
    abort(404)

@admin_bp.route("/<subject>/options/add", methods=["GET", "POST"], endpoint="add_option")
def add_option(subject):
    subject = subject.lower().strip()
    if subject != "home":
        abort(404)
        
    questions = HomeQuestion.query.order_by(HomeQuestion.id).all()
    if request.method == "POST":
        option = HomeQuestionOption(
            question_id=request.form.get("question_id", type=int),
            option_text=request.form.get("option_text"),
            sort_order=request.form.get("sort_order", type=int) or 1
        )
        db.session.add(option)
        db.session.commit()
        flash("Option created.", "success")
        return redirect(url_for("admin_bp.manage_options", subject=subject))
        
    return render_template("admin/programs/home/option_form.html", questions=questions, option=None, subject=subject)

@admin_bp.route("/<subject>/options/edit/<int:option_id>", methods=["GET", "POST"], endpoint="edit_option")
def edit_option(subject, option_id):
    subject = subject.lower().strip()
    if subject != "home":
        abort(404)
        
    option = HomeQuestionOption.query.get_or_404(option_id)
    questions = HomeQuestion.query.order_by(HomeQuestion.id).all()
    if request.method == "POST":
        option.question_id = request.form.get("question_id", type=int)
        option.option_text = request.form.get("option_text")
        option.sort_order = request.form.get("sort_order", type=int) or 1
        db.session.commit()
        flash("Option updated.", "success")
        return redirect(url_for("admin_bp.manage_options", subject=subject))
        
    return render_template("admin/programs/home/option_form.html", option=option, questions=questions, subject=subject)

@admin_bp.route("/<subject>/options/delete/<int:option_id>", methods=["POST"], endpoint="delete_option")
def delete_option(subject, option_id):
    subject = subject.lower().strip()
    if subject != "home":
        abort(404)
        
    option = HomeQuestionOption.query.get_or_404(option_id)
    db.session.delete(option)
    db.session.commit()
    flash("Option deleted.", "success")
    return redirect(url_for("admin_bp.manage_options", subject=subject))
