from flask import flash, redirect, render_template, request, url_for
from app.extensions import db
from .. import admin_bp
from flask import render_template
from app.models.home import HomeChapter, HomeQuestion, HomeQuestionOption

@admin_bp.route("/home/", endpoint="home_home")
def home_home():

    chapter_count = HomeChapter.query.count()

    question_count = HomeQuestion.query.count()

    option_count = HomeQuestionOption.query.count()

    chapter_complete = chapter_count >= 10
    question_complete = question_count >= 50
    option_complete = option_count >= 150

    return render_template(
        "admin/home/index.html",
        chapter_count=chapter_count,
        question_count=question_count,
        option_count=option_count,
        chapter_complete=chapter_complete,
        question_complete=question_complete,
        option_complete=option_complete,
    )

@admin_bp.route("/chapters")
def chapters():

    chapters = (
        HomeChapter.query
        .order_by(HomeChapter.chapter_number)
        .all()
    )

    return render_template(
        "admin/home/chapters.html",
        chapters=chapters
    )

@admin_bp.route(
    "/chapters/add",
    methods=["GET", "POST"]
)
def add_chapter():

    if request.method == "POST":

        next_number = (
            db.session.query(
                db.func.max(HomeChapter.chapter_number)
            ).scalar() or 0
        ) + 1

        chapter = HomeChapter(
            chapter_number=next_number,
            title=request.form.get("title"),
            objective=request.form.get("objective"),
            image_filename=request.form.get("image_filename"),
            pass_mark=request.form.get(
                "pass_mark",
                type=int
            )
        )
        db.session.add(chapter)
        db.session.commit()

        flash(
            "Chapter created.",
            "success"
        )

        return redirect(
            url_for("admin_bp.chapters")
        )

    return render_template(
        "admin/home/chapter_form.html"
    )

@admin_bp.route(
    "/chapters/edit/<int:chapter_id>",
    methods=["GET", "POST"]
)
def edit_chapter(chapter_id):

    chapter = HomeChapter.query.get_or_404(
        chapter_id
    )

    if request.method == "POST":

        chapter.chapter_number = request.form.get(
            "chapter_number",
            type=int
        )

        chapter.title = request.form.get(
            "title"
        )

        chapter.objective = request.form.get(
            "objective"
        )

        chapter.image_filename = request.form.get(
            "image_filename"
        )

        chapter.pass_mark = request.form.get(
            "pass_mark",
            type=int
        )

        db.session.commit()

        flash(
            "Chapter updated.",
            "success"
        )

        return redirect(
            url_for("admin_bp.chapters")
        )

    return render_template(
        "admin/home/chapter_form.html",
        chapter=chapter
    )

@admin_bp.route(
    "/chapters/delete/<int:chapter_id>",
    methods=["POST"]
)
def delete_chapter(chapter_id):

    chapter = HomeChapter.query.get_or_404(
        chapter_id
    )

    db.session.delete(chapter)
    db.session.commit()

    flash(
        "Chapter deleted.",
        "success"
    )

    return redirect(
        url_for("admin_bp.chapters")
    )

@admin_bp.route("/questions")
def questions():

    questions = (
        HomeQuestion.query
        .order_by(
            HomeQuestion.chapter_id,
            HomeQuestion.id
        )
        .all()
    )

    return render_template(
        "admin/home/questions.html",
        questions=questions
    )

@admin_bp.route(
    "/questions/add",
    methods=["GET", "POST"]
)
def add_question():

    chapters = (
        HomeChapter.query
        .order_by(HomeChapter.chapter_number)
        .all()
    )

    if request.method == "POST":

        question = HomeQuestion(
            chapter_id=request.form.get(
                "chapter_id",
                type=int
            ),
            question=request.form.get(
                "question"
            ),
            question_type=request.form.get(
                "question_type"
            ),
            correct_answer=request.form.get(
                "correct_answer"
            )
        )

        db.session.add(question)
        db.session.commit()

        flash(
            "Question created.",
            "success"
        )

        return redirect(
            url_for("admin_bp.questions")
        )

    return render_template(
        "admin/home/question_form.html",
        chapters=chapters,
        question=None
    )

@admin_bp.route(
    "/questions/edit/<int:question_id>",
    methods=["GET", "POST"]
)
def edit_question(question_id):

    question = HomeQuestion.query.get_or_404(
        question_id
    )

    chapters = (
        HomeChapter.query
        .order_by(HomeChapter.chapter_number)
        .all()
    )

    if request.method == "POST":

        question.chapter_id = request.form.get(
            "chapter_id",
            type=int
        )

        question.question = request.form.get(
            "question"
        )

        question.question_type = request.form.get(
            "question_type"
        )

        question.correct_answer = request.form.get(
            "correct_answer"
        )

        db.session.commit()

        flash(
            "Question updated.",
            "success"
        )

        return redirect(
            url_for("admin_bp.questions")
        )

    return render_template(
        "admin/home/question_form.html",
        question=question,
        chapters=chapters
    )

@admin_bp.route(
    "/questions/delete/<int:question_id>",
    methods=["POST"]
)
def delete_question(question_id):

    question = HomeQuestion.query.get_or_404(
        question_id
    )

    db.session.delete(question)
    db.session.commit()

    flash(
        "Question deleted.",
        "success"
    )

    return redirect(
        url_for("admin_bp.questions")
    )

@admin_bp.route("/options")
def options():

    options = (
        HomeQuestionOption.query
        .order_by(
            HomeQuestionOption.question_id,
            HomeQuestionOption.sort_order
        )
        .all()
    )

    return render_template(
        "admin/home/options.html",
        options=options
    )

@admin_bp.route(
    "/options/add",
    methods=["GET", "POST"]
)
def add_option():

    questions = (
        HomeQuestion.query
        .order_by(HomeQuestion.id)
        .all()
    )

    if request.method == "POST":

        option = HomeQuestionOption(
            question_id=request.form.get(
                "question_id",
                type=int
            ),
            option_text=request.form.get(
                "option_text"
            ),
            sort_order=request.form.get(
                "sort_order",
                type=int
            ) or 1
        )

        db.session.add(option)
        db.session.commit()

        flash(
            "Option created.",
            "success"
        )

        return redirect(
            url_for("admin_bp.options")
        )

    return render_template(
        "admin/home/option_form.html",
        questions=questions,
        option=None
    )

@admin_bp.route(
    "/options/edit/<int:option_id>",
    methods=["GET", "POST"]
)
def edit_option(option_id):

    option = HomeQuestionOption.query.get_or_404(
        option_id
    )

    questions = (
        HomeQuestion.query
        .order_by(HomeQuestion.id)
        .all()
    )

    if request.method == "POST":

        option.question_id = request.form.get(
            "question_id",
            type=int
        )

        option.option_text = request.form.get(
            "option_text"
        )

        option.sort_order = request.form.get(
            "sort_order",
            type=int
        ) or 1

        db.session.commit()

        flash(
            "Option updated.",
            "success"
        )

        return redirect(
            url_for("admin_bp.options")
        )

    return render_template(
        "admin/home/option_form.html",
        option=option,
        questions=questions
    )

@admin_bp.route(
    "/options/delete/<int:option_id>",
    methods=["POST"]
)
def delete_option(option_id):

    option = HomeQuestionOption.query.get_or_404(
        option_id
    )

    db.session.delete(option)
    db.session.commit()

    flash(
        "Option deleted.",
        "success"
    )

    return redirect(
        url_for("admin_bp.options")
    )







