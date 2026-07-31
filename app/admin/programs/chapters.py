from flask import flash, redirect, render_template, request, url_for, abort
from app.extensions import db
from app.admin import admin_bp
from app.models.home import HomeChapter

@admin_bp.route("/<subject>/chapters", endpoint="manage_chapters")
def manage_chapters(subject):
    subject = subject.lower().strip()
    if subject == "home":
        chapters = HomeChapter.query.order_by(HomeChapter.chapter_number).all()
        return render_template("admin/programs/home/chapters.html", chapters=chapters, subject=subject)
    abort(404)

@admin_bp.route("/<subject>/chapters/add", methods=["GET", "POST"], endpoint="add_chapter")
def add_chapter(subject):
    subject = subject.lower().strip()
    if subject != "home":
        abort(404)
        
    if request.method == "POST":
        next_number = (db.session.query(db.func.max(HomeChapter.chapter_number)).scalar() or 0) + 1
        chapter = HomeChapter(
            chapter_number=next_number,
            title=request.form.get("title"),
            objective=request.form.get("objective"),
            image_filename=request.form.get("image_filename"),
            pass_mark=request.form.get("pass_mark", type=int)
        )
        db.session.add(chapter)
        db.session.commit()
        flash("Chapter created.", "success")
        return redirect(url_for("admin_bp.manage_chapters", subject=subject))
        
    return render_template("admin/programs/home/chapter_form.html", subject=subject, chapter=None)

@admin_bp.route("/<subject>/chapters/edit/<int:chapter_id>", methods=["GET", "POST"], endpoint="edit_chapter")
def edit_chapter(subject, chapter_id):
    subject = subject.lower().strip()
    if subject != "home":
        abort(404)
        
    chapter = HomeChapter.query.get_or_404(chapter_id)
    
    if request.method == "POST":
        chapter.chapter_number = request.form.get("chapter_number", type=int)
        chapter.title = request.form.get("title")
        chapter.objective = request.form.get("objective")
        chapter.image_filename = request.form.get("image_filename")
        chapter.pass_mark = request.form.get("pass_mark", type=int)
        db.session.commit()
        flash("Chapter updated.", "success")
        return redirect(url_for("admin_bp.manage_chapters", subject=subject))
        
    return render_template("admin/programs/home/chapter_form.html", chapter=chapter, subject=subject)

@admin_bp.route("/<subject>/chapters/delete/<int:chapter_id>", methods=["POST"], endpoint="delete_chapter")
def delete_chapter(subject, chapter_id):
    subject = subject.lower().strip()
    if subject != "home":
        abort(404)
        
    chapter = HomeChapter.query.get_or_404(chapter_id)
    db.session.delete(chapter)
    db.session.commit()
    flash("Chapter deleted.", "success")
    return redirect(url_for("admin_bp.manage_chapters", subject=subject))
