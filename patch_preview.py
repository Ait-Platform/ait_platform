with open("templates/admin/programs/reading/lessons.html", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace("url_for('admin_bp.preview_lesson', lesson_id=l.id)", "url_for('reading_bp.view_lesson', lesson_id=l.id)")

with open("templates/admin/programs/reading/lessons.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated preview_lesson to view_lesson")
