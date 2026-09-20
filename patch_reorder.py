with open("templates/admin/programs/reading/reorder.html", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace("url_for('admin_bp.reading_lessons')", "url_for('admin_bp.lessons', subject=subject)")

with open("templates/admin/programs/reading/reorder.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Fixed reading_lessons endpoint in reorder.html")
