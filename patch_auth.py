with open("app/admin/programs/lessons.py", "r", encoding="utf-8") as f:
    text = f.read()

# Remove the explicit checks since admin_bp.before_request already does this perfectly
old_check = """def audit_video_disk():
    if not (session.get("is_admin") or session.get("role") == "admin"):
        abort(403)"""
new_check = """def audit_video_disk():"""
text = text.replace(old_check, new_check)

old_check2 = """def download_disk_file():
    if not (session.get("is_admin") or session.get("role") == "admin"):
        abort(403)"""
new_check2 = """def download_disk_file():"""
text = text.replace(old_check2, new_check2)

with open("app/admin/programs/lessons.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Removed redundant auth checks")
