import re

with open('app/admin/programs/routes.py', 'r') as f:
    content = f.read()

old = 'return render_template("admin/programs/fallback_dashboard.html", subject=subject, subject_name=subj_obj.name)'
new = 'return render_template("admin/programs/fallback_dashboard.html", subject=subject, subject_name=subj_obj.name, subj_obj=subj_obj)'
content = content.replace(old, new)

old_try = 'return render_template(f"admin/programs/{subject}/dashboard.html", subject=subject, subject_name=subj_obj.name)'
new_try = 'return render_template(f"admin/programs/{subject}/dashboard.html", subject=subject, subject_name=subj_obj.name, subj_obj=subj_obj)'
content = content.replace(old_try, new_try)

with open('app/admin/programs/routes.py', 'w') as f:
    f.write(content)
