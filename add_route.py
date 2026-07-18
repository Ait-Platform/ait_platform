with open('app/admin/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

new_route = """
@admin_bp.route("/modules_control", methods=["GET", "POST"], endpoint="modules_control")
def modules_control():
    if request.method == "POST":
        updates = []
        for k, v in request.form.items():
            if k.startswith('visibility_') or k.startswith('yoco_mode_'):
                updates.append((k, v))
        for key, val in updates:
            db.session.execute(text("INSERT INTO system_settings (key, value) VALUES (:k, :v) ON CONFLICT(key) DO UPDATE SET value=excluded.value"), {"k": key, "v": val})
        db.session.commit()
        flash("Module controls updated successfully", "success")
        return redirect(url_for("admin_bp.modules_control"))
        
    settings = db.session.execute(text("SELECT key, value FROM system_settings")).fetchall()
    settings_dict = {s.key: s.value for s in settings}
    return render_template("admin/modules_control.html", settings=settings_dict)
"""

content += "\n" + new_route

with open('app/admin/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
