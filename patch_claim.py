import re
with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_block = """        if existing_claim and request.method == "POST":
            flash("You already have a pending request. If you made a mistake, please click the 'Unsure / Other' tile.", "warning")
            return redirect(url_for("uip_bp.router_page", org_slug=org.slug))

        if request.method == "GET":"""

new_block = """        if existing_claim and request.method == "POST":
            # EXCEPT FOR GENESIS SECRETARY
            position = request.form.get("position", "").strip().lower()
            if position == "secretary":
                # Let it proceed below to the Genesis logic. We will let the Genesis flow handle it.
                pass
            else:
                flash("You already have a pending request. If you made a mistake, please click the 'Unsure / Other' tile.", "warning")
                return redirect(url_for("uip_bp.router_page", org_slug=org.slug))

        if request.method == "GET":"""

text = text.replace(old_block, new_block)
with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated existing claim logic")
