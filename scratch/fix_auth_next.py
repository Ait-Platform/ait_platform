import re

routes_path = 'app/auth/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    text = f.read()

old_logic = '''        # If the subject is completely free, bypass payment
        if subj_obj.commercial_mode == "free":
            ue = db.session.get(UserEnrollment, enrollment_id)
            if ue:
                ue.status = "active"
                db.session.commit()
            if subj_obj.slug in ["cultural_fire", "culturalfire"]:
                return redirect(url_for("cultural_bp.cultural_fire_router"))
            return redirect(url_for("auth_bp.bridge_dashboard"))'''

new_logic = '''        # If the subject is completely free, bypass payment
        if subj_obj.commercial_mode == "free":
            ue = db.session.get(UserEnrollment, enrollment_id)
            if ue:
                ue.status = "active"
                db.session.commit()
            if subj_obj.slug in ["cultural_fire", "culturalfire"]:
                return redirect(url_for("cultural_bp.cultural_fire_router"))
                
            if next_url and ("/sace/claim_code" in next_url or "/sace/provisioning" in next_url):
                return redirect(next_url)
                
            return redirect(url_for("auth_bp.bridge_dashboard"))'''

text = text.replace(old_logic, new_logic)

# Strip BOM just in case!
import codecs
if text.startswith(codecs.BOM_UTF8.decode('utf-8')):
    text = text[1:]

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(text)

print("Auth routing fixed.")
