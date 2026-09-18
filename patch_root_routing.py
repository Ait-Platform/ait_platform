import re

with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

pattern = r'if appointment and not force_menu:\s*from app\.models\.core import CoreOrganizationMember(.*?)\n\s*return redirect\(url_for\("uip_bp\.committee_dashboard", org_slug=org\.slug\)\)'
replacement = """if appointment and not force_menu:
        from app.models.core import CoreOrganizationMember\g<1>
        pos = appointment.position.lower()
        if pos in ['chairman', 'chairperson', 'chair', 'vice chair', 'vice chairman']:
            return redirect(url_for('uip_bp.dashboard', org_slug=org.slug))
        return redirect(url_for("uip_bp.committee_dashboard", org_slug=org.slug))"""

text = re.sub(pattern, replacement, text, flags=re.DOTALL)

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Applied routing fix to root repo")
