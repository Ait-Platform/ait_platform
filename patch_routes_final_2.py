import re

with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

pattern = re.compile(r'if claim and ":" in claim.title:\s*new_pos = claim.title.split\(": "\)\[-1\]\s*([^.]*)\.position = new_pos\s*db\.session\.commit\(\)', re.MULTILINE | re.DOTALL)

def replacement(match):
    obj_name = match.group(1)
    return f"""if claim:
                new_pos = claim.title.split(": ")[-1] if ":" in claim.title else (claim.title.split(" - ")[-1] if " - " in claim.title else claim.title)
                {obj_name}.position = new_pos.strip()
                db.session.commit()"""

text, count = pattern.subn(replacement, text)
print(f"Replacements made: {count}")

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
