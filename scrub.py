with open("app/program_uip/routes.py", "r", encoding="utf-8-sig") as f:
    lines = f.readlines()

out = []
for i, line in enumerate(lines):
    # Remove deep imports of db
    if line.startswith('        from app import db') or line.startswith('        from app.extensions import db'):
        pass # delete this line
    elif line.startswith('            from app import db') or line.startswith('            from app.extensions import db'):
        pass # delete this line
    elif line.startswith('                    from app import db') or line.startswith('                    from app.extensions import db'):
        pass # delete this line
    else:
        out.append(line)

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.writelines(out)
print("Scrubbed deep db imports")
