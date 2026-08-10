import sys

with open('app/auth/routes.py', 'r', encoding='utf-8') as f:
    code = f.read()

target = """          single_slug = getattr(rows[0], 'slug', '')
          if single_slug:
              return redirect(url_for("auth_bp.dashboard_info", subject=single_slug))"""

replacement = """          single_slug = getattr(rows[0], 'slug', '')
          if single_slug and single_slug != 'staff':
              return redirect(url_for("auth_bp.dashboard_info", subject=single_slug))"""

if target in code:
    with open('app/auth/routes.py', 'w', encoding='utf-8') as f:
        f.write(code.replace(target, replacement))
    print('Fixed')
else:
    print('Target not found')
