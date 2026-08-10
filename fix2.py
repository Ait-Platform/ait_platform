import sys

with open('app/auth/routes.py', 'r', encoding='utf-8') as f:
    code = f.read()

target = """      # Bypass bridge dashboard if they only have 1 enrollment
      force_bridge = request.args.get('force') == '1'
      is_admin = any(getattr(r, 'access_level', '') == 'admin' for r in rows)
      if not force_bridge and len(rows) == 1 and not is_admin:
          # Auto-forward to the specific subject dashboard
          single_slug = getattr(rows[0], 'slug', '')
          if single_slug:
              return redirect(url_for("auth_bp.dashboard_info", subject=single_slug))"""

replacement = """      # Bypass bridge dashboard if they only have 1 enrollment
      force_bridge = request.args.get('force') == '1'
      is_admin = any(getattr(r, 'access_level', '') == 'admin' for r in rows)
      if not force_bridge and len(rows) == 1 and not is_admin:
          # Auto-forward to the specific subject dashboard
          single_slug = getattr(rows[0], 'slug', '')
          if single_slug and single_slug != 'staff':
              return redirect(url_for("auth_bp.dashboard_info", subject=single_slug))"""

if target in code:
    with open('app/auth/routes.py', 'w', encoding='utf-8') as f:
        f.write(code.replace(target, replacement))
    print('Fixed')
else:
    print('Target not found')
