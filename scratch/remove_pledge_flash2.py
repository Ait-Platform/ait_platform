import re

routes_path = 'app/program_sace/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    routes = f.read()

old_flash = '''        db.session.commit()
        
        if not current_user.is_authenticated:
            flash("show_reg_modal", "sace_reg_prompt")
    
    return redirect(url_for('sace_bp.provisioning_map'))'''
    
new_flash = '''        db.session.commit()
    
    return redirect(url_for('sace_bp.provisioning_map'))'''

routes = routes.replace(old_flash, new_flash)

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(routes)
