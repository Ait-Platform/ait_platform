import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Make it automatically true if they have a letterhead, or false if not.
old_use_custom = '''    shop.terms_and_conditions = request.form.get("terms_and_conditions")
    shop.use_custom_letterhead = True if request.form.get(
        "use_custom_letterhead") else False
    shop.onboarding_status = 'active'
'''

new_use_custom = '''    shop.terms_and_conditions = request.form.get("terms_and_conditions")
    shop.onboarding_status = 'active'
    # Default to true if a checkbox is checked, but we'll override it later if they actually have a letterhead url
    shop.use_custom_letterhead = True if request.form.get("use_custom_letterhead") else False
'''

content = content.replace(old_use_custom, new_use_custom)

# Then later when saving the file:
old_lh_save = '''        letterhead_file.save(os.path.join(upload_folder, lh_filename))
        shop.letterhead_url = lh_filename

    db.session.commit()'''

new_lh_save = '''        letterhead_file.save(os.path.join(upload_folder, lh_filename))
        shop.letterhead_url = lh_filename
        
    if shop.letterhead_url:
        shop.use_custom_letterhead = True
    else:
        shop.use_custom_letterhead = False

    db.session.commit()'''

content = content.replace(old_lh_save, new_lh_save)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
