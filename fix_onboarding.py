import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Fix business_name indentation
old_biz = '''        shop.business_name = request.form.get(
            "business_name") or "My Mechanic Shop"
    shop.address = request.form.get("address")'''

new_biz = '''    
    shop.business_name = request.form.get("business_name") or shop.business_name or "My Mechanic Shop"
    shop.address = request.form.get("address")'''

content = content.replace(old_biz, new_biz)

# 2. Add Debtors sync at the end of onboarding_process
old_end = '''
        letterhead_file.save(os.path.join(upload_folder, lh_filename))
        shop.letterhead_url = lh_filename

    db.session.commit()
    flash("Shop profile setup complete!", "success")
'''

new_end = '''
        letterhead_file.save(os.path.join(upload_folder, lh_filename))
        shop.letterhead_url = lh_filename

    db.session.commit()
    
    # Sync with SenderProfile for Debtors module
    from app.models.debtors import SenderProfile
    sender_profile = SenderProfile.query.filter_by(user_id=current_user.id, is_default=True).first()
    if not sender_profile:
        sender_profile = SenderProfile(user_id=current_user.id, is_default=True)
        db.session.add(sender_profile)
    sender_profile.business_name = shop.business_name
    sender_profile.address = shop.address
    sender_profile.phone = shop.phone
    sender_profile.email = shop.email
    sender_profile.logo_url = shop.logo_url
    sender_profile.letterhead_url = shop.letterhead_url
    sender_profile.use_custom_letterhead = shop.use_custom_letterhead
    db.session.commit()
    
    flash("Shop profile successfully saved!", "success")
'''

content = content.replace(old_end, new_end)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
