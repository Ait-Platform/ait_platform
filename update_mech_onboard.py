import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_onboard = '''    shop.terms_and_conditions = request.form.get("terms_and_conditions")
    shop.onboarding_status = 'active'
    # Default to true if a checkbox is checked, but we'll override it later if they actually have a letterhead url
    shop.use_custom_letterhead = True if request.form.get("use_custom_letterhead") else False'''

new_onboard = '''    shop.terms_and_conditions = request.form.get("terms_and_conditions")
    shop.bank_details = request.form.get("bank_details")
    shop.onboarding_status = 'active'
    # Default to true if a checkbox is checked, but we'll override it later if they actually have a letterhead url
    shop.use_custom_letterhead = True if request.form.get("use_custom_letterhead") else False'''

content = content.replace(old_onboard, new_onboard)

old_sync = '''        if sender_profile:
            sender_profile.business_name = shop.business_name
            sender_profile.address = shop.address
            sender_profile.phone = shop.phone
            sender_profile.email = shop.email
            sender_profile.logo_url = shop.logo_url
            sender_profile.letterhead_url = shop.letterhead_url
            sender_profile.use_custom_letterhead = shop.use_custom_letterhead
        else:
            sender_profile = SenderProfile(
                user_id=current_user.id,
                business_name=shop.business_name,
                address=shop.address,
                phone=shop.phone,
                email=shop.email,
                logo_url=shop.logo_url,
                letterhead_url=shop.letterhead_url,
                use_custom_letterhead=shop.use_custom_letterhead,
                is_default=True
            )
            db.session.add(sender_profile)'''

new_sync = '''        if sender_profile:
            sender_profile.business_name = shop.business_name
            sender_profile.address = shop.address
            sender_profile.phone = shop.phone
            sender_profile.email = shop.email
            sender_profile.logo_url = shop.logo_url
            sender_profile.letterhead_url = shop.letterhead_url
            sender_profile.use_custom_letterhead = shop.use_custom_letterhead
        else:
            sender_profile = SenderProfile(
                user_id=current_user.id,
                business_name=shop.business_name,
                address=shop.address,
                phone=shop.phone,
                email=shop.email,
                logo_url=shop.logo_url,
                letterhead_url=shop.letterhead_url,
                use_custom_letterhead=shop.use_custom_letterhead,
                is_default=True
            )
            db.session.add(sender_profile)
            
        # Also sync to SoaProfile so SOA can use it directly
        from app.models.debtors import SoaProfile
        soa_profile = SoaProfile.query.filter_by(user_id=current_user.id).first()
        if not soa_profile:
            soa_profile = SoaProfile(user_id=current_user.id)
            db.session.add(soa_profile)
        soa_profile.business_name = shop.business_name
        soa_profile.address = shop.address
        soa_profile.phone = shop.phone
        soa_profile.email = shop.email
        soa_profile.logo_url = shop.logo_url
        soa_profile.letterhead_url = shop.letterhead_url
        soa_profile.use_custom_letterhead = shop.use_custom_letterhead
        soa_profile.bank_details = shop.bank_details
'''

content = content.replace(old_sync, new_sync)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
