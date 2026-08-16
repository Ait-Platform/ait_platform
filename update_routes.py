import re

with open('app/program_debtors/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_logic = '''
    if debtor.sender_profile_id:
        profile = SenderProfile.query.get(debtor.sender_profile_id)
    else:
        profile = SenderProfile.query.filter_by(
            user_id=current_user.id, is_default=True).first()

    # Mock profile for mechanic integration if no Debtors profile exists
    if not profile and debtor.slug_reference == 'mechanic':
        from app.models.mechanic import MechShop
        shop = MechShop.query.filter_by(
            user_id=current_user.id, onboarding_status='active').first()
        if shop:
            class MockProfile:
                business_name = shop.business_name
                address = shop.address
                phone = shop.phone
                email = shop.email
                logo_url = shop.letterhead_url if shop.use_custom_letterhead else shop.logo_url
                use_custom_letterhead = shop.use_custom_letterhead
            profile = MockProfile()
'''

new_logic = '''
    if debtor.sender_profile_id:
        profile = SenderProfile.query.get(debtor.sender_profile_id)
    else:
        profile = SenderProfile.query.filter_by(
            user_id=current_user.id, is_default=True).first()

    # Fallback/Override for Mechanic Integration
    if debtor.slug_reference == 'mechanic':
        from app.models.mechanic import MechShop
        shop = MechShop.query.filter_by(
            user_id=current_user.id, onboarding_status='active').first()
        if shop:
            # We want to pull the Mechanic letterhead if the debtor sender profile lacks one
            has_logo = profile and getattr(profile, 'logo_url', None)
            
            # If no profile, OR if the profile doesn't have a logo/letterhead, we use the shop's
            if not profile or not has_logo:
                class MockProfile:
                    business_name = shop.business_name if not profile else profile.business_name
                    address = shop.address if not profile else profile.address
                    phone = shop.phone if not profile else profile.phone
                    email = shop.email if not profile else profile.email
                    logo_url = shop.letterhead_url if shop.use_custom_letterhead else shop.logo_url
                    use_custom_letterhead = shop.use_custom_letterhead
                profile = MockProfile()
            else:
                # Add the attribute dynamically so the template renders it
                setattr(profile, 'use_custom_letterhead', False)
'''

content = content.replace(old_logic, new_logic)

with open('app/program_debtors/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated routes.py successfully")
