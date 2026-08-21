import re

with open('app/program_debtors/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace the MockProfile logic so we don't force use_custom_letterhead = False
old_mock = '''            # If no profile, OR if the profile doesn't have a logo/letterhead, we use the shop's
            if not profile or not has_logo:
                class MockProfile:
                    business_name = shop.business_name if not profile else profile.business_name
                    address = shop.address if not profile else profile.address
                    phone = shop.phone if not profile else profile.phone
                    email = shop.email if not profile else profile.email
                    logo_url = shop.logo_url if not profile else getattr(profile, "logo_url", None)
                    letterhead_url = shop.letterhead_url
                    use_custom_letterhead = shop.use_custom_letterhead
                    terms_and_conditions = shop.terms_and_conditions
                profile = MockProfile()
            else:
                # Add the attribute dynamically so the template renders it
                setattr(profile, 'use_custom_letterhead', False)'''

new_mock = '''            # We now properly sync letterhead_url and use_custom_letterhead to SenderProfile
            # so we only need MockProfile if there's no SenderProfile at all.
            if not profile:
                class MockProfile:
                    business_name = shop.business_name
                    address = shop.address
                    phone = shop.phone
                    email = shop.email
                    logo_url = shop.logo_url
                    letterhead_url = shop.letterhead_url
                    use_custom_letterhead = shop.use_custom_letterhead
                    terms_and_conditions = shop.terms_and_conditions
                profile = MockProfile()
            else:
                # If they have a profile, ensure the terms_and_conditions fallback exists
                if not hasattr(profile, 'terms_and_conditions') or not profile.terms_and_conditions:
                    setattr(profile, 'terms_and_conditions', shop.terms_and_conditions)
                # It now natively has use_custom_letterhead and letterhead_url in the DB!
'''

content = content.replace(old_mock, new_mock)

with open('app/program_debtors/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
