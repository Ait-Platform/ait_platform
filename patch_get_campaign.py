import re
with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_func = """    def get_campaign(profile):
        if not profile.campaign_status:
            c = UipMemberCampaign(member_profile_id=profile.id, invite_wave=0)
            db.session.add(c)
            # flush so we can use it
            db.session.flush()
        return profile.campaign_status"""

new_func = """    def get_campaign(profile):
        if not profile.campaign_status:
            c = UipMemberCampaign(member_profile_id=profile.id, invite_wave=0)
            db.session.add(c)
            db.session.flush()
            return c
        return profile.campaign_status"""

text = text.replace(old_func, new_func)

with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Fixed get_campaign NoneType bug")
