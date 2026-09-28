import re

filepath_router = 'app/program_uip/routes.py'
with open(filepath_router, 'r', encoding='utf-8') as f:
    router_content = f.read()

# Fix UipCommitteeMember query in router_page
safe_committee_query = '''      from sqlalchemy import func, or_
      email_check = False
      if current_user.email and current_user.email.strip():
          email_check = func.lower(func.trim(UipCommitteeMember.email)) == current_user.email.strip().lower()
          
      appointment = UipCommitteeMember.query.filter(
          UipCommitteeMember.organization_id == org.id,
          UipCommitteeMember.status == "CURRENT",
          or_(UipCommitteeMember.user_id == current_user.id, email_check)
      ).first()'''

old_committee_query = '''      from sqlalchemy import func
      force_menu = request.args.get('force')
      
      # 1. Auto-route if already verified committee
      from app.models.uip_governance import UipCommitteeMember
      appointment = UipCommitteeMember.query.filter(
          UipCommitteeMember.organization_id == org.id,
          UipCommitteeMember.status == "CURRENT",
          func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
      ).first()'''

new_committee_query = '''      from sqlalchemy import func
      force_menu = request.args.get('force')
      
      # 1. Auto-route if already verified committee
      from app.models.uip_governance import UipCommitteeMember
''' + safe_committee_query

router_content = router_content.replace(old_committee_query, new_committee_query)

with open(filepath_router, 'w', encoding='utf-8') as f:
    f.write(router_content)

print("Fixed router to match David robustly")
