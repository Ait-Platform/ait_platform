import re

routes_path = 'app/program_sace/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    routes = f.read()

search = '''      interaction = SaceWorkshopInteraction(
          user_id=current_user.id,
          activity_slug='workshop_post_test',
          response_data=json.dumps(answers)
      )
      db.session.add(interaction)'''

replace = '''      interaction = SaceWorkshopInteraction(
          user_id=current_user.id,
          activity_slug='workshop_post_test',
          response_data=json.dumps(answers)
      )
      db.session.add(interaction)
      db.session.flush()
      
      from app.models.core import CoreAuditEvent
      audit = CoreAuditEvent(
          user_id=current_user.id,
          action="SACE_EVALUATION_COMPLETED",
          entity_type="SaceWorkshopInteraction",
          entity_id=interaction.id,
          details=f"SACE Auditor submitted final evaluation. Score: {score}%"
      )
      db.session.add(audit)'''

routes = routes.replace(search, replace)

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(routes)
