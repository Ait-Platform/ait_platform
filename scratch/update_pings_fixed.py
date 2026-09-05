import re

routes_path = 'app/program_sace/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    routes = f.read()

# Normalize line endings
routes = routes.replace('\r\n', '\n')

# Pledge Ping
pledge_search = '''          interaction = SaceWorkshopInteraction(
              user_id=current_user.id,
              activity_slug="admin_patent_pledge",
              response_data="Admin accepted IP pledge"
          )
          db.session.add(interaction)'''
          
pledge_replace = '''          interaction = SaceWorkshopInteraction(
              user_id=current_user.id,
              activity_slug="admin_patent_pledge",
              response_data="Admin accepted IP pledge"
          )
          db.session.add(interaction)
          db.session.flush() # Get ID
          
          from app.models.core import CoreAuditEvent
          audit = CoreAuditEvent(
              user_id=current_user.id,
              action="SACE_PLEDGE_ACCEPTED",
              entity_type="SaceWorkshopInteraction",
              entity_id=interaction.id,
              details="SACE Administrator accepted the Intellectual Property Pledge."
          )
          db.session.add(audit)'''
          
routes = routes.replace(pledge_search, pledge_replace)

# Auditor Ping
auditor_search = '''          interaction = SaceWorkshopInteraction(
              user_id=current_user.id,
              activity_slug="auditor_provisioned",
              response_data=json.dumps(data)
          )
          db.session.add(interaction)'''

auditor_replace = '''          interaction = SaceWorkshopInteraction(
              user_id=current_user.id,
              activity_slug="auditor_provisioned",
              response_data=json.dumps(data)
          )
          db.session.add(interaction)
          db.session.flush() # Get ID
          
          from app.models.core import CoreAuditEvent
          audit = CoreAuditEvent(
              user_id=current_user.id,
              action="SACE_AUDITOR_PROVISIONED",
              entity_type="SaceWorkshopInteraction",
              entity_id=interaction.id,
              details=f"SACE Admin provisioned auditor: {first_name} {last_name} ({email})"
          )
          db.session.add(audit)'''

routes = routes.replace(auditor_search, auditor_replace)

# Submit Ping
submit_search = '''      interaction = SaceWorkshopInteraction(
          user_id=current_user.id,
          activity_slug='workshop_post_test',
          response_data=json.dumps(answers)
      )
      db.session.add(interaction)'''

submit_replace = '''      interaction = SaceWorkshopInteraction(
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

routes = routes.replace(submit_search, submit_replace)

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(routes)
