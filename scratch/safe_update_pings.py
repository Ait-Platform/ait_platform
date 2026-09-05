import re

routes_path = 'app/program_sace/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    routes = f.read()

# Normalize line endings
routes = routes.replace('\r\n', '\n')

# 1. Pledge Ping
pledge_old = '''          interaction = SaceWorkshopInteraction(
              user_id=current_user.id,
              activity_slug="admin_patent_pledge",
              response_data="Admin accepted IP pledge"
          )
          db.session.add(interaction)'''
          
pledge_new = '''          interaction = SaceWorkshopInteraction(
              user_id=current_user.id,
              activity_slug="admin_patent_pledge",
              response_data="Admin accepted IP pledge"
          )
          db.session.add(interaction)
          db.session.flush()
          from app.models.core import CoreAuditEvent
          db.session.add(CoreAuditEvent(
              user_id=current_user.id,
              action="SACE_PLEDGE_ACCEPTED",
              entity_type="SaceWorkshopInteraction",
              entity_id=interaction.id,
              details="SACE Administrator accepted the Intellectual Property Pledge."
          ))'''
routes = routes.replace(pledge_old, pledge_new)

# 2. Auditor Ping
auditor_old = '''          interaction = SaceWorkshopInteraction(
              user_id=current_user.id,
              activity_slug="auditor_provisioned",
              response_data=json.dumps(data)
          )
          db.session.add(interaction)'''
          
auditor_new = '''          interaction = SaceWorkshopInteraction(
              user_id=current_user.id,
              activity_slug="auditor_provisioned",
              response_data=json.dumps(data)
          )
          db.session.add(interaction)
          db.session.flush()
          from app.models.core import CoreAuditEvent
          db.session.add(CoreAuditEvent(
              user_id=current_user.id,
              action="SACE_AUDITOR_PROVISIONED",
              entity_type="SaceWorkshopInteraction",
              entity_id=interaction.id,
              details=f"SACE Admin provisioned auditor: {first_name} {last_name} ({email})"
          ))'''
routes = routes.replace(auditor_old, auditor_new)


# 3. Post Test Ping
test_old = '''      interaction = SaceWorkshopInteraction(
          user_id=current_user.id,
          activity_slug='workshop_post_test',
          response_data=json.dumps(answers)
      )
      db.session.add(interaction)'''
      
test_new = '''      interaction = SaceWorkshopInteraction(
          user_id=current_user.id,
          activity_slug='workshop_post_test',
          response_data=json.dumps(answers)
      )
      db.session.add(interaction)
      db.session.flush()
      from app.models.core import CoreAuditEvent
      db.session.add(CoreAuditEvent(
          user_id=current_user.id,
          action="SACE_EVALUATION_COMPLETED",
          entity_type="SaceWorkshopInteraction",
          entity_id=interaction.id,
          details=f"SACE Auditor submitted final evaluation. Score: {score}%"
      ))'''
routes = routes.replace(test_old, test_new)


with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(routes)
