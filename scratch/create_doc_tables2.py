from app import create_app, db
from sqlalchemy import text
app=create_app()
app.app_context().push()

t1 = "CREATE TABLE IF NOT EXISTS uip_document (id SERIAL PRIMARY KEY, organization_id INTEGER NOT NULL REFERENCES core_organization(id), uploader_id INTEGER NOT NULL REFERENCES \\"user\\"(id), filename VARCHAR(255) NOT NULL, file_type VARCHAR(50), description VARCHAR(255), access_classification VARCHAR(50) DEFAULT 'PRIVATE', interaction_id INTEGER REFERENCES core_interaction(id), meeting_id INTEGER REFERENCES uip_committee_meeting(id), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
t2 = "CREATE TABLE IF NOT EXISTS uip_broadcast (id SERIAL PRIMARY KEY, organization_id INTEGER NOT NULL REFERENCES core_organization(id), sender_id INTEGER NOT NULL REFERENCES \\"user\\"(id), subject VARCHAR(255), body_text TEXT, channel VARCHAR(50), target_audience VARCHAR(50), status VARCHAR(50) DEFAULT 'DRAFT', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, sent_at TIMESTAMP)"

db.session.execute(text(t1))
db.session.execute(text(t2))
db.session.commit()
