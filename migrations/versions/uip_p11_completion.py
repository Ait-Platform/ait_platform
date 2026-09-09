"""UIP pilot invitations and reuse of existing organisation AI scaffolding.
No existing row updates, no startup provisioning, no other-product migrations.
"""
from alembic import op
import sqlalchemy as sa
revision = "uip_p11_completion"
down_revision = "uip_p10_finance"
branch_labels = None
depends_on = None
DDL = ('ALTER TABLE core_organization_wallet ADD COLUMN status VARCHAR(20)', 'ALTER TABLE core_organization_wallet ADD COLUMN updated_at TIMESTAMP WITH TIME ZONE', 'ALTER TABLE core_organization_ledger ADD COLUMN reference VARCHAR(80)', 'ALTER TABLE core_organization_ledger ADD COLUMN entry_type VARCHAR(30)', 'ALTER TABLE core_organization_ledger ADD COLUMN product VARCHAR(30)', 'ALTER TABLE core_organization_ledger ADD COLUMN actor_user_id INTEGER REFERENCES "user"(id)', 'ALTER TABLE core_organization_ledger ADD COLUMN request_id INTEGER REFERENCES core_ai_request(id)', 'ALTER TABLE core_organization_ledger ADD COLUMN balance_after INTEGER', 'ALTER TABLE core_ai_request ADD COLUMN request_key VARCHAR(36)', 'ALTER TABLE core_ai_request ADD COLUMN product VARCHAR(30)', 'ALTER TABLE core_ai_request ADD COLUMN feature VARCHAR(50)', 'ALTER TABLE core_ai_request ADD COLUMN input_digest VARCHAR(64)', 'ALTER TABLE core_ai_request ADD COLUMN credits INTEGER', 'ALTER TABLE core_ai_request ADD COLUMN error_category VARCHAR(50)', 'ALTER TABLE core_ai_usage ADD COLUMN credits_charged INTEGER', 'ALTER TABLE core_ai_usage ADD COLUMN organization_ledger_id INTEGER REFERENCES core_organization_ledger(id)', 'ALTER TABLE uip_survey ADD COLUMN eligibility_snapshot JSON', 'ALTER TABLE uip_survey_response ALTER COLUMN actor_user_id DROP NOT NULL', 'ALTER TABLE uip_audit_event ALTER COLUMN actor_user_id DROP NOT NULL', "\nCREATE TABLE uip_voting_invitation (\n\tid SERIAL NOT NULL, \n\torganization_id INTEGER NOT NULL, \n\tsurvey_id INTEGER NOT NULL, \n\tmember_id INTEGER NOT NULL, \n\ttoken_digest VARCHAR(64) NOT NULL, \n\texpires_at TIMESTAMP WITH TIME ZONE NOT NULL, \n\tstatus VARCHAR(20) NOT NULL, \n\tattempts INTEGER NOT NULL, \n\tattempted_at TIMESTAMP WITH TIME ZONE NOT NULL, \n\taccepted_at TIMESTAMP WITH TIME ZONE, \n\terror_category VARCHAR(40), \n\tPRIMARY KEY (id), \n\tCONSTRAINT fk_uip_invite_survey_org FOREIGN KEY(survey_id, organization_id) REFERENCES uip_survey (id, organization_id), \n\tCONSTRAINT fk_uip_invite_member_org FOREIGN KEY(member_id, organization_id) REFERENCES uip_member_profile (id, organization_id), \n\tCONSTRAINT uq_uip_invite_member UNIQUE (survey_id, member_id), \n\tCONSTRAINT ck_uip_invite_status CHECK (status IN ('ATTEMPTED','ACCEPTED','FAILED','TEST_SUPPRESSED') AND attempts > 0), \n\tUNIQUE (token_digest)\n)\n\n", 'ALTER TABLE core_ai_request ADD CONSTRAINT uq_core_ai_request_key UNIQUE (organization_id,product,request_key)', 'ALTER TABLE core_organization_ledger ADD CONSTRAINT uq_core_ledger_reference UNIQUE (reference)', 'ALTER TABLE core_ai_usage ADD CONSTRAINT uq_core_usage_org_ledger UNIQUE (organization_ledger_id)', "CREATE FUNCTION uip_p11_ledger_guard() RETURNS trigger LANGUAGE plpgsql AS $$\nBEGIN\n IF OLD.product = 'uip' THEN RAISE EXCEPTION 'AIT organisation credit history is append-only'; END IF;\n IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;\n RETURN NEW;\nEND $$", 'CREATE TRIGGER uip_ai_ledger_immutable BEFORE UPDATE OR DELETE ON core_organization_ledger FOR EACH ROW EXECUTE FUNCTION uip_p11_ledger_guard()', "CREATE FUNCTION uip_p11_wallet_guard() RETURNS trigger LANGUAGE plpgsql AS $$\nBEGIN\n IF NEW.status IS NOT NULL AND (NEW.status NOT IN ('ACTIVE','SUSPENDED') OR NEW.balance < 0) THEN RAISE EXCEPTION 'Invalid controlled wallet balance or status'; END IF;\n RETURN NEW;\nEND $$", 'CREATE TRIGGER uip_ai_wallet_guard BEFORE INSERT OR UPDATE ON core_organization_wallet FOR EACH ROW EXECUTE FUNCTION uip_p11_wallet_guard()', "CREATE FUNCTION uip_p11_public_actor_guard() RETURNS trigger LANGUAGE plpgsql AS $$\nBEGIN\n IF NEW.actor_user_id IS NULL AND (NEW.action <> 'survey.responded' OR NEW.entity_type <> 'UipSurvey' OR NEW.metadata_json->>'source' IS DISTINCT FROM 'secure_invitation') THEN RAISE EXCEPTION 'Only invitation ballots permit a public audit actor'; END IF;\n RETURN NEW;\nEND $$", 'CREATE TRIGGER uip_public_actor_guard BEFORE INSERT ON uip_audit_event FOR EACH ROW EXECUTE FUNCTION uip_p11_public_actor_guard()')

DDL += (
    """CREATE FUNCTION uip_p11_snapshot_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF OLD.eligibility_snapshot IS NOT NULL AND NEW.eligibility_snapshot::jsonb IS DISTINCT FROM OLD.eligibility_snapshot::jsonb THEN RAISE EXCEPTION 'Survey eligibility snapshot is frozen'; END IF;
 RETURN NEW;
END $$""",
    "CREATE TRIGGER uip_survey_snapshot_guard BEFORE UPDATE ON uip_survey FOR EACH ROW EXECUTE FUNCTION uip_p11_snapshot_guard()",
)

def upgrade():
    if op.get_bind().dialect.name != "postgresql":
        raise RuntimeError("UIP completion requires PostgreSQL")
    for statement in DDL:
        op.execute(sa.text(statement))

def downgrade():
    raise RuntimeError("Voting and credit history must be preserved; downgrade is refused")
