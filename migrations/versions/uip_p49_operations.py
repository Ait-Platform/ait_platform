"""Add UIP operational capability; only the UIP branch is advanced.

Frozen PostgreSQL DDL. No application factory or production assumptions.
Historical timestamps and file content are never fabricated.
"""
from alembic import op
import sqlalchemy as sa

revision = "uip_p49_operations"
down_revision = "uip_p3_work_orders"
branch_labels = None
depends_on = None

DDL = (
    """ALTER TABLE "uip_municipal_referral" ADD COLUMN organization_id INTEGER""",
    """ALTER TABLE "uip_municipal_referral" ADD COLUMN version INTEGER DEFAULT '1' NOT NULL""",
    """ALTER TABLE "uip_document" ADD COLUMN title VARCHAR(255)""",
    """ALTER TABLE "uip_document" ADD COLUMN category VARCHAR(100)""",
    """ALTER TABLE "uip_document" ADD COLUMN folder_id INTEGER""",
    """ALTER TABLE "uip_document" ADD COLUMN current_version INTEGER DEFAULT '0' NOT NULL""",
    """ALTER TABLE "uip_committee_meeting" ADD COLUMN agenda TEXT""",
    """ALTER TABLE "uip_committee_meeting" ADD COLUMN eligibility_basis JSON""",
    """ALTER TABLE "uip_committee_meeting" ADD COLUMN quorum_rule JSON""",
    """ALTER TABLE "uip_committee_meeting" ADD COLUMN eligible_count INTEGER""",
    """ALTER TABLE "uip_committee_meeting" ADD COLUMN attendance_count INTEGER""",
    """ALTER TABLE "uip_committee_meeting" ADD COLUMN required_quorum INTEGER""",
    """ALTER TABLE "uip_committee_meeting" ADD COLUMN quorum_achieved BOOLEAN""",
    """ALTER TABLE "uip_committee_meeting" ADD COLUMN quorum_recorded_at TIMESTAMP WITH TIME ZONE""",
    """ALTER TABLE "uip_committee_meeting" ADD COLUMN quorum_recorded_by INTEGER""",
    """ALTER TABLE "uip_resolution" ADD COLUMN organization_id INTEGER""",
    """ALTER TABLE "uip_resolution" ADD COLUMN survey_id INTEGER""",
    """ALTER TABLE "uip_resolution" ADD COLUMN decision_date DATE""",
    """ALTER TABLE "uip_resolution" ADD COLUMN recorded_by INTEGER""",
    """ALTER TABLE "uip_resolution" ADD COLUMN responsible_user_id INTEGER""",
    """ALTER TABLE "uip_resolution" ADD COLUMN result_basis JSON""",
    """ALTER TABLE "uip_resolution" ADD COLUMN supersedes_id INTEGER""",
    """UPDATE uip_municipal_referral r SET organization_id = i.organization_id FROM core_interaction i WHERE i.id = r.interaction_id""",
    """UPDATE uip_resolution r SET organization_id = m.organization_id FROM uip_committee_meeting m WHERE m.id = r.meeting_id""",
    """ALTER TABLE uip_municipal_referral ALTER COLUMN organization_id SET NOT NULL""",
    """ALTER TABLE uip_resolution ALTER COLUMN organization_id SET NOT NULL""",
    """ALTER TABLE uip_resolution ALTER COLUMN meeting_id DROP NOT NULL""",
    """ALTER TABLE uip_municipal_referral ADD CONSTRAINT uq_uip_referral_org UNIQUE (id, organization_id)""",
    """ALTER TABLE uip_document ADD CONSTRAINT uq_uip_document_org UNIQUE (id, organization_id)""",
    """ALTER TABLE uip_committee_meeting ADD CONSTRAINT uq_uip_meeting_org UNIQUE (id, organization_id)""",
    """ALTER TABLE uip_resolution ADD CONSTRAINT uq_uip_resolution_org UNIQUE (id, organization_id)""",
    """CREATE TABLE uip_sla_policy (
	id SERIAL NOT NULL, 
	organization_id INTEGER NOT NULL, 
	category VARCHAR(100) NOT NULL, 
	priority VARCHAR(20) NOT NULL, 
	stage VARCHAR(30) NOT NULL, 
	target_minutes INTEGER NOT NULL, 
	warning_minutes INTEGER NOT NULL, 
	created_by INTEGER NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	is_active BOOLEAN NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT uq_uip_sla_policy_org UNIQUE (id, organization_id), 
	CONSTRAINT ck_uip_sla_policy_minutes CHECK (target_minutes > 0 AND warning_minutes >= 0 AND warning_minutes <= target_minutes), 
	CONSTRAINT ck_uip_sla_policy_stage CHECK (stage IN ('acknowledgement','dispatch','acceptance','commencement','completion','closure')), 
	FOREIGN KEY(organization_id) REFERENCES core_organization (id), 
	FOREIGN KEY(created_by) REFERENCES "user" (id)
)""",
    """CREATE UNIQUE INDEX uq_uip_sla_policy_active ON uip_sla_policy (organization_id, category, priority, stage) WHERE is_active""",
    """CREATE TABLE uip_sla_clock (
	id SERIAL NOT NULL, 
	organization_id INTEGER NOT NULL, 
	interaction_id INTEGER NOT NULL, 
	work_order_id INTEGER, 
	policy_id INTEGER NOT NULL, 
	stage VARCHAR(30) NOT NULL, 
	started_at TIMESTAMP WITH TIME ZONE NOT NULL, 
	target_at TIMESTAMP WITH TIME ZONE NOT NULL, 
	warning_at TIMESTAMP WITH TIME ZONE NOT NULL, 
	finished_at TIMESTAMP WITH TIME ZONE, 
	stopped_at TIMESTAMP WITH TIME ZONE, 
	stop_reason VARCHAR(30), 
	PRIMARY KEY (id), 
	CONSTRAINT fk_uip_sla_issue_org FOREIGN KEY(interaction_id, organization_id) REFERENCES core_interaction (id, organization_id), 
	CONSTRAINT fk_uip_sla_order_org FOREIGN KEY(work_order_id, organization_id) REFERENCES uip_work_order (id, organization_id), 
	CONSTRAINT fk_uip_sla_policy_org FOREIGN KEY(policy_id, organization_id) REFERENCES uip_sla_policy (id, organization_id), 
	CONSTRAINT ck_uip_sla_clock_dates CHECK (target_at >= warning_at AND warning_at >= started_at), 
	CONSTRAINT ck_uip_sla_clock_finish CHECK (finished_at IS NULL OR finished_at >= started_at)
)""",
    """CREATE UNIQUE INDEX uq_uip_sla_issue_stage ON uip_sla_clock (interaction_id, stage) WHERE work_order_id IS NULL""",
    """CREATE UNIQUE INDEX uq_uip_sla_order_stage ON uip_sla_clock (work_order_id, stage) WHERE work_order_id IS NOT NULL""",
    """CREATE TABLE uip_follow_up (
	id SERIAL NOT NULL, 
	organization_id INTEGER NOT NULL, 
	interaction_id INTEGER NOT NULL, 
	actor_user_id INTEGER NOT NULL, 
	occurred_at TIMESTAMP WITH TIME ZONE NOT NULL, 
	recorded_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	method VARCHAR(30) NOT NULL, 
	outcome VARCHAR(30) NOT NULL, 
	next_action VARCHAR(30) NOT NULL, 
	next_action_at TIMESTAMP WITH TIME ZONE, 
	note TEXT, 
	completed_at TIMESTAMP WITH TIME ZONE, 
	completed_by INTEGER, 
	PRIMARY KEY (id), 
	CONSTRAINT fk_uip_followup_issue_org FOREIGN KEY(interaction_id, organization_id) REFERENCES core_interaction (id, organization_id), 
	CONSTRAINT ck_uip_followup_method CHECK (method IN ('TELEPHONE','EMAIL','WHATSAPP','IN_PERSON','INTERNAL','OTHER')), 
	CONSTRAINT ck_uip_followup_outcome CHECK (outcome IN ('CONTACTED','NO_ANSWER','UNREACHABLE','INFORMATION_RECEIVED','ESCALATED','NO_CONTACT_REQUIRED')), 
	CONSTRAINT ck_uip_followup_next CHECK (next_action IN ('NONE','CONTACT','REVIEW','ASSIGN','ESCALATE')), 
	CONSTRAINT ck_uip_followup_due CHECK (next_action = 'NONE' OR next_action_at IS NOT NULL), 
	CONSTRAINT ck_uip_followup_completed CHECK ((completed_at IS NULL) = (completed_by IS NULL)), 
	FOREIGN KEY(actor_user_id) REFERENCES "user" (id), 
	FOREIGN KEY(completed_by) REFERENCES "user" (id)
)""",
    """CREATE TABLE uip_referral_event (
	id SERIAL NOT NULL, 
	organization_id INTEGER NOT NULL, 
	referral_id INTEGER NOT NULL, 
	actor_user_id INTEGER NOT NULL, 
	occurred_at TIMESTAMP WITH TIME ZONE NOT NULL, 
	recorded_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	previous_state VARCHAR(50), 
	new_state VARCHAR(50) NOT NULL, 
	municipality_reference VARCHAR(100), 
	note TEXT, 
	resulting_version INTEGER NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT fk_uip_referral_event_org FOREIGN KEY(referral_id, organization_id) REFERENCES uip_municipal_referral (id, organization_id), 
	CONSTRAINT uq_uip_referral_event_version UNIQUE (referral_id, resulting_version), 
	FOREIGN KEY(actor_user_id) REFERENCES "user" (id)
)""",
    """CREATE TABLE uip_communication_log (
	id SERIAL NOT NULL, 
	organization_id INTEGER NOT NULL, 
	interaction_id INTEGER, 
	work_order_id INTEGER, 
	referral_id INTEGER, 
	actor_user_id INTEGER NOT NULL, 
	occurred_at TIMESTAMP WITH TIME ZONE NOT NULL, 
	recorded_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	channel VARCHAR(30) NOT NULL, 
	direction VARCHAR(20) NOT NULL, 
	party_classification VARCHAR(30) NOT NULL, 
	purpose VARCHAR(30) NOT NULL, 
	status VARCHAR(30) NOT NULL, 
	summary TEXT, 
	PRIMARY KEY (id), 
	CONSTRAINT fk_uip_comm_issue_org FOREIGN KEY(interaction_id, organization_id) REFERENCES core_interaction (id, organization_id), 
	CONSTRAINT fk_uip_comm_order_org FOREIGN KEY(work_order_id, organization_id) REFERENCES uip_work_order (id, organization_id), 
	CONSTRAINT fk_uip_comm_referral_org FOREIGN KEY(referral_id, organization_id) REFERENCES uip_municipal_referral (id, organization_id), 
	CONSTRAINT ck_uip_comm_direction CHECK (direction IN ('INBOUND','OUTBOUND')), 
	CONSTRAINT ck_uip_comm_status CHECK (status IN ('RECORDED','RECEIVED','FAILED','DELIVERY_UNAVAILABLE')), 
	FOREIGN KEY(organization_id) REFERENCES core_organization (id), 
	FOREIGN KEY(actor_user_id) REFERENCES "user" (id)
)""",
    """CREATE TABLE uip_document_folder (
	id SERIAL NOT NULL, 
	organization_id INTEGER NOT NULL, 
	name VARCHAR(100) NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT uq_uip_document_folder_org UNIQUE (id, organization_id), 
	CONSTRAINT uq_uip_document_folder_name UNIQUE (organization_id, name), 
	FOREIGN KEY(organization_id) REFERENCES core_organization (id)
)""",
    """CREATE TABLE uip_document_version (
	id SERIAL NOT NULL, 
	organization_id INTEGER NOT NULL, 
	document_id INTEGER NOT NULL, 
	version INTEGER NOT NULL, 
	effective_date DATE NOT NULL, 
	actor_user_id INTEGER NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	filename VARCHAR(255) NOT NULL, 
	storage_key VARCHAR(64) NOT NULL, 
	content_type VARCHAR(100) NOT NULL, 
	size_bytes INTEGER NOT NULL, 
	sha256 VARCHAR(64) NOT NULL, 
	replacement_reason TEXT NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT fk_uip_document_version_org FOREIGN KEY(document_id, organization_id) REFERENCES uip_document (id, organization_id), 
	CONSTRAINT uq_uip_document_version UNIQUE (document_id, version), 
	CONSTRAINT ck_uip_document_version_values CHECK (version > 0 AND size_bytes > 0), 
	FOREIGN KEY(actor_user_id) REFERENCES "user" (id), 
	UNIQUE (storage_key)
)""",
    """CREATE TABLE uip_quorum_rule (
	id SERIAL NOT NULL, 
	organization_id INTEGER NOT NULL, 
	percentage INTEGER NOT NULL, 
	minimum INTEGER NOT NULL, 
	relationship VARCHAR(20) NOT NULL, 
	updated_by INTEGER NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT ck_uip_quorum_values CHECK (percentage > 0 AND percentage <= 100 AND minimum >= 1), 
	CONSTRAINT ck_uip_quorum_relationship CHECK (relationship IN ('owner','occupier')), 
	UNIQUE (organization_id), 
	FOREIGN KEY(organization_id) REFERENCES core_organization (id), 
	FOREIGN KEY(updated_by) REFERENCES "user" (id)
)""",
    """CREATE TABLE uip_meeting_participant (
	id SERIAL NOT NULL, 
	organization_id INTEGER NOT NULL, 
	meeting_id INTEGER NOT NULL, 
	member_id INTEGER NOT NULL, 
	attended_by_member_id INTEGER, 
	status VARCHAR(20) NOT NULL, 
	recorded_by INTEGER NOT NULL, 
	recorded_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT fk_uip_participant_meeting_org FOREIGN KEY(meeting_id, organization_id) REFERENCES uip_committee_meeting (id, organization_id), 
	CONSTRAINT fk_uip_participant_member_org FOREIGN KEY(member_id, organization_id) REFERENCES uip_member_profile (id, organization_id), 
	CONSTRAINT fk_uip_participant_proxy_org FOREIGN KEY(attended_by_member_id, organization_id) REFERENCES uip_member_profile (id, organization_id), 
	CONSTRAINT uq_uip_meeting_member UNIQUE (meeting_id, member_id), 
	CONSTRAINT ck_uip_attendance_status CHECK (status IN ('INVITED','PRESENT','APOLOGY','ABSENT')), 
	FOREIGN KEY(recorded_by) REFERENCES "user" (id)
)""",
    """CREATE TABLE uip_survey (
	id SERIAL NOT NULL, 
	organization_id INTEGER NOT NULL, 
	title VARCHAR(255) NOT NULL, 
	purpose TEXT NOT NULL, 
	opens_at TIMESTAMP WITH TIME ZONE NOT NULL, 
	closes_at TIMESTAMP WITH TIME ZONE NOT NULL, 
	relationship VARCHAR(20) NOT NULL, 
	identifiable BOOLEAN NOT NULL, 
	questions JSON NOT NULL, 
	status VARCHAR(20) NOT NULL, 
	results JSON, 
	created_by INTEGER NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	finalized_at TIMESTAMP WITH TIME ZONE, 
	finalized_by INTEGER, 
	PRIMARY KEY (id), 
	CONSTRAINT uq_uip_survey_org UNIQUE (id, organization_id), 
	CONSTRAINT ck_uip_survey_dates CHECK (closes_at > opens_at), 
	CONSTRAINT ck_uip_survey_relationship CHECK (relationship IN ('owner','occupier')), 
	CONSTRAINT ck_uip_survey_state CHECK (status IN ('OPEN','FINALIZED')), 
	FOREIGN KEY(organization_id) REFERENCES core_organization (id), 
	FOREIGN KEY(created_by) REFERENCES "user" (id), 
	FOREIGN KEY(finalized_by) REFERENCES "user" (id)
)""",
    """CREATE TABLE uip_survey_response (
	id SERIAL NOT NULL, 
	organization_id INTEGER NOT NULL, 
	survey_id INTEGER NOT NULL, 
	member_id INTEGER NOT NULL, 
	actor_user_id INTEGER NOT NULL, 
	eligibility_basis JSON NOT NULL, 
	answers JSON NOT NULL, 
	responded_at TIMESTAMP WITH TIME ZONE NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT fk_uip_response_survey_org FOREIGN KEY(survey_id, organization_id) REFERENCES uip_survey (id, organization_id), 
	CONSTRAINT fk_uip_response_member_org FOREIGN KEY(member_id, organization_id) REFERENCES uip_member_profile (id, organization_id), 
	CONSTRAINT uq_uip_survey_response UNIQUE (survey_id, member_id), 
	FOREIGN KEY(actor_user_id) REFERENCES "user" (id)
)""",
    """CREATE TABLE uip_decision_event (
	id SERIAL NOT NULL, 
	organization_id INTEGER NOT NULL, 
	decision_id INTEGER NOT NULL, 
	actor_user_id INTEGER NOT NULL, 
	status VARCHAR(30) NOT NULL, 
	note TEXT NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT fk_uip_decision_event_org FOREIGN KEY(decision_id, organization_id) REFERENCES uip_resolution (id, organization_id), 
	CONSTRAINT ck_uip_decision_status CHECK (status IN ('RECORDED','IN_PROGRESS','COMPLETED','SUPERSEDED')), 
	FOREIGN KEY(actor_user_id) REFERENCES "user" (id)
)""",
    """ALTER TABLE uip_municipal_referral ADD CONSTRAINT fk_uip_referral_issue_org FOREIGN KEY(interaction_id, organization_id) REFERENCES core_interaction (id, organization_id)""",
    """ALTER TABLE uip_document ADD CONSTRAINT fk_uip_document_folder_org FOREIGN KEY(folder_id, organization_id) REFERENCES uip_document_folder (id, organization_id)""",
    """ALTER TABLE uip_resolution ADD CONSTRAINT fk_uip_resolution_survey_org FOREIGN KEY(survey_id, organization_id) REFERENCES uip_survey (id, organization_id)""",
    """ALTER TABLE uip_resolution ADD CONSTRAINT fk_uip_resolution_supersedes_org FOREIGN KEY(supersedes_id, organization_id) REFERENCES uip_resolution (id, organization_id)""",
    """ALTER TABLE uip_resolution ADD CONSTRAINT fk_uip_resolution_meeting_org FOREIGN KEY(meeting_id, organization_id) REFERENCES uip_committee_meeting (id, organization_id)""",
    """ALTER TABLE uip_resolution ADD CONSTRAINT ck_uip_resolution_source CHECK ((meeting_id IS NOT NULL) <> (survey_id IS NOT NULL))""",
    """ALTER TABLE uip_municipal_referral ADD FOREIGN KEY(organization_id) REFERENCES core_organization (id)""",
    """ALTER TABLE uip_committee_meeting ADD FOREIGN KEY(quorum_recorded_by) REFERENCES "user" (id)""",
    """ALTER TABLE uip_resolution ADD FOREIGN KEY(responsible_user_id) REFERENCES "user" (id)""",
    """ALTER TABLE uip_resolution ADD FOREIGN KEY(organization_id) REFERENCES core_organization (id)""",
    """ALTER TABLE uip_resolution ADD FOREIGN KEY(recorded_by) REFERENCES "user" (id)""",
    """CREATE FUNCTION uip_p49_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'UIP historical records are immutable'; END $$""",
    """CREATE TRIGGER uip_referral_event_immutable BEFORE UPDATE OR DELETE ON uip_referral_event FOR EACH ROW EXECUTE FUNCTION uip_p49_immutable()""",
    """CREATE TRIGGER uip_document_version_immutable BEFORE UPDATE OR DELETE ON uip_document_version FOR EACH ROW EXECUTE FUNCTION uip_p49_immutable()""",
    """CREATE TRIGGER uip_survey_response_immutable BEFORE UPDATE OR DELETE ON uip_survey_response FOR EACH ROW EXECUTE FUNCTION uip_p49_immutable()""",
    """CREATE TRIGGER uip_decision_event_immutable BEFORE UPDATE OR DELETE ON uip_decision_event FOR EACH ROW EXECUTE FUNCTION uip_p49_immutable()""",
    """CREATE TRIGGER uip_resolution_immutable BEFORE UPDATE OR DELETE ON uip_resolution FOR EACH ROW EXECUTE FUNCTION uip_p49_immutable()""",
    """CREATE FUNCTION uip_p49_meeting_frozen() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.quorum_recorded_at IS NOT NULL THEN
    RAISE EXCEPTION 'Concluded UIP meeting and quorum are immutable';
  END IF;
  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END $$""",
    """CREATE TRIGGER uip_meeting_frozen BEFORE UPDATE OR DELETE ON uip_committee_meeting FOR EACH ROW EXECUTE FUNCTION uip_p49_meeting_frozen()""",
    """CREATE FUNCTION uip_p49_attendance_frozen() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE meeting_key integer; org_key integer;
BEGIN
  IF TG_OP = 'DELETE' THEN meeting_key := OLD.meeting_id; org_key := OLD.organization_id;
  ELSE meeting_key := NEW.meeting_id; org_key := NEW.organization_id; END IF;
  PERFORM id FROM uip_committee_meeting WHERE id=meeting_key AND organization_id=org_key FOR UPDATE;
  IF EXISTS (SELECT 1 FROM uip_committee_meeting WHERE id=meeting_key AND organization_id=org_key AND quorum_recorded_at IS NOT NULL) THEN
    RAISE EXCEPTION 'Attendance for a concluded UIP meeting is immutable';
  END IF;
  IF TG_OP = 'UPDATE' AND ROW(NEW.meeting_id, NEW.organization_id, NEW.member_id) IS DISTINCT FROM ROW(OLD.meeting_id, OLD.organization_id, OLD.member_id) THEN
    RAISE EXCEPTION 'UIP attendance identity is immutable';
  END IF;
  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END $$""",
    """CREATE TRIGGER uip_attendance_frozen BEFORE INSERT OR UPDATE OR DELETE ON uip_meeting_participant FOR EACH ROW EXECUTE FUNCTION uip_p49_attendance_frozen()""",
    """CREATE FUNCTION uip_p49_survey_frozen() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' OR OLD.status = 'FINALIZED' THEN
    RAISE EXCEPTION 'UIP survey history is immutable';
  END IF;
  IF ROW(NEW.organization_id, NEW.title, NEW.purpose, NEW.opens_at, NEW.closes_at, NEW.relationship, NEW.identifiable, NEW.questions::text)
     IS DISTINCT FROM ROW(OLD.organization_id, OLD.title, OLD.purpose, OLD.opens_at, OLD.closes_at, OLD.relationship, OLD.identifiable, OLD.questions::text) THEN
    RAISE EXCEPTION 'UIP survey design cannot change after creation';
  END IF;
  RETURN NEW;
END $$""",
    """CREATE TRIGGER uip_survey_frozen BEFORE UPDATE OR DELETE ON uip_survey FOR EACH ROW EXECUTE FUNCTION uip_p49_survey_frozen()""",
)


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        raise RuntimeError("UIP operational migration requires PostgreSQL")
    for statement in DDL:
        op.execute(sa.text(statement))


def downgrade():
    raise RuntimeError("UIP operational history must be preserved; downgrade is refused")
