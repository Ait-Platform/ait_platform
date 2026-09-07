"""UIP Phase 2 register and isolated audit; no business-data backfill.

Revision ID: a27c9e4b6102
Revises: uip_p2_prod_base
"""
from alembic import op
import sqlalchemy as sa
revision = "a27c9e4b6102"
down_revision = "uip_p2_prod_base"
branch_labels = None
depends_on = None


def upgrade():
    op.create_unique_constraint("uq_core_member_id_org", "core_organization_member", ["id", "organization_id"])
    op.alter_column("core_organization_member", "user_id", existing_type=sa.Integer(), nullable=True)
    op.execute("""CREATE TABLE uip_member_profile (
	id SERIAL NOT NULL, 
	organization_id INTEGER NOT NULL, 
	membership_id INTEGER NOT NULL, 
	reference VARCHAR(50) NOT NULL, 
	name VARCHAR(255) NOT NULL, 
	member_type VARCHAR(20) NOT NULL, 
	email VARCHAR(255), 
	phone VARCHAR(50), 
	is_active BOOLEAN NOT NULL, 
	eligibility_status VARCHAR(20) NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT uq_uip_member_id_org UNIQUE (id, organization_id), 
	CONSTRAINT uq_uip_member_reference UNIQUE (organization_id, reference), 
	CONSTRAINT fk_uip_profile_membership_org FOREIGN KEY(membership_id, organization_id) REFERENCES core_organization_member (id, organization_id), 
	CONSTRAINT ck_uip_member_type CHECK (member_type IN ('person','business')), 
	CONSTRAINT ck_uip_member_eligibility CHECK (eligibility_status IN ('unverified','eligible','ineligible')), 
	FOREIGN KEY(organization_id) REFERENCES core_organization (id), 
	UNIQUE (membership_id)
)""")
    op.execute("""CREATE TABLE uip_property (
	id SERIAL NOT NULL, 
	organization_id INTEGER NOT NULL, 
	reference VARCHAR(50) NOT NULL, 
	address VARCHAR(500) NOT NULL, 
	rates_reference VARCHAR(100), 
	classification VARCHAR(20) NOT NULL, 
	is_active BOOLEAN NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT uq_uip_property_id_org UNIQUE (id, organization_id), 
	CONSTRAINT uq_uip_property_reference UNIQUE (organization_id, reference), 
	CONSTRAINT ck_uip_property_classification CHECK (classification IN ('residential','business','mixed','other')), 
	FOREIGN KEY(organization_id) REFERENCES core_organization (id)
)""")
    op.execute("""CREATE TABLE uip_property_member (
	id SERIAL NOT NULL, 
	organization_id INTEGER NOT NULL, 
	property_id INTEGER NOT NULL, 
	member_id INTEGER NOT NULL, 
	relationship VARCHAR(20) NOT NULL, 
	valid_from DATE NOT NULL, 
	valid_to DATE, 
	is_verified BOOLEAN NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT fk_uip_property_member_property FOREIGN KEY(property_id, organization_id) REFERENCES uip_property (id, organization_id), 
	CONSTRAINT fk_uip_property_member_member FOREIGN KEY(member_id, organization_id) REFERENCES uip_member_profile (id, organization_id), 
	CONSTRAINT uq_uip_property_member UNIQUE (organization_id, property_id, member_id, relationship, valid_from), 
	CONSTRAINT ck_uip_property_member_relationship CHECK (relationship IN ('owner','occupier','representative')), 
	CONSTRAINT ck_uip_property_member_dates CHECK (valid_to IS NULL OR valid_to >= valid_from), 
	FOREIGN KEY(organization_id) REFERENCES core_organization (id)
)""")
    op.execute("""CREATE TABLE uip_member_representative (
	id SERIAL NOT NULL, 
	organization_id INTEGER NOT NULL, 
	member_id INTEGER NOT NULL, 
	representative_id INTEGER NOT NULL, 
	valid_from DATE NOT NULL, 
	valid_to DATE, 
	is_verified BOOLEAN NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT fk_uip_represented_member FOREIGN KEY(member_id, organization_id) REFERENCES uip_member_profile (id, organization_id), 
	CONSTRAINT fk_uip_representative_member FOREIGN KEY(representative_id, organization_id) REFERENCES uip_member_profile (id, organization_id), 
	CONSTRAINT uq_uip_representative UNIQUE (organization_id, member_id, representative_id, valid_from), 
	CONSTRAINT ck_uip_representative_distinct CHECK (member_id <> representative_id), 
	CONSTRAINT ck_uip_representative_dates CHECK (valid_to IS NULL OR valid_to >= valid_from), 
	FOREIGN KEY(organization_id) REFERENCES core_organization (id)
)""")
    op.execute("""CREATE TABLE uip_communication_preference (
	id SERIAL NOT NULL, 
	organization_id INTEGER NOT NULL, 
	member_id INTEGER NOT NULL, 
	channel VARCHAR(20) NOT NULL, 
	preference VARCHAR(20) NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT fk_uip_preference_member FOREIGN KEY(member_id, organization_id) REFERENCES uip_member_profile (id, organization_id), 
	CONSTRAINT uq_uip_preference UNIQUE (organization_id, member_id, channel), 
	CONSTRAINT ck_uip_preference_channel CHECK (channel IN ('Telephone','Email','WhatsApp','Post')), 
	CONSTRAINT ck_uip_preference_value CHECK (preference IN ('unspecified','allowed','declined')), 
	FOREIGN KEY(organization_id) REFERENCES core_organization (id)
)""")
    op.execute("""CREATE TABLE uip_audit_event (
	id SERIAL NOT NULL, 
	organization_id INTEGER NOT NULL, 
	actor_user_id INTEGER NOT NULL, 
	action VARCHAR(80) NOT NULL, 
	entity_type VARCHAR(50) NOT NULL, 
	entity_id INTEGER NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	metadata_json JSON NOT NULL, 
	PRIMARY KEY (id), 
	FOREIGN KEY(organization_id) REFERENCES core_organization (id), 
	FOREIGN KEY(actor_user_id) REFERENCES "user" (id)
)""")
    op.execute("""CREATE INDEX ix_uip_audit_org_entity ON uip_audit_event (organization_id, entity_type, entity_id)""")
    op.execute("""CREATE INDEX ix_uip_audit_org_time ON uip_audit_event (organization_id, created_at, id)""")
    op.add_column("core_interaction", sa.Column("member_id", sa.Integer(), nullable=True))
    op.add_column("core_interaction", sa.Column("property_id", sa.Integer(), nullable=True))
    op.add_column("core_interaction", sa.Column("recorded_by", sa.Integer(), nullable=True))
    op.create_foreign_key("fk_interaction_recorded_by", "core_interaction", "user", ["recorded_by"], ["id"])
    op.create_foreign_key("fk_interaction_uip_member_org", "core_interaction", "uip_member_profile", ["member_id", "organization_id"], ["id", "organization_id"])
    op.create_foreign_key("fk_interaction_uip_property_org", "core_interaction", "uip_property", ["property_id", "organization_id"], ["id", "organization_id"])
    op.execute("""CREATE FUNCTION uip_audit_reject_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN RAISE EXCEPTION 'UIP audit events are append-only'; END;
    $$""")
    op.execute("""CREATE TRIGGER uip_audit_append_only BEFORE UPDATE OR DELETE ON uip_audit_event
    FOR EACH ROW EXECUTE FUNCTION uip_audit_reject_mutation()""")


def downgrade():
    # Refuse to discard register/audit history or nullable non-login memberships.
    # Empty-schema rollback is supported; populated rollback needs an explicit plan.
    op.execute("""DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM uip_member_profile)
       OR EXISTS (SELECT 1 FROM uip_property)
       OR EXISTS (SELECT 1 FROM uip_audit_event)
       OR EXISTS (SELECT 1 FROM core_organization_member WHERE user_id IS NULL)
       OR EXISTS (SELECT 1 FROM core_interaction WHERE member_id IS NOT NULL OR property_id IS NOT NULL OR recorded_by IS NOT NULL)
    THEN RAISE EXCEPTION 'Phase 2 contains data: downgrade requires a reviewed preservation plan';
    END IF; END $$""")
    for name in ("fk_interaction_uip_member_org", "fk_interaction_uip_property_org", "fk_interaction_recorded_by"):
        op.drop_constraint(name, "core_interaction", type_="foreignkey")
    for name in ("member_id", "property_id", "recorded_by"):
        op.drop_column("core_interaction", name)
    for name in ("uip_audit_event", "uip_communication_preference", "uip_member_representative", "uip_property_member", "uip_property", "uip_member_profile"):
        op.drop_table(name)
    op.execute("DROP FUNCTION uip_audit_reject_mutation()")
    op.alter_column("core_organization_member", "user_id", existing_type=sa.Integer(), nullable=False)
    op.drop_constraint("uq_core_member_id_org", "core_organization_member", type_="unique")
