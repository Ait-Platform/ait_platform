-- Synthetic CURRENT UIP request dependencies only; NOT a migration.
-- Applied exclusively inside the disposable test schema after historical migration tests.


CREATE TABLE auth_subject (
	id SERIAL NOT NULL, 
	slug VARCHAR(100), 
	PRIMARY KEY (id), 
	UNIQUE (slug)
)

;


CREATE TABLE uip_register_import (
	id SERIAL NOT NULL, 
	organization_id INTEGER NOT NULL, 
	source_type VARCHAR(50) NOT NULL, 
	source_identifier VARCHAR(255), 
	batch_reference VARCHAR(100), 
	date_received DATE NOT NULL, 
	effective_date DATE NOT NULL, 
	imported_by_user_id INTEGER NOT NULL, 
	document_id INTEGER, 
	status VARCHAR(50) NOT NULL, 
	notes TEXT, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	FOREIGN KEY(organization_id) REFERENCES core_organization (id), 
	FOREIGN KEY(imported_by_user_id) REFERENCES "user" (id), 
	FOREIGN KEY(document_id) REFERENCES uip_document (id)
)

;


CREATE TABLE core_organization_entitlement (
	id SERIAL NOT NULL, 
	organization_id INTEGER NOT NULL, 
	subject_id INTEGER NOT NULL, 
	status VARCHAR(50) NOT NULL, 
	start_date TIMESTAMP WITHOUT TIME ZONE, 
	end_date TIMESTAMP WITHOUT TIME ZONE, 
	is_trial BOOLEAN, 
	payment_provenance VARCHAR(255), 
	PRIMARY KEY (id), 
	CONSTRAINT uq_org_subject_entitlement UNIQUE (organization_id, subject_id), 
	FOREIGN KEY(organization_id) REFERENCES core_organization (id), 
	FOREIGN KEY(subject_id) REFERENCES auth_subject (id)
)

;


CREATE TABLE uip_committee_term (
	id SERIAL NOT NULL, 
	organization_id INTEGER NOT NULL, 
	term_name VARCHAR(100) NOT NULL, 
	created_by INTEGER, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	FOREIGN KEY(organization_id) REFERENCES core_organization (id), 
	FOREIGN KEY(created_by) REFERENCES "user" (id)
)

;


CREATE TABLE uip_delegation (
	id SERIAL NOT NULL, 
	organization_id INTEGER NOT NULL, 
	delegated_user_id INTEGER NOT NULL, 
	appointed_by_user_id INTEGER NOT NULL, 
	delegation_type VARCHAR(50) NOT NULL, 
	status VARCHAR(20) NOT NULL, 
	effective_date TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	document_id INTEGER, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT ck_uip_delegation_status CHECK (status IN ('ACTIVE','REVOKED')), 
	FOREIGN KEY(organization_id) REFERENCES core_organization (id), 
	FOREIGN KEY(delegated_user_id) REFERENCES "user" (id), 
	FOREIGN KEY(appointed_by_user_id) REFERENCES "user" (id), 
	FOREIGN KEY(document_id) REFERENCES uip_document (id)
)

;


CREATE TABLE uip_member_campaign (
	id SERIAL NOT NULL, 
	member_profile_id INTEGER NOT NULL, 
	invite_wave INTEGER NOT NULL, 
	last_invite_at TIMESTAMP WITHOUT TIME ZONE, 
	PRIMARY KEY (id), 
	UNIQUE (member_profile_id), 
	FOREIGN KEY(member_profile_id) REFERENCES uip_member_profile (id)
)

;


CREATE TABLE uip_organogram_seat (
	id SERIAL NOT NULL, 
	organization_id INTEGER NOT NULL, 
	title VARCHAR(100) NOT NULL, 
	group_level VARCHAR(50) NOT NULL, 
	qualifier VARCHAR(50) NOT NULL, 
	display_order INTEGER, 
	duty VARCHAR(50) NOT NULL, 
	PRIMARY KEY (id), 
	FOREIGN KEY(organization_id) REFERENCES core_organization (id)
)

;


CREATE TABLE uip_register_import_exception (
	id SERIAL NOT NULL, 
	import_id INTEGER NOT NULL, 
	row_number INTEGER, 
	source_reference VARCHAR(100), 
	reason VARCHAR(255) NOT NULL, 
	incoming_data JSON NOT NULL, 
	status VARCHAR(50) DEFAULT 'OPEN' NOT NULL, 
	resolution_notes TEXT, 
	resolved_at TIMESTAMP WITH TIME ZONE, 
	resolved_by_user_id INTEGER, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	PRIMARY KEY (id), 
	FOREIGN KEY(import_id) REFERENCES uip_register_import (id), 
	FOREIGN KEY(resolved_by_user_id) REFERENCES "user" (id)
)

;


CREATE TABLE uip_committee_member (
	id SERIAL NOT NULL, 
	term_id INTEGER NOT NULL, 
	organization_id INTEGER NOT NULL, 
	name VARCHAR(255) NOT NULL, 
	email VARCHAR(255) NOT NULL, 
	position VARCHAR(50) NOT NULL, 
	status VARCHAR(20) NOT NULL, 
	created_by INTEGER, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated_by INTEGER, 
	updated_at TIMESTAMP WITH TIME ZONE, 
	seat_id INTEGER, 
	photo_url VARCHAR(500), 
	PRIMARY KEY (id), 
	CONSTRAINT ck_uip_committee_member_status CHECK (status IN ('CURRENT','FORMER','VACANT')), 
	CONSTRAINT ck_uip_committee_member_position CHECK (position IN ('Chairperson','Vice-Chairperson','Treasurer','Secretary','Committee Member','Advisory Committee Member')), 
	FOREIGN KEY(term_id) REFERENCES uip_committee_term (id), 
	FOREIGN KEY(organization_id) REFERENCES core_organization (id), 
	FOREIGN KEY(created_by) REFERENCES "user" (id), 
	FOREIGN KEY(updated_by) REFERENCES "user" (id), 
	FOREIGN KEY(seat_id) REFERENCES uip_organogram_seat (id)
)

;


CREATE TABLE uip_resolution_comment (
	id SERIAL NOT NULL, 
	resolution_id INTEGER NOT NULL, 
	user_id INTEGER NOT NULL, 
	message TEXT NOT NULL, 
	timestamp TIMESTAMP WITHOUT TIME ZONE, 
	PRIMARY KEY (id), 
	FOREIGN KEY(resolution_id) REFERENCES uip_resolution (id), 
	FOREIGN KEY(user_id) REFERENCES "user" (id)
)

;


CREATE TABLE uip_resolution_vote (
	id SERIAL NOT NULL, 
	resolution_id INTEGER NOT NULL, 
	user_id INTEGER NOT NULL, 
	vote VARCHAR(20) NOT NULL, 
	timestamp TIMESTAMP WITHOUT TIME ZONE, 
	PRIMARY KEY (id), 
	CONSTRAINT uq_uip_resolution_vote UNIQUE (resolution_id, user_id), 
	FOREIGN KEY(resolution_id) REFERENCES uip_resolution (id), 
	FOREIGN KEY(user_id) REFERENCES "user" (id)
)

;

ALTER TABLE "core_interaction" ADD COLUMN parent_id INTEGER;

ALTER TABLE "uip_resolution" ADD COLUMN voting_scope VARCHAR(20);

ALTER TABLE "uip_resolution" ADD COLUMN quorum_target INTEGER;

ALTER TABLE "uip_resolution" ADD COLUMN expires_at TIMESTAMP WITHOUT TIME ZONE;

ALTER TABLE "uip_member_profile" ADD COLUMN record_source VARCHAR(50) DEFAULT 'MANUAL' NOT NULL;

ALTER TABLE "uip_member_profile" ADD COLUMN last_import_id INTEGER;

ALTER TABLE "uip_property" ADD COLUMN record_source VARCHAR(50) DEFAULT 'MANUAL' NOT NULL;

ALTER TABLE "uip_property" ADD COLUMN last_import_id INTEGER;

ALTER TABLE "uip_property_member" ADD COLUMN record_source VARCHAR(50) DEFAULT 'MANUAL' NOT NULL;

ALTER TABLE "uip_property_member" ADD COLUMN last_import_id INTEGER;
-- Explicit constraints for the additive synthetic dependencies above.
ALTER TABLE core_interaction ADD FOREIGN KEY (parent_id) REFERENCES core_interaction(id);
ALTER TABLE uip_member_profile ADD FOREIGN KEY (last_import_id) REFERENCES uip_register_import(id);
ALTER TABLE uip_property ADD FOREIGN KEY (last_import_id) REFERENCES uip_register_import(id);
ALTER TABLE uip_property_member ADD FOREIGN KEY (last_import_id) REFERENCES uip_register_import(id);
CREATE INDEX ix_uip_committee_member_email ON uip_committee_member(email);
