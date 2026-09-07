-- Frozen synthetic Phase 1 schema; no production data.
CREATE TABLE "user" (
	is_active INTEGER DEFAULT 1 NOT NULL,
	id SERIAL NOT NULL,
	name VARCHAR(255),
	email VARCHAR(255),
	PRIMARY KEY (id),
	UNIQUE (email)
);

CREATE TABLE ait_token_transaction (
	id SERIAL NOT NULL,
	PRIMARY KEY (id)
);

CREATE TABLE core_organization (
	id SERIAL NOT NULL,
	name VARCHAR(255) NOT NULL,
	slug VARCHAR(255) NOT NULL,
	area VARCHAR(255),
	municipality_ref VARCHAR(255),
	contact_email VARCHAR(255),
	contact_phone VARCHAR(50),
	status VARCHAR(50),
	config_json TEXT,
	created_at TIMESTAMP WITHOUT TIME ZONE,
	PRIMARY KEY (id)
);

CREATE UNIQUE INDEX ix_core_organization_slug ON core_organization (slug);

CREATE INDEX ix_core_organization_status ON core_organization (status);

CREATE TABLE core_permission (
	id SERIAL NOT NULL,
	name VARCHAR(255) NOT NULL,
	slug VARCHAR(255) NOT NULL,
	description TEXT,
	PRIMARY KEY (id)
);

CREATE UNIQUE INDEX ix_core_permission_slug ON core_permission (slug);

CREATE TABLE core_organization_wallet (
	id SERIAL NOT NULL,
	organization_id INTEGER NOT NULL,
	balance INTEGER NOT NULL,
	created_at TIMESTAMP WITHOUT TIME ZONE,
	PRIMARY KEY (id),
	UNIQUE (organization_id),
	FOREIGN KEY(organization_id) REFERENCES core_organization (id)
);

CREATE TABLE core_organization_member (
	id SERIAL NOT NULL,
	organization_id INTEGER NOT NULL,
	user_id INTEGER NOT NULL,
	is_active BOOLEAN NOT NULL,
	joined_at TIMESTAMP WITHOUT TIME ZONE,
	left_at TIMESTAMP WITHOUT TIME ZONE,
	PRIMARY KEY (id),
	FOREIGN KEY(organization_id) REFERENCES core_organization (id),
	FOREIGN KEY(user_id) REFERENCES "user" (id)
);

CREATE TABLE core_role (
	id SERIAL NOT NULL,
	name VARCHAR(255) NOT NULL,
	slug VARCHAR(255) NOT NULL,
	organization_id INTEGER,
	PRIMARY KEY (id),
	FOREIGN KEY(organization_id) REFERENCES core_organization (id)
);

CREATE INDEX ix_core_role_slug ON core_role (slug);

CREATE TABLE core_interaction (
	id SERIAL NOT NULL,
	reference VARCHAR(50),
	organization_id INTEGER NOT NULL,
	creator_id INTEGER NOT NULL,
	assigned_to INTEGER,
	closed_by INTEGER,
	channel VARCHAR(50),
	category VARCHAR(100),
	interaction_type VARCHAR(50) NOT NULL,
	title VARCHAR(255) NOT NULL,
	description TEXT,
	status VARCHAR(50),
	priority VARCHAR(50),
	created_at TIMESTAMP WITHOUT TIME ZONE,
	updated_at TIMESTAMP WITHOUT TIME ZONE,
	closed_at TIMESTAMP WITHOUT TIME ZONE,
	PRIMARY KEY (id),
	FOREIGN KEY(organization_id) REFERENCES core_organization (id),
	FOREIGN KEY(assigned_to) REFERENCES "user" (id),
	FOREIGN KEY(closed_by) REFERENCES "user" (id),
	FOREIGN KEY(creator_id) REFERENCES "user" (id)
);

CREATE INDEX ix_core_interaction_status ON core_interaction (status);

CREATE UNIQUE INDEX ix_core_interaction_reference ON core_interaction (reference);

CREATE TABLE core_audit_event (
	id SERIAL NOT NULL,
	organization_id INTEGER,
	user_id INTEGER,
	action VARCHAR(100) NOT NULL,
	entity_type VARCHAR(100),
	entity_id INTEGER,
	details TEXT,
	ip_address VARCHAR(50),
	created_at TIMESTAMP WITHOUT TIME ZONE,
	PRIMARY KEY (id),
	FOREIGN KEY(user_id) REFERENCES "user" (id),
	FOREIGN KEY(organization_id) REFERENCES core_organization (id)
);

CREATE INDEX ix_core_audit_event_created_at ON core_audit_event (created_at);

CREATE INDEX ix_core_audit_event_action ON core_audit_event (action);

CREATE TABLE uip_provider (
	id SERIAL NOT NULL,
	organization_id INTEGER NOT NULL,
	name VARCHAR(255) NOT NULL,
	service_type VARCHAR(100),
	contact_email VARCHAR(255),
	contact_phone VARCHAR(50),
	is_active BOOLEAN,
	created_at TIMESTAMP WITHOUT TIME ZONE,
	PRIMARY KEY (id),
	FOREIGN KEY(organization_id) REFERENCES core_organization (id)
);

CREATE TABLE uip_committee_meeting (
	id SERIAL NOT NULL,
	organization_id INTEGER NOT NULL,
	title VARCHAR(255) NOT NULL,
	meeting_type VARCHAR(50),
	scheduled_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
	location VARCHAR(255),
	status VARCHAR(50),
	minutes_text TEXT,
	created_at TIMESTAMP WITHOUT TIME ZONE,
	PRIMARY KEY (id),
	FOREIGN KEY(organization_id) REFERENCES core_organization (id)
);

CREATE TABLE uip_broadcast (
	id SERIAL NOT NULL,
	organization_id INTEGER NOT NULL,
	sender_id INTEGER NOT NULL,
	subject VARCHAR(255),
	body_text TEXT,
	channel VARCHAR(50),
	target_audience VARCHAR(50),
	status VARCHAR(50),
	created_at TIMESTAMP WITHOUT TIME ZONE,
	sent_at TIMESTAMP WITHOUT TIME ZONE,
	PRIMARY KEY (id),
	FOREIGN KEY(sender_id) REFERENCES "user" (id),
	FOREIGN KEY(organization_id) REFERENCES core_organization (id)
);

CREATE TABLE core_organization_ledger (
	id SERIAL NOT NULL,
	wallet_id INTEGER NOT NULL,
	amount INTEGER NOT NULL,
	description VARCHAR(255),
	created_at TIMESTAMP WITHOUT TIME ZONE,
	PRIMARY KEY (id),
	FOREIGN KEY(wallet_id) REFERENCES core_organization_wallet (id)
);

CREATE TABLE core_role_permission (
	id SERIAL NOT NULL,
	role_id INTEGER NOT NULL,
	permission_id INTEGER NOT NULL,
	PRIMARY KEY (id),
	FOREIGN KEY(permission_id) REFERENCES core_permission (id),
	FOREIGN KEY(role_id) REFERENCES core_role (id)
);

CREATE TABLE core_role_assignment (
	id SERIAL NOT NULL,
	user_id INTEGER NOT NULL,
	organization_id INTEGER NOT NULL,
	role_id INTEGER NOT NULL,
	assigned_at TIMESTAMP WITHOUT TIME ZONE,
	PRIMARY KEY (id),
	FOREIGN KEY(role_id) REFERENCES core_role (id),
	FOREIGN KEY(user_id) REFERENCES "user" (id),
	FOREIGN KEY(organization_id) REFERENCES core_organization (id)
);

CREATE TABLE core_task (
	id SERIAL NOT NULL,
	interaction_id INTEGER NOT NULL,
	assignee_id INTEGER,
	title VARCHAR(255),
	description TEXT,
	status VARCHAR(50),
	due_date TIMESTAMP WITHOUT TIME ZONE,
	created_at TIMESTAMP WITHOUT TIME ZONE,
	completed_at TIMESTAMP WITHOUT TIME ZONE,
	PRIMARY KEY (id),
	FOREIGN KEY(interaction_id) REFERENCES core_interaction (id),
	FOREIGN KEY(assignee_id) REFERENCES "user" (id)
);

CREATE INDEX ix_core_task_status ON core_task (status);

CREATE TABLE core_remuneration_rule (
	id SERIAL NOT NULL,
	organization_id INTEGER NOT NULL,
	interaction_type VARCHAR(50) NOT NULL,
	rate_cents INTEGER NOT NULL,
	role_id INTEGER,
	requires_approval BOOLEAN,
	PRIMARY KEY (id),
	FOREIGN KEY(organization_id) REFERENCES core_organization (id),
	FOREIGN KEY(role_id) REFERENCES core_role (id)
);

CREATE TABLE core_ai_request (
	id SERIAL NOT NULL,
	organization_id INTEGER,
	user_id INTEGER,
	interaction_id INTEGER,
	model_requested VARCHAR(50),
	provider_used VARCHAR(50),
	prompt_text TEXT,
	response_text TEXT,
	status VARCHAR(50),
	created_at TIMESTAMP WITHOUT TIME ZONE,
	completed_at TIMESTAMP WITHOUT TIME ZONE,
	PRIMARY KEY (id),
	FOREIGN KEY(interaction_id) REFERENCES core_interaction (id),
	FOREIGN KEY(organization_id) REFERENCES core_organization (id),
	FOREIGN KEY(user_id) REFERENCES "user" (id)
);

CREATE TABLE uip_work_order (
	id SERIAL NOT NULL,
	interaction_id INTEGER NOT NULL,
	provider_id INTEGER NOT NULL,
	reference VARCHAR(50),
	description TEXT,
	status VARCHAR(50),
	cost_cents INTEGER,
	created_at TIMESTAMP WITHOUT TIME ZONE,
	completed_at TIMESTAMP WITHOUT TIME ZONE,
	verified_at TIMESTAMP WITHOUT TIME ZONE,
	PRIMARY KEY (id),
	UNIQUE (reference),
	FOREIGN KEY(interaction_id) REFERENCES core_interaction (id),
	FOREIGN KEY(provider_id) REFERENCES uip_provider (id)
);

CREATE TABLE uip_municipal_referral (
	id SERIAL NOT NULL,
	interaction_id INTEGER NOT NULL,
	department VARCHAR(100),
	municipality_reference VARCHAR(100),
	status VARCHAR(50),
	sla_expected_date TIMESTAMP WITHOUT TIME ZONE,
	created_at TIMESTAMP WITHOUT TIME ZONE,
	resolved_at TIMESTAMP WITHOUT TIME ZONE,
	PRIMARY KEY (id),
	FOREIGN KEY(interaction_id) REFERENCES core_interaction (id)
);

CREATE TABLE uip_document (
	id SERIAL NOT NULL,
	organization_id INTEGER NOT NULL,
	uploader_id INTEGER NOT NULL,
	filename VARCHAR(255) NOT NULL,
	file_type VARCHAR(50),
	description VARCHAR(255),
	access_classification VARCHAR(50),
	interaction_id INTEGER,
	meeting_id INTEGER,
	created_at TIMESTAMP WITHOUT TIME ZONE,
	PRIMARY KEY (id),
	FOREIGN KEY(meeting_id) REFERENCES uip_committee_meeting (id),
	FOREIGN KEY(uploader_id) REFERENCES "user" (id),
	FOREIGN KEY(interaction_id) REFERENCES core_interaction (id),
	FOREIGN KEY(organization_id) REFERENCES core_organization (id)
);

CREATE TABLE core_remuneration_event (
	id SERIAL NOT NULL,
	interaction_id INTEGER NOT NULL,
	user_id INTEGER NOT NULL,
	rule_id INTEGER,
	amount_cents INTEGER NOT NULL,
	status VARCHAR(50),
	created_at TIMESTAMP WITHOUT TIME ZONE,
	approved_at TIMESTAMP WITHOUT TIME ZONE,
	PRIMARY KEY (id),
	FOREIGN KEY(rule_id) REFERENCES core_remuneration_rule (id),
	FOREIGN KEY(user_id) REFERENCES "user" (id),
	FOREIGN KEY(interaction_id) REFERENCES core_interaction (id)
);

CREATE INDEX ix_core_remuneration_event_status ON core_remuneration_event (status);

CREATE TABLE core_ai_usage (
	id SERIAL NOT NULL,
	request_id INTEGER NOT NULL,
	tokens_in INTEGER,
	tokens_out INTEGER,
	cost_cents INTEGER,
	ledger_transaction_id INTEGER,
	created_at TIMESTAMP WITHOUT TIME ZONE,
	PRIMARY KEY (id),
	FOREIGN KEY(ledger_transaction_id) REFERENCES ait_token_transaction (id),
	FOREIGN KEY(request_id) REFERENCES core_ai_request (id)
);

CREATE TABLE uip_resolution (
	id SERIAL NOT NULL,
	meeting_id INTEGER NOT NULL,
	title VARCHAR(255) NOT NULL,
	description TEXT,
	status VARCHAR(50),
	linked_task_id INTEGER,
	created_at TIMESTAMP WITHOUT TIME ZONE,
	PRIMARY KEY (id),
	FOREIGN KEY(linked_task_id) REFERENCES core_task (id),
	FOREIGN KEY(meeting_id) REFERENCES uip_committee_meeting (id)
);
