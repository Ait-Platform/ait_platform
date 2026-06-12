DROP SEQUENCE IF EXISTS bil_bank_detail_id_seq CASCADE;
ALTER TABLE bil_tenant ADD COLUMN IF NOT EXISTS bank_detail_id INTEGER REFERENCES bil_bank_detail(id);
