# Lease mail-merge text viewer

View Lease renders text extracted from a temporary in-memory merged Stage 5 DOCX. It reads only the authorized saved draft JSONB, not posted values or live owner/tenant records. Complete drafts use the existing Generate Word builder; incomplete previews use the same master and merger with absent placeholders preserved. Strict Word download validation remains unchanged.

Body paragraphs, headings and tables are extracted in original XML order. Text is escaped by Jinja. This is a merge-placement check, not Word pagination, headers/footers, floating-object or layout reproduction. No PDF, LibreOffice, remote conversion, new dependencies or database changes are required. Viewing has private/no-store responses and does not append generation history or save drafts.
