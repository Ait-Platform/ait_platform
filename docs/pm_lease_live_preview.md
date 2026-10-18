# Lease saved-particulars viewer and submenu

Lease remains item 2 in Property Management. Its workspace and input pages share the seven-option Lease submenu: Parties, Premises, Period, Amounts, Special Conditions, View Lease and Generate PDF. Generate Word remains a separate utility leading to the existing completion checklist and download endpoint. Existing master/supporting/history routes remain available.

View Lease reads authorized saved draft JSONB only and creates a temporary in-memory merged DOCX from the pinned Stage 5 master. Missing placeholders remain visible. Body paragraphs and tables are extracted in XML order. The viewer groups text into logical pages at document headings, explicit page breaks and next-page section breaks. It ignores cached Word lastRenderedPageBreak markers. Tables remain intact. This is section navigation, not Microsoft Word pagination; a long clause or schedule can require scrolling inside a logical page.

One logical page is displayed at a time with Previous, editable Page number of total, Next and Zoom controls. Per-page mapping details identify original master tokens, occurrence counts and existing saved source fields, linking to the existing form. Additional Special Conditions without a slot in the master are not silently appended or inserted. The same saved source can populate several locations.

No draft writes or generation history updates occur from viewing or the PDF status page. Strict Word generation remains unchanged. Generate PDF explicitly reports that PDF generation is not yet available. No PDF conversion, LibreOffice, additional dependency, external service or hosting change is introduced. Protected programmes and the Word master remain untouched.
