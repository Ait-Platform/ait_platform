# Annual municipal tariff automation

## Stage 3B ? conservative extraction and verification

Retained Stage 3A PDFs can be opened through Verification Status. Verification reads exact stored bytes, validates their digest and official URL, extracts reviewed municipal five-column tables and compares exact decimal rates against the existing register. No database writes occur during verification. Original source, page, excerpt, financial year, unit, category, daily-band mapping and discharge evidence remain attached to every extracted component.

The reviewed parser supports the municipal domestic water/sewerage layout, without individual break-pressure tanks and for the stated valuation/development category. VAT-exclusive annual columns are selected explicitly; changes to column layout, bands or discharge evidence fail closed. Unsupported publications/layouts remain exceptions. It does not turn a generic electricity scale or new municipal charge into a supported METSOA component automatically.

The current publication's fixed rows can be extracted without approving their allocation basis. CCMGMT is per submeter, which is not established as equivalent to AIT W_MGT. The water loss monthly basis, refuse category and electricity scale require evidence. No W_MGT calculation or meter allocation is changed.

Adoption must bind the particular tariff publication, financial year and annual effective dates. A final filename or a general budget adoption announcement is insufficient. The initial verifier recognises a strict self-specific adoption statement; other council-resolution formats are HOLD pending a reviewed evidence adapter. The official 2026/27 PDF currently lacks that recognised statement. It must not be represented as automatically verified or activated.

NEW, UPDATED and UNCHANGED require verified adoption and supported mapping. Missing, ambiguous, conflicting or unsupported components classify EXCEPTION and HOLD. No proximity tolerance is used. Automated tests use official text excerpts with synthetic adoption statements clearly identified as tests, not municipal evidence. The existing individual approval route remains unchanged as the controlled fallback; it is not the normal automated path.
