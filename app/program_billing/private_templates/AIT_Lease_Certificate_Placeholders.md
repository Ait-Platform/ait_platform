# Revised blank Lease Certificate

The original source remains unchanged. SHA-256:
C976BEF23F26FE189CEAAC3C9A04916F43B01AF038BC581191757C41B4753F8A

## Repeatable parties

Native Word repeating-section controls contain three sample Lessor rows and one Lessee group. Repeat each Lessor identity row as needed. Repeat each entire Lessee group, including its representative and authority rows. In the desktop Word content-control menu use Insert Section After or remove a sample section. Keep at least one blank sample visible. Renumber indexed placeholders after manual duplication; automatic renumbering/data binding is intentionally not implemented.

Lessor n NAME and IDENTITY_NUMBER correspond to _contracting_lessors.identities[n-1].name and .identity_number. The representative for all Lessors remains in _parties.lessor. Lessee n fields correspond to _parties.lessees[n-1] (legacy single-lessee compatibility remains an application concern, not part of this template). Authority status/evidence corresponds to _ait_standard.authorities.landlords or tenant-<zero-based-index>.

## Field names and source convention

Uppercase underscore placeholders are unbound display names matching existing structured field names. They are not database bindings and do not assert that a value or authority has been confirmed.

PROPERTY_NAME/ADDRESS, LEASE_SCOPE, LEASED_PORTION_DESCRIPTION, PROPERTY_EXTENT_SQM, LEASED_AREA_SQM, UNIT_COUNT, PROPERTY_LEGAL_DESCRIPTION, EXCLUDED_AREAS and PLAN_A/B_IDENTIFIER belong to draft _premises.
COMMENCEMENT_DATE, DURATION_MONTHS, EXPIRY_DATE and RENEWAL_NOTICE_DATE belong to _period. OCCUPATION_DATE is s1-19.value; RENEWAL_PROVISION is s1-26.value.
MONTHLY_RENT, MONTHLY_RENT_WORDS, RENT_EFFECTIVE_DATE, DEPOSIT_AMOUNT, ADJUSTMENT_1/2/3_APPLICABILITY/DATE/AMOUNT and PARKING_APPLICABILITY/DETAILS/BAY_COUNT/RENT_PER_BAY belong to _amounts.
KEY_DEPOSIT_PARTICULARS = s1-30.value; AGREEMENT_PREPARATION_FEE_PARTICULARS = s1-31.value; PAYMENT_DAY_PARTICULARS = s1-25.value; PAYMENT_METHOD_REFERENCE = s1-37.value; VAT_TREATMENT = s1-34.value; RATES_INSURANCE_BASELINES = s1-35.value; DEPOSIT_DEADLINE_PARTICULARS = s1-29.value.
UTILITY_ALLOCATION_METHOD, UTILITY_REFERENCES and UTILITY_NO_REFERENCES_REASON belong to _banking_utilities. Show only the applicable reference mode when actual data is eventually used.
BANK_DETAIL_ID is the selected existing bil_bank_detail.id. BANK_NAME, BRANCH_NAME, BRANCH_CODE, ACCOUNT_TYPE, ACCOUNT_HOLDER and ACCOUNT_NUMBER use its six existing fields. No record is selected or bound in this document.
LEASE_REFERENCE uses the existing draft s1-01.value.

## Pagination and scope

A4 portrait; source typography, six-table arrangement, shading, margins and footer retained. A continuation-heading pageBreakBefore replaces the source inline manual page break, keeping the two-page preview deliberate. Section headings stay with their content, rows do not split and heights remain flexible. Populated long content or more parties may require continuation pages. Special Conditions and Signing remain separate documents.

## Complete visible placeholder list

- [ACCOUNT_HOLDER]
- [ACCOUNT_NUMBER]
- [ACCOUNT_TYPE]
- [ADJUSTMENT_1_AMOUNT]
- [ADJUSTMENT_1_APPLICABILITY]
- [ADJUSTMENT_1_DATE]
- [ADJUSTMENT_2_AMOUNT]
- [ADJUSTMENT_2_APPLICABILITY]
- [ADJUSTMENT_2_DATE]
- [ADJUSTMENT_3_AMOUNT]
- [ADJUSTMENT_3_APPLICABILITY]
- [ADJUSTMENT_3_DATE]
- [AGREEMENT_PREPARATION_FEE_PARTICULARS]
- [BANK_NAME]
- [BRANCH_CODE]
- [BRANCH_NAME]
- [COMMENCEMENT_DATE]
- [DEPOSIT_AMOUNT]
- [DEPOSIT_DEADLINE_PARTICULARS]
- [DURATION_MONTHS]
- [EXCLUDED_AREAS]
- [EXPIRY_DATE]
- [KEY_DEPOSIT_PARTICULARS]
- [LEASE_REFERENCE]
- [LEASE_SCOPE]
- [LEASED_AREA_SQM]
- [LEASED_PORTION_DESCRIPTION]
- [LESSEE_1_AUTHORITY_CONFIRMED]
- [LESSEE_1_AUTHORITY_EVIDENCE]
- [LESSEE_1_IDENTITY_NUMBER]
- [LESSEE_1_LEGAL_NAME]
- [LESSEE_1_SIGNATORY_CAPACITY]
- [LESSEE_1_SIGNATORY_IDENTITY_NUMBER]
- [LESSEE_1_SIGNATORY_NAME]
- [LESSOR_1_IDENTITY_NUMBER]
- [LESSOR_1_NAME]
- [LESSOR_2_IDENTITY_NUMBER]
- [LESSOR_2_NAME]
- [LESSOR_3_IDENTITY_NUMBER]
- [LESSOR_3_NAME]
- [LESSOR_AUTHORITY_CONFIRMED]
- [LESSOR_AUTHORITY_EVIDENCE]
- [LESSOR_SIGNATORY_CAPACITY]
- [LESSOR_SIGNATORY_IDENTITY_NUMBER]
- [LESSOR_SIGNATORY_NAME]
- [MONTHLY_RENT]
- [MONTHLY_RENT_WORDS]
- [OCCUPATION_DATE]
- [PARKING_APPLICABILITY]
- [PARKING_BAY_COUNT]
- [PARKING_DETAILS]
- [PARKING_RENT_PER_BAY]
- [PAYMENT_DAY_PARTICULARS]
- [PAYMENT_METHOD_REFERENCE]
- [PLAN_A_IDENTIFIER]
- [PLAN_B_IDENTIFIER]
- [PROPERTY_ADDRESS]
- [PROPERTY_EXTENT_SQM]
- [PROPERTY_LEGAL_DESCRIPTION]
- [PROPERTY_NAME]
- [RATES_INSURANCE_BASELINES]
- [RENEWAL_NOTICE_DATE]
- [RENEWAL_PROVISION]
- [RENT_EFFECTIVE_DATE]
- [UNIT_COUNT]
- [UTILITY_ALLOCATION_METHOD]
- [UTILITY_NO_REFERENCES_REASON]
- [UTILITY_REFERENCES]
- [VAT_TREATMENT]

## Approved implementation correction

BANK_DETAIL_ID is internal selection metadata only. It is absent from the visible master. The Selected account cell repeats ACCOUNT_HOLDER and BANK_NAME; no table, heading, page geometry, style or other package part was redesigned. Application mapping keeps it in the explicit _certificate_banking draft snapshot. No banking selection is inferred from property links alone.
