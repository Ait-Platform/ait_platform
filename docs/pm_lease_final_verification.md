# Lease and addendum final local verification

## Result

Ready for further review, not final approval/deployment. 101 focused regression executions passed; the additional final-verification suite passed 33 executions (including inherited cases). No commit, push, deployment, production changes or migrations.

## Historic example comparison remains blocked

The actual December 2023 and July 2024 addenda were not found in project files or Codex attachments. Files named synthetic-december-2023 and synthetic-july-2024 are newly generated test cases, NOT those historic examples. Comparison against their exact amendment language, signature requirements and layout remains outstanding. Supply their actual file paths/copies to finish that comparison.

The available local output/pm_stage5/Lease_Review_Draft_R01.docx has SHA-256 f6785d81e9fdedf9541cf6b6adc67a44e938750a97c0982a33c0a31e78a53bbe, differing from the older pinned R01 manifest digest. It was used only as a synthetic fixture's layout package, not asserted to be the latest verified production master. Its bytes were checked unchanged. No production database/R2 document was accessed.

## Confirmed functionality

- Exact original signed-copy ID and SHA-256 are retained by both generated amendments; immutable original download bytes remained identical after both generations. Wrong document/draft identifiers and altered digests were rejected.
- Two synthetic amendments preserve numbered rental, deposit and renewal changes, their separate effective dates, three landlords, tenant company and separate representatives. Continuing-effect and signature sections are present; the full original Terms and Conditions are not reproduced.
- The final test uncovered and fixed one local addendum-only mapping defect: a single selected tenant stored in lessees[] without a legacy lessee mirror was omitted. Generation now normalizes a deep-copied snapshot only; original saved particulars and original signed copies remain unchanged.
- Email calls the existing send_standard_report_email() and is tested for exact attachment bytes, recipient/message, confirmed submission versus failure, no send on GET/cancel, stale/tampered selections, duplicate attempts and unauthorized access. Live mail transport/delivery was not attempted.
- The separate Generate Word submenu link is absent. Direct underlying review/generation routes still respond and their prior validations remain tested. Standard-lease legal-release restrictions remain intact.

## Actual browser rendering and print

Headless installed Chrome rendered the actual fixture-generated viewer HTML with local Tailwind-equivalent header styles; no public CDN or live application was contacted. Screenshots confirm Back at the right, Email/Print aligned side by side immediately underneath. Print invocation was intercepted/count-checked; Chromium print-to-PDF exercised the page's actual print CSS. Every source DOCX body paragraph was found in each PDF. All logical pages were included, navigation/actions omitted, and each sample printed as two A4 pages. Images of both samples were inspected: amendment numbering and amounts/dates are readable, with signature sections on the second page and no observed clipping.

This is a controlled local fixture, not the complete deployed site's stylesheet or a production browser. It verifies source content and print behavior, not identical Microsoft Word formatting/page boundaries.

## Native Word verification remains blocked

The installed Microsoft Word automation attempt opened no usable review PDF and stalled. Only its task-owned helper/hidden Word instance were stopped; pre-existing Word instances were left untouched. Both generated DOCX SHA-256 values remain exactly as emitted by the tested application. Native Word pagination, print-driver behavior and final professional layout still require interactive review. No renderer dependencies were installed and no unrelated Office documents were modified.

## Review artifacts

- tmp/lease-final-verification/synthetic-december-2023.docx
- tmp/lease-final-verification/synthetic-july-2024.docx
- Corresponding -browser.png screenshots and -browser-print.pdf / page PNGs.
- artifacts.json, browser-verification.json and word-verification.json record hashes and outcomes.

## Changes made in this verification stage

- app/program_billing/lease_history.py: in-memory single-tenant snapshot compatibility fix only.
- tests/test_pm_lease_final_verification.py: multi-amendment, original-association and tampered-byte regression cases.
- docs/pm_lease_final_verification.md: this report.
- Local-only synthetic artifacts and inspection script under tmp/lease-final-verification.

All earlier uncommitted Lease/Standard work remains local. Protected programme sources, models, migrations, masters and production settings remain unchanged. Existing private-storage and legal-approval gates were not enabled or bypassed.
