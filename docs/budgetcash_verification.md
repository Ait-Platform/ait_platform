# BudgetCash readiness verification

18 November 2026. **BudgetCash is not ready for independent external user testing.** The existing manual cashbook works in isolated testing after the focused corrections below. Four reproducible failures and unresolved commercial wording remain.

| Area | Result and evidence |
|---|---|
| Welcome, information and pricing | Actual templates and BudgetCash routes render and link to shared registration. Live subject visibility, country quotes and Render configuration were not verified. |
| Registration | Actual shared register GET/POST accepts valid details, rejects short passwords, stages a hashed password and leads to the enrolment/payment decision. The real shared user/enrolment helpers persist one identity, one wallet and one pending enrolment across fresh sessions. A production payment or trial activation was not performed. |
| Returning users | The actual shared returning-registrant password path authenticates an existing test account and opens its dashboard. Session removal, sign-out and sign-in simulations preserve saved cashbook entries. The complete production login/payment journey remains unverified. |
| Accounts and categories | Create income, expense, asset and liability accounts, assign a reporting group, restore the 18 existing defaults, and repeat restoration without duplicates. Duplicate account names produce a warning. |
| Budget setup and periods | The implementation is an account cashbook with dated snapshots and monthly reports. No saved budget-allocation plan or budget-versus-actual target model/form exists. These features were not added. |
| Income and expenses | Capture, retrieve and edit the existing account/date/amount fields. Two test captures at a fixture tariff of 2 tokens reduce the test wallet from 100 to 96 tokens. Edits retain the existing charging behavior. |
| Calculations | Independently checked income 1234.56, amended expenses 200.01 and net cash movement 1034.55. Asset snapshot 1000.00 less liability snapshot 250.00 gives net worth 750.00. Monthly group results and empty reports pass. |
| Reports | Financial statements and group reports preview. Print calls the browser print function. Chromium print-to-PDF produces a PDF containing the expected 432.10 figure. There is no application PDF/download endpoint. Native Windows printing was not tested. |
| Persistence | Fresh SQLAlchemy sessions and authenticated requests retrieve saved accounts, entries, enrolments and balances. No live user records were used. |
| Isolation and security | Anonymous dashboard/summary access is blocked. Tests reject foreign account IDs for entry creation, entry edits and snapshot insertion. Reads and financial statements exclude mismatched user/account records. Missing CSRF tokens are rejected, and the browser test uses HTTPS with CSRF enabled. |
| Desktop and mobile | Actual programme templates render with the existing white cards, amber strip, borders and navigation. Screenshots at 1280px and 390px show usable content without horizontal page overflow. Shared platform navigation is suppressed in this isolated browser harness. |
| Error handling | Missing account/entry fields, duplicate account names, invalid snapshot amounts, invalid report months and empty data have been exercised. Repeated cashbook form submission remains a known failure. |

Focused corrections made in `app/program_budget/routes.py` and `services.py`:

- Validate account ownership before a ledger insert, reassignment or account snapshot write. Reject unauthorized IDs before any token deduction.
- Apply matching owner predicates to account/ledger and account/snapshot joins, preserving existing records while excluding inconsistent cross-user associations from display and totals.
- Require authentication for the old summary endpoint and scope its underlying lookup to the authenticated user.
- Include account names in the financial-statement query. Previously, any non-zero snapshot reached `a['name']` after a query selecting only `id, kind`, raising `NoSuchColumnError`.
- Break equal-date snapshot ties by record ID. Normalize malformed group-report months using the existing statement-report fallback pattern.
- Catch invalid numeric snapshot inputs before changing saved data, display a warning and return to the existing form.

The four expected-failure regressions deliberately remain visible:

1. **CSV import fails.** `import_statement()` filters `BudLedger.ext_ref`, but that mapped property does not exist. Its insert also supplies `ext_ref` and `balance_cents`, omits required `user_id`/`account_id`, and provides no account-mapping workflow. `/budget/import` redirects to the manual ledger, so the import form is not accessible through that entry point. Restoring statement import requires a reviewed reconciliation of the legacy import contract and current schema/workflow.
2. **External summary fails with data.** `budget_summary()` reads `balance_cents` from `BudLedger`, which has no such mapped field. Empty or other-user-only external data returns zeros safely after the isolation fix. Non-empty owned external data still raises `AttributeError`.
3. **Shared registration lacks server-side email validation.** A direct BudgetCash registration POST with `invalid-email` and a sufficiently long password proceeds to the decision screen. Browser email input validation does not cover crafted requests. The shared auth implementation was not changed because it serves other AIT programmes.
4. **Repeated submission creates duplicate entries and deductions.** Two identical cashbook POSTs persist two entries and charge twice. The existing workflow has no submission identity or idempotency mechanism. Distinguishing retries from legitimate same-day equal payments requires a focused design before implementing a prevention rule.

Commercial and remaining security decisions required before public testing:

- About advertises a free trial followed by a monthly fee. Pricing advertises once-off registration, token billing, 200 starting tokens and 10 tokens per snapshot. The actual shared signup helper grants 100 tokens. Ledger capture uses the configured `budget/ledger_entry` tariff with a code fallback of 2; snapshot routes contain no token deduction. Production tariff values were not read. No prices, grants or charging rules were changed in this review.
- Confirm the approved registration/trial/payment arrangement and repair the public wording or implementation accordingly. Complete an isolated real Paystack test-mode onboarding only after that arrangement is established.
- Review wallet concurrency and retry behavior. Ledger capture reads and decrements a wallet without row locking, and token deduction was not load-tested across workers.
- Several ancillary `next` parameters accept protocol-relative URLs, and monetary parsing uses mixed float rounding/string truncation. These were recorded for a separately scoped security/validation pass; no new money-rounding policy was invented.
- ORM account mappings omit extra fields used by the SQL routes. Those fields are present in the inspected local schema. Production schema compatibility remains unverified. No schema changes or migration execution are part of these corrections.

Verification used the current real BudgetCash routes, models, templates and shared persistence helpers with **session-temporary PostgreSQL tables in verified localhost `ait_local_db`**. Every account, ledger, snapshot, wallet, enrolment and user created by the tests is temporary. Temporary table identities use their own sequences. No factory boot maintenance, production migrations, live payments, support emails or production financial writes were executed. Shared activity routing and configured country-quote responses are controlled boundaries in the harness.

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_budget_review.py tests/test_auth_bridge_redirects.py tests/test_startup_lazy_imports.py -q --tb=short --disable-warnings
```

Result: **31 passed and 4 expected failures**, including **15 BudgetCash passes and the 4 documented BudgetCash failures**. The expected failures are not readiness passes. Existing warning output remains. Programme screenshots and the test print PDF are local QA files under `tmp/budget-review/` and are not committed.

The separate Bridge pause is commit `09a04dbf`. It removes only the Franchise Opportunities entry and updates its visibility regression. Franchisee routes, registration, Admin Register, records and environment settings remain available. The BudgetCash patch requires **no migration or Render configuration change**. After deployment, manual smoke checks of the actual country-price service, approved enrolment/payment path and current production schema are still required.
