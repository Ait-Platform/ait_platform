# PM billing admin template audit

The confirmed production fault was `TemplateNotFound: admin/billing/tariffs.html`.
Commit `cc5f9bd6` moved the billing admin templates under `templates/admin/programs/billing/` without updating these route references.

The audit inspected all Python files under `app/admin/programs/billing/`: 39 literal render call sites were found. The prior landing fix already corrected one; this patch corrects 35 more call sites (31 distinct names). Resolved targets and their static Jinja dependencies are tracked by Git and parse/load successfully. The billing-scoped rendering wrapper reuses the existing PM template loader, including its repository-root fallback. It does not change the application-wide Jinja loader or create aliases/duplicate templates.

## Corrected references

| Obsolete reference | Existing tracked target |
| --- | --- |
| `admin/billing/data/index.html` | `templates/admin/programs/billing/data/index.html` |
| `admin/billing/export_picker.html` | `templates/admin/programs/billing/export_picker.html` |
| `admin/billing/index.html` | `templates/admin/programs/billing/index.html` |
| `admin/billing/invoices.html` | `templates/admin/programs/billing/invoices.html` |
| `admin/billing/meters.html` | `templates/admin/programs/billing/meters.html` |
| `admin/billing/muni/capture.html` | `templates/admin/programs/billing/muni/capture.html` |
| `admin/billing/muni/exports.html` | `templates/admin/programs/billing/muni/exports.html` |
| `admin/billing/muni/index.html` | `templates/admin/programs/billing/muni/index.html` |
| `admin/billing/muni/ledger.html` | `templates/admin/programs/billing/muni/ledger.html` |
| `admin/billing/muni/print_ledger.html` | `templates/admin/programs/billing/muni/print_ledger.html` |
| `admin/billing/muni/recon_statement.html` | `templates/admin/programs/billing/muni/recon_statement.html` |
| `admin/billing/occupants/form.html` | `templates/admin/programs/billing/occupants/form.html` |
| `admin/billing/occupants/index.html` | `templates/admin/programs/billing/occupants/index.html` |
| `admin/billing/occupants/show.html` | `templates/admin/programs/billing/occupants/show.html` |
| `admin/billing/payment_form.html` | `templates/admin/programs/billing/payment_form.html` |
| `admin/billing/payment_new.html` | `templates/admin/programs/billing/payment_new.html` |
| `admin/billing/readings_consumption.html` | `templates/admin/programs/billing/readings_consumption.html` |
| `admin/billing/readings_enter.html` | `templates/admin/programs/billing/readings_enter.html` |
| `admin/billing/readings_view.html` | `templates/admin/programs/billing/readings_view.html` |
| `admin/billing/recurring_form.html` | `templates/admin/programs/billing/recurring_form.html` |
| `admin/billing/recurring_index.html` | `templates/admin/programs/billing/recurring_index.html` |
| `admin/billing/reports.html` | `templates/admin/programs/billing/reports.html` |
| `admin/billing/tariffs.html` | `templates/admin/programs/billing/tariffs.html` |
| `admin/billing/tenant_item_edit.html` | `templates/admin/programs/billing/tenant_item_edit.html` |
| `admin/billing/tenant_ledger.html` | `templates/admin/programs/billing/tenant_ledger.html` |
| `admin/billing/tenant_soa.html` | `templates/admin/programs/billing/tenant_soa.html` |
| `admin/billing/tenants.html` | `templates/admin/programs/billing/tenants.html` |
| `admin/billing/units.html` | `templates/admin/programs/billing/units.html` |
| `admin/billing/units/index.html` | `templates/admin/programs/billing/units/index.html` |
| `admin/billing/units/new.html` | `templates/admin/programs/billing/units/new.html` |
| `admin/muni/recon.html` | `templates/admin/programs/billing/muni/recon.html` |

## Unresolved source material - not substituted

These files are absent from the repository, including the moved template tree; the available Git history does not establish a replacement. Their calls are intentionally unchanged. These particular workflows can still fail with TemplateNotFound and require a separately scoped recovery of their intended templates.

| Function / URL | Unavailable reference |
| --- | --- |
| `readings_export`, HTML branch of `/admin/readings/export` | `aadmin_bp/billing/readings_export.html` |
| `metsoa_water_config`, `/admin/billing/metsoa/water/<meter_id>` | `admin/billing/metsoa_water_config.html` |
| `tenants_edit`, `/admin/billing/tenants/<tenant_id>/edit` | `billing/tenant/form.html` |

The audit test permits only these explicit existing gaps and fails for additional missing literal targets or dependencies. It does not claim these gaps have been repaired.

## Verification and limits

- Isolated local PostgreSQL identity, tariff and platform-settings fixtures; no application factory or production connection.
- Actual admin guard and GET handlers: tariffs, units, meters, invoices, reports, readings landing, data hub and new-unit GET.
- Tariff/settings row snapshots unchanged; the affected GET test records SELECT statements only. Existing settings are seeded in the isolated fixture because the unchanged legacy tariff handler creates settings when absent.
- Authenticated browser GETs check tariffs and five other moved pages, displayed saved tariff values, CSRF form fields and Back navigation. Anonymous affected GETs still redirect through the existing guard.
- Every resolved template is checked for Git tracking, loadability and static Jinja dependencies. This is not an end-to-end execution of every financial action or every template branch.
- Normalised AST comparison against the parent source confirms that only renderer calls and verified template-name literals changed; SQL, validation, tariff calculations, financial writes, access controls and route registrations are unchanged.
- No template content, models, migrations or excluded modules were changed. No production data writes or migrations were performed. Render authenticated page and destination verification remains outstanding.
