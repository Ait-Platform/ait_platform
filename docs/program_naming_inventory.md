# UIP and Retirement naming dependency inventory

Recorded before edits. Line numbers refer to the original working tree. Existing dirty changes are preserved.

Active dependencies include Python imports, Flask blueprint registration, render_template paths, Jinja inheritance/includes, tests, validation scripts and documentation.

Historical root patch scripts, scratch experiments/logs and the baseline-specific deployment patch are inventoried but not executed or rewritten. The prior release checkout/ZIP under artifacts remains a frozen release artifact; it is not the current application.

| File | Original matching lines | Treatment |
|---|---|---|
| `AGENT.md` |  | rename/update |
| `fix.py` | 2, 25 | historical; retain |
| `fix_base.py` | 3, 7, 18 | historical; retain |
| `docs/retirement_deployment.md` | 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36 | rename/update |
| `docs/UIP_UI_REDESIGN.md` | 29, 33, 35 | rename/update |
| `docs/UIP_PILOT_COMPLETION.md` | 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 104, 105, 106, 107, 108, 109, 110, 111 | rename/update |
| `docs/UIP_PHASE10_FINANCE.md` | 112, 113, 114, 115, 116, 117, 123, 124, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136 | rename/update |
| `docs/retirement_stage2.md` | 9, 11, 12, 13, 14, 15, 16, 17, 18, 130 | rename/update |
| `docs/retirement_integration.patch` | 40, 43 | historical; retain |
| `app/__init__.py` | 634, 635 | rename/update |
| `app/uip/__init__.py` |  | rename/update |
| `app/uip/services/__init__.py` |  | rename/update |
| `app/uip/services/work_orders.py` |  | rename/update |
| `app/uip/services/sla.py` |  | rename/update |
| `app/uip/services/routing.py` |  | rename/update |
| `app/uip/services/register.py` |  | rename/update |
| `app/uip/services/reception.py` |  | rename/update |
| `app/uip/services/providers.py` |  | rename/update |
| `app/uip/services/operations.py` |  | rename/update |
| `app/uip/services/invitations.py` |  | rename/update |
| `app/uip/services/governance.py` |  | rename/update |
| `app/uip/services/finance.py` |  | rename/update |
| `app/uip/services/documents.py` |  | rename/update |
| `app/uip/services/dashboard.py` |  | rename/update |
| `app/uip/services/audit.py` |  | rename/update |
| `app/uip/services/ai.py` |  | rename/update |
| `app/uip/secretary_routes.py` | 63, 92 | rename/update |
| `app/uip/routes.py` | 13, 14, 15, 16, 121, 123, 125, 129, 130, 173, 237, 261, 451, 483, 540, 566, 582, 646, 647, 657, 750, 752, 828, 851, 856, 878, 881, 887, 898, 910, 951, 954, 960, 968, 975, 997, 1004, 1026, 1044, 1062, 1097, 1126, 1146 | rename/update |
| `app/uip/provisioning_routes.py` | 110 | rename/update |
| `app/uip/presentation.py` | 9 | rename/update |
| `app/uip/pilot_routes.py` | 10, 11, 41, 55, 65, 86, 97 | rename/update |
| `app/uip/operational_routes.py` | 13, 14, 44, 80, 102, 111, 430, 464 | rename/update |
| `app/uip/log_privacy.py` |  | rename/update |
| `app/uip/gateway.py` | 2 | rename/update |
| `app/uip/finance_routes.py` | 11, 12, 13, 25 | rename/update |
| `app/uip/completion_routes.py` | 15, 16, 17, 65, 103, 113, 169, 183, 190, 257 | rename/update |
| `app/uip/committee_routes.py` | 114, 262, 335, 352 | rename/update |
| `patch_routing2.py` | 1, 22 | historical; retain |
| `patch_routing.py` | 3, 24 | historical; retain |
| `patch_routes3.py` | 3, 6, 35, 45 | historical; retain |
| `patch_routes2.py` | 3, 6, 34, 51 | historical; retain |
| `patch_routes.py` | 3, 17 | historical; retain |
| `patch_router.py` | 3, 7, 10, 14 | historical; retain |
| `patch_res_ui.py` | 3, 38 | historical; retain |
| `patch_res.py` | 3, 6, 11, 16 | historical; retain |
| `patch_queries.py` | 4, 7, 11, 14 | historical; retain |
| `patch_prov.py` | 3, 14 | historical; retain |
| `patch_completion.py` | 3, 36 | historical; retain |
| `patch_claims.py` | 3, 19 | historical; retain |
| `patch_view.py` | 3, 6, 21, 29 | historical; retain |
| `patch_verify_sec.py` | 3, 50 | historical; retain |
| `patch_uip_start2.py` | 1, 23 | historical; retain |
| `patch_uip_start.py` | 3, 6, 23, 27 | historical; retain |
| `patch_ui.py` | 1, 7 | historical; retain |
| `patch_sec_roles2.py` | 1, 50 | historical; retain |
| `patch_sec_roles.py` | 3, 34 | historical; retain |
| `patch_sec3.py` | 3, 28, 118 | historical; retain |
| `patch_sec2.py` | 3, 21 | historical; retain |
| `patch_sec.py` | 3, 9 | historical; retain |
| `app/retire/__init__.py` |  | rename/update |
| `scripts/validate_uip_phase49.py` | 32, 36, 50, 52 | rename/update |
| `app/retire/routes.py` | 86, 91, 100, 140, 153, 182, 193, 205, 214, 242 | rename/update |
| `scripts/refactor_program_paths.py` | 10, 11 | historical; retain |
| `templates/retire/about.html` |  | rename/update |
| `templates/retire/join.html` |  | rename/update |
| `templates/retire/entry.html` |  | rename/update |
| `templates/retire/dashboard.html` |  | rename/update |
| `templates/retire/register.html` |  | rename/update |
| `templates/retire/pending.html` |  | rename/update |
| `templates/retire/review.html` |  | rename/update |
| `templates/uip/issue_table.html` |  | rename/update |
| `templates/uip/invitations.html` | 1 | rename/update |
| `templates/retire/welcome.html` |  | rename/update |
| `templates/retire/status.html` |  | rename/update |
| `templates/uip/public_vote.html` | 1 | rename/update |
| `templates/uip/public_about.html` |  | rename/update |
| `templates/uip/provisioning.html` | 1 | rename/update |
| `templates/uip/work_orders/view.html` | 1 | rename/update |
| `templates/uip/interactions/view.html` | 1 | rename/update |
| `templates/uip/work_orders/provider_view.html` | 1 | rename/update |
| `templates/uip/work_orders/list.html` | 1 | rename/update |
| `templates/uip/help.html` | 1 | rename/update |
| `templates/uip/waiting_lounge.html` | 1 | rename/update |
| `templates/uip/validation_error.html` | 1 | rename/update |
| `templates/uip/setup.html` | 1 | rename/update |
| `templates/uip/service_status.html` | 1 | rename/update |
| `templates/uip/service_standards.html` | 1 | rename/update |
| `templates/uip/router.html` | 1 | rename/update |
| `templates/uip/relationship_dates.html` |  | rename/update |
| `templates/uip/register_pagination.html` |  | rename/update |
| `templates/uip/register_import.html` | 1 | rename/update |
| `templates/uip/reception/new_interaction.html` | 1, 17 | rename/update |
| `templates/uip/providers/view.html` | 1 | rename/update |
| `templates/uip/providers/list.html` | 1 | rename/update |
| `templates/uip/providers/form.html` | 1 | rename/update |
| `templates/uip/finance/_year.html` |  | rename/update |
| `templates/uip/finance/_transactions.html` |  | rename/update |
| `templates/uip/finance/_fields.html` |  | rename/update |
| `templates/uip/finance/_budget_form.html` | 1 | rename/update |
| `templates/uip/finance/_budget.html` |  | rename/update |
| `templates/uip/finance/transactions.html` | 1 | rename/update |
| `templates/uip/finance/overview.html` | 1, 2, 5, 6 | rename/update |
| `templates/uip/finance/form.html` | 1 | rename/update |
| `templates/uip/finance/detail.html` | 1, 2 | rename/update |
| `templates/uip/finance/commitments.html` | 1 | rename/update |
| `templates/uip/finance/budget.html` | 1 | rename/update |
| `templates/uip/properties/view.html` | 1, 4, 6 | rename/update |
| `templates/uip/properties/list.html` | 1, 4 | rename/update |
| `templates/uip/properties/form.html` | 1 | rename/update |
| `templates/uip/price.html` |  | rename/update |
| `templates/uip/dashboards/subcommittee.html` | 1 | rename/update |
| `templates/uip/dashboards/secretary_workspace.html` | 1 | rename/update |
| `templates/uip/dashboards/resolution_view.html` | 1 | rename/update |
| `templates/uip/dashboards/resident.html` | 1 | rename/update |
| `templates/uip/dashboards/reports.html` | 1 | rename/update |
| `templates/uip/dashboards/receptionist.html` | 1, 18 | rename/update |
| `templates/uip/dashboards/public.html` | 1 | rename/update |
| `templates/uip/dashboards/process_claims.html` | 1 | rename/update |
| `templates/uip/dashboards/municipal_officer.html` | 1 | rename/update |
| `templates/uip/dashboards/manager.html` | 1, 133 | rename/update |
| `templates/uip/dashboards/committee.html` | 1 | rename/update |
| `templates/uip/base_public.html` | 10 | rename/update |
| `templates/uip/base.html` | 6, 11 | rename/update |
| `templates/uip/operations/staff_links.html` |  | rename/update |
| `templates/uip/operations/page.html` | 1 | rename/update |
| `templates/uip/operations/issues.html` | 1, 5 | rename/update |
| `templates/uip/navigation.html` |  | rename/update |
| `templates/uip/my_access.html` | 1 | rename/update |
| `templates/uip/audit/list.html` | 1, 11 | rename/update |
| `templates/uip/assistant.html` | 1 | rename/update |
| `templates/uip/ai_wallet.html` | 1 | rename/update |
| `templates/uip/admin/settings.html` | 1 | rename/update |
| `templates/uip/activate_committee.html` | 1 | rename/update |
| `templates/uip/access_denied.html` | 1 | rename/update |
| `templates/uip/members/view.html` | 1, 9, 10 | rename/update |
| `templates/uip/members/list.html` | 1, 4 | rename/update |
| `templates/uip/members/form.html` | 1 | rename/update |
| `templates/uip/manage_committee.html` | 1 | rename/update |
| `tests/uip/test_completion.py` | 59, 101, 211, 212 | rename/update |
| `tests/uip/test_audit.py` | 5, 82, 113 | rename/update |
| `tests/uip/phase3_helpers.py` | 3 | rename/update |
| `tests/uip/conftest.py` | 17 | rename/update |
| `tests/uip/check_menu_repair.py` | 12 | rename/update |
| `tests/retire/check_stage2_local.py` | 30 | rename/update |
| `tests/retire/check_stage2.py` | 28, 202, 428 | rename/update |
| `tests/uip/test_phase49_concurrency.py` | 4 | rename/update |
| `tests/uip/test_phase49.py` | 9, 10, 252 | rename/update |
| `tests/uip/test_log_privacy.py` | 5 | rename/update |
| `tests/uip/test_intake_search.py` | 5 | rename/update |
| `tests/uip/test_finance.py` | 12, 305 | rename/update |
| `tests/uip/test_register.py` | 5 | rename/update |
| `tests/uip/test_redesign.py` | 8, 9 | rename/update |
| `tests/uip/test_providers.py` | 4 | rename/update |
| `tests/uip/test_pilot.py` | 14, 223 | rename/update |
| `tests/uip/test_work_orders.py` | 4 | rename/update |
| `tests/uip/test_sign_out.py` | 33, 35 | rename/update |
| `tests/uip/test_work_order_access.py` | 3, 67 | rename/update |
| `tests/uip/test_work_order_concurrency.py` | 15 | rename/update |
| `UIP_OPERATIONAL_COMPLETION.md` | 21, 22, 23, 24, 25, 26, 27, 28, 29, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58 | rename/update |
| `scratch/add_ai_button.py` | 3, 37 | historical; retain |
| `scratch/add_uip_settings.py` | 3, 30, 34 | historical; retain |
| `scratch/add_uip_price_route2.py` | 3, 9, 31 | historical; retain |
| `scratch/add_uip_price_route.py` | 3, 34, 41 | historical; retain |
| `scratch/add_uip_ix.py` | 3, 49, 53 | historical; retain |
| `scratch/append_reports.py` | 11, 17 | historical; retain |
| `scratch/append_ai_route.py` | 1 | historical; retain |
| `scratch/append_uip_route.py` | 2 | historical; retain |
| `scratch/append_routes.py` | 25 | historical; retain |
| `scratch/create_uip_price.py` | 56 | historical; retain |
| `scratch/fix_bom2.py` | 1, 6 | historical; retain |
| `scratch/fix_bom.py` | 1, 8 | historical; retain |
| `scratch/fix_init.py` | 3, 8 | historical; retain |
| `scratch/fix_imports.py` | 3, 8 | historical; retain |
| `scratch/fix_gateway.py` | 3, 8 | historical; retain |
| `scratch/fix_quotes.py` | 3, 8 | historical; retain |
| `scratch/fix_template.py` | 2, 7 | historical; retain |
| `scratch/fix_uip_price_html.py` | 3 | historical; retain |
| `scratch/fix_uip_price_bug.py` | 3 | historical; retain |
| `scratch/fix_uip_init.py` | 3 | historical; retain |
| `scratch/patch_403.py` | 3, 24 | historical; retain |
| `scratch/noop.py` | 4 | historical; retain |
| `scratch/patch_uip_init_commercial.py` | 3, 38 | historical; retain |
| `scratch/patch_uip_init.py` | 3, 36 | historical; retain |
| `scratch/patch_uip_about_form.py` | 3, 37, 40 | historical; retain |
| `scratch/patch_uip_about_flashes.py` | 3, 35, 38 | historical; retain |
| `scratch/patch_uip_about_cta3.py` | 3, 26, 29 | historical; retain |
| `scratch/patch_uip_about_cta2.py` | 3, 32, 35 | historical; retain |
| `scratch/patch_uip_about_cta.py` | 3, 22, 25 | historical; retain |
| `scratch/patch_uip_about2.py` | 3, 30, 33 | historical; retain |
| `scratch/patch_uip_about.py` | 3, 16 | historical; retain |
| `scratch/patch_routes.py` | 3, 13, 43, 70, 111, 115, 120, 125, 129 | historical; retain |
| `scratch/patch_role.py` | 3, 47 | historical; retain |
| `scratch/patch_uip_start.py` | 3, 15, 19 | historical; retain |
| `scratch/patch_uip_routes.py` | 3, 8, 16, 29, 32 | historical; retain |
| `scratch/patch_uip_price.py` | 3, 25, 36 | historical; retain |
| `scratch/register_uip_bp.py` | 6, 8 | historical; retain |
| `scratch/strip_null2.py` | 3, 15 | historical; retain |
| `scratch/strip_null.py` | 3, 13 | historical; retain |
| `scratch/test_stage3_5.py` | 9 | historical; retain |
| `scratch/test_stage3.py` | 15, 16, 161 | historical; retain |
| `scratch/test_stage2.py` | 14, 15, 128 | historical; retain |
| `scratch/update_auto_patch_more.py` | 3 | historical; retain |
| `scratch/update_auto_patch_dates.py` | 3 | historical; retain |
| `scratch/update_auto_patch.py` | 3 | historical; retain |
| `scratch/update_dashboard_routing.py` | 3, 7, 12 | historical; retain |
| `scratch/update_interaction3.py` | 3 | historical; retain |
| `scratch/update_interaction2.py` | 3 | historical; retain |
| `scratch/update_interaction.py` | 3 | historical; retain |
| `scratch/update_metric2.py` | 3 | historical; retain |
| `scratch/update_metric.py` | 3 | historical; retain |
| `scratch/update_manager.py` | 3 | historical; retain |
| `scratch/update_resident.py` | 3, 14 | historical; retain |
| `scratch/update_uip_datalist.py` | 3 | historical; retain |
| `scratch/update_uip_about.py` | 62 | historical; retain |
| `scratch/update_uip_routes.py` | 3, 14 | historical; retain |
| `scratch/update_view_extended.py` | 3, 50 | historical; retain |

## Scan limitations

rg: D:\Users\yeshk\Documents\ait_platform\.pytest_cache: Access is denied. (os error 5)
rg: D:\Users\yeshk\Documents\ait_platform\templates\program_adv_math: Access is denied. (os error 5)
rg: D:\Users\yeshk\Documents\ait_platform\scratch\uip_ux_check_35919cb8a92e49cb8c2bdd5eb9caf0d2: Access is denied. (os error 5)
rg: D:\Users\yeshk\Documents\ait_platform\scratch\uip_validation_d705b4264c854974a6c241fd8fa45b50: Access is denied. (os error 5)
rg: D:\Users\yeshk\Documents\ait_platform\scratch\uip_ux_check_3ac0fc68b22c4995a52b2aa7412fe72a: Access is denied. (os error 5)
rg: D:\Users\yeshk\Documents\ait_platform\scratch\uip_ux_check_f98724d379aa434e8375c9912085a5c7: Access is denied. (os error 5)
