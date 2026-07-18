import re

with open('app/utils/billing_metsoa_builder.py', 'r', encoding='utf-8') as f:
    text = f.read()

get_cons_rows = '''def _get_consumption_rows(property_id, month_str):
    return db.session.execute(text("""
      SELECT
        c.meter_id                         AS meter_id,
        COALESCE(m.meter_number, CAST(m.id AS TEXT)) AS meter_label,
        LOWER(m.utility_type)              AS utility_type,
        c.last_date                        AS prev_date,
        c.last_read                        AS prev_value,
        c.new_date                         AS curr_date,
        c.new_read                         AS curr_value,
        c.days                             AS days,
        c.consumption                      AS consumption
      FROM bil_consumption c
      JOIN bil_meter m ON m.id = c.meter_id
      JOIN bil_sectional_unit su ON su.id = m.sectional_unit_id
      WHERE su.property_id = :pid AND c.month = :m
      ORDER BY CASE WHEN LOWER(m.utility_type) LIKE 'elec%%' THEN 0 ELSE 1 END,
               m.meter_number
    """), {"pid": property_id, "m": month_str}).mappings().all()'''

# Replace the whole function body using split/join or careful regex
start_idx = text.find('def _get_consumption_rows(tenant_id, month_str):')
end_idx = text.find('def _get_electric_rate(tariffs):')

text = text[:start_idx] + get_cons_rows + "\n\n" + text[end_idx:]

# Replace arguments in build functions
text = text.replace('def build_metsoa_payload(tenant_id, month_str):', 'def build_metsoa_payload(property_id, month_str):')
text = text.replace('base = _get_consumption_rows(tenant_id, month_str)', 'base = _get_consumption_rows(property_id, month_str)')
text = text.replace('def build_electrical_rows(tenant_id, month_str):', 'def build_electrical_rows(property_id, month_str):')
text = text.replace('build_metsoa_payload(tenant_id, month_str)', 'build_metsoa_payload(property_id, month_str)')
text = text.replace('def build_water_rows(tenant_id, month_str):', 'def build_water_rows(property_id, month_str):')

with open('app/utils/billing_metsoa_builder.py', 'w', encoding='utf-8') as f:
    f.write(text)

print('Updated billing_metsoa_builder.py')
