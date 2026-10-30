-- Read-only production verification. No inserts, updates, or reconciliation.
-- Municipal Accounts opening/latest summaries and Readings both use bil_meter_reading.
WITH expected(account_number,utility,meter_number,reading_value,reading_date) AS (
 VALUES ('83192896040','Water','CEL888',0::numeric,DATE '2026-09-23'),
        ('83192896040','Electricity','2188802S',30025::numeric,DATE '2026-08-26'),
        ('83347721969','Water','CXN998',614::numeric,DATE '2026-09-11'),
        ('83347721969','Electricity','7150696S',49577::numeric,DATE '2026-08-26')
)
SELECT e.*,a.id AS account_id,a.property_id AS account_property_id,
       m.id AS meter_id,m.property_id AS meter_property_id,
       count(r.id) FILTER (WHERE r.reading_value=e.reading_value) AS matching_canonical_readings,
       count(r.id) FILTER (WHERE r.reading_value<>e.reading_value) AS conflicting_same_date_readings
FROM expected e
LEFT JOIN bil_muni_account a ON a.account_number=e.account_number AND a.property_id=26
LEFT JOIN bil_meter m ON m.id=CASE WHEN e.utility='Water' THEN a.water_meter_id ELSE a.elec_meter_id END
 AND m.meter_number=e.meter_number
LEFT JOIN bil_meter_reading r ON r.meter_id=m.id AND r.reading_date=e.reading_date
GROUP BY e.account_number,e.utility,e.meter_number,e.reading_value,e.reading_date,a.id,a.property_id,m.id,m.property_id
ORDER BY e.account_number,e.utility;
-- Expected: registered meter IDs, both properties 26, matching=1, conflicting=0.
-- Missing/wrong account or meter links, matching=0, matching>1, or conflicting>0
-- require separate historical review. Never automatically insert/backfill from this report.
