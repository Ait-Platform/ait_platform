BRIDGE_QUERY = """
WITH globals AS (
  SELECT CASE WHEN EXISTS (
    SELECT 1 FROM auth_approved_admin aa
    WHERE lower(aa.email) = lower(:email)
  ) THEN 1 ELSE 0 END AS is_admin_global
)
SELECT
  s.id,
  s.slug,
  s.name,
  CASE
    WHEN (SELECT is_admin_global FROM globals) = 1 THEN 'admin'
    WHEN EXISTS (
      SELECT 1 FROM auth_subject_admin sa
      WHERE sa.subject_id = s.id
        AND lower(sa.email) = lower(:email)
    ) THEN 'admin'
    WHEN EXISTS (
      SELECT 1
      FROM user_enrollment ue
      WHERE ue.subject_id = s.id
        AND ue.status     = 'active'
        AND ue.user_id IN (
          SELECT id FROM "user"
          WHERE lower(email) = lower(:email)
        )
    ) THEN 'enrolled'
    ELSE 'locked'
  END AS access_level
FROM auth_subject s
WHERE
  (
    (SELECT is_admin_global FROM globals) = 1
    OR EXISTS (
        SELECT 1
        FROM auth_subject_admin sa
        WHERE sa.subject_id = s.id
          AND lower(sa.email) = lower(:email)
    )
    OR EXISTS (
        SELECT 1
        FROM user_enrollment ue
        WHERE ue.subject_id = s.id
          AND ue.status     = 'active'
          AND ue.user_id IN (
            SELECT id FROM "user"
            WHERE lower(email) = lower(:email)
          )
    )
  )
ORDER BY s.name;
"""
