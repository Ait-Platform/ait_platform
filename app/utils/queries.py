BRIDGE_QUERY = """
WITH globals AS (
  SELECT CASE WHEN EXISTS (
    SELECT 1
    FROM auth_approved_admin aa
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
      SELECT 1
      FROM user_enrollment ue
      JOIN "user" u ON u.id = ue.user_id
      WHERE ue.subject_id  = s.id
        AND ue.status      = 'active'
        AND lower(u.email) = lower(:email)
    ) THEN 'enrolled'
    ELSE 'locked'
  END AS access_level
FROM auth_subject s
WHERE
  (
    -- global admin sees all subjects
    (SELECT is_admin_global FROM globals) = 1
    -- or user is actively enrolled in the subject
    OR EXISTS (
        SELECT 1
        FROM user_enrollment ue
        JOIN "user" u ON u.id = ue.user_id
        WHERE ue.subject_id  = s.id
          AND ue.status      = 'active'
          AND lower(u.email) = lower(:email)
    )
  )
ORDER BY s.name
"""
