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
    ELSE 'enrolled'
  END AS access_level
FROM auth_subject s
WHERE
  s.is_active = 1
  AND (
    (SELECT is_admin_global FROM globals) = 1
    OR EXISTS (
      SELECT 1
      FROM user_enrollment ue
      JOIN "user" u ON u.id = ue.user_id
      WHERE ue.subject_id  = s.id
        AND lower(u.email) = lower(:email)
        AND (
           ue.status IN ('active', 'started', 'enrolled', 'paid', 'completed', 'teacher') OR
           (ue.trial_end IS NOT NULL AND ue.trial_end > CURRENT_TIMESTAMP) OR
           (ue.expires_at IS NOT NULL AND ue.expires_at > CURRENT_TIMESTAMP)
        )
    )
  )
  AND (
    COALESCE(s.program_type, '') != 'admin'
    OR (SELECT is_admin_global FROM globals) = 1
  )
ORDER BY s.name
"""
