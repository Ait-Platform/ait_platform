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
        AND lower(u.email) = lower(:email)
        AND (
           s.commercial_mode = 'free' OR
           s.requires_price = 0 OR
           ue.status IN ('active', 'started', 'enrolled', 'paid', 'completed') OR
           (ue.trial_end IS NOT NULL AND ue.trial_end > CURRENT_TIMESTAMP) OR
           (ue.expires_at IS NOT NULL AND ue.expires_at > CURRENT_TIMESTAMP)
        )
    ) THEN 'enrolled'
    ELSE 'locked'
  END AS access_level
FROM auth_subject s
WHERE
  s.is_active = 1
  AND (
    (s.is_hidden_on_bridge IS NULL OR s.is_hidden_on_bridge = FALSE)
    OR (SELECT is_admin_global FROM globals) = 1
    OR EXISTS (
        SELECT 1
        FROM user_enrollment ue
        JOIN "user" u ON u.id = ue.user_id
        WHERE ue.subject_id = s.id
          AND lower(u.email) = lower(:email)
    )
  )
ORDER BY s.name
"""
