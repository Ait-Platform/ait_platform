INSERT INTO auth_subject (
    slug, name, is_active, program_type, commercial_mode, enroll_policy, processor_default
) VALUES 
    ('sace_evaluator', 'SACE Evaluation Portal', 1, 'course', 'free', 'auto_enroll', 'yoco'),
    ('sace_facilitator', 'SACE Workshop Facilitator', 1, 'course', 'free', 'auto_enroll', 'yoco'),
    ('sace_participant', 'SACE CPTD Reading Activity', 1, 'course', 'free', 'auto_enroll', 'yoco');
