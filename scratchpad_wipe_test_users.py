from app import create_app, db
from app.models.auth import User
from sqlalchemy import text

app = create_app()

with app.app_context():
    admin_email = "ait@mathwithhands.com"
    test_users = User.query.filter(User.email != admin_email).all()
    
    if not test_users:
        print("No test users found to delete.")
    else:
        user_ids = [u.id for u in test_users]
        user_ids_str = ','.join(map(str, user_ids))
        
        print(f"Deleting data for {len(user_ids)} test users...")
        
        # We need to delete from dependent tables first to avoid FK constraints.
        # This list covers all known tables referencing the user or their enrollment/biodata.
        
        # 1. Things depending on enrollment or submission
        queries = [
            f"DELETE FROM cfi_showcase_votes WHERE user_id IN ({user_ids_str});",
            f"DELETE FROM cfi_segment_items WHERE enrollment_id IN (SELECT id FROM user_enrollment WHERE user_id IN ({user_ids_str}));",
            f"DELETE FROM cfi_judge_assignment WHERE user_id IN ({user_ids_str}) OR enrollment_id IN (SELECT id FROM user_enrollment WHERE user_id IN ({user_ids_str}));",
            f"DELETE FROM cfi_group_members WHERE enrollment_id IN (SELECT id FROM user_enrollment WHERE user_id IN ({user_ids_str}));",
            f"DELETE FROM cfi_groups WHERE leader_id IN (SELECT id FROM user_enrollment WHERE user_id IN ({user_ids_str}));",
            f"DELETE FROM cfi_submission_participants WHERE enrollment_id IN (SELECT id FROM user_enrollment WHERE user_id IN ({user_ids_str}));",
            f"DELETE FROM cfi_talent_files WHERE submission_id IN (SELECT id FROM cfi_talent_submission WHERE user_id IN ({user_ids_str}));",
            f"DELETE FROM cfi_talent_submission WHERE user_id IN ({user_ids_str}) OR user_enrollment_id IN (SELECT id FROM user_enrollment WHERE user_id IN ({user_ids_str}));",
            f"DELETE FROM cfi_sponsorships WHERE user_id IN ({user_ids_str}) OR participant_id IN (SELECT id FROM user_enrollment WHERE user_id IN ({user_ids_str}));",
            f"DELETE FROM cfi_supporters WHERE user_id IN ({user_ids_str}) OR participant_id IN (SELECT id FROM user_enrollment WHERE user_id IN ({user_ids_str}));",
            f"DELETE FROM cfi_parent WHERE parent_id IN ({user_ids_str}) OR child_id IN ({user_ids_str});",
            f"DELETE FROM auth_subscriptions WHERE user_id IN ({user_ids_str});",
            f"DELETE FROM auth_payment_log WHERE user_id IN ({user_ids_str});",
            f"DELETE FROM user_roles WHERE user_id IN ({user_ids_str});",
            f"DELETE FROM auth_baton WHERE user_id IN ({user_ids_str});",
            f"DELETE FROM user_entitlement WHERE user_id IN ({user_ids_str});",
            f"DELETE FROM user_program WHERE user_id IN ({user_ids_str});",
            f"DELETE FROM adv_math_progress WHERE user_id IN ({user_ids_str});",
            f"DELETE FROM loss_result WHERE user_id IN ({user_ids_str});",
            
            # 2. Delete Enrollments BEFORE Biodata (since Enrollment has biodata_id)
            f"DELETE FROM user_enrollment WHERE user_id IN ({user_ids_str});",
            
            # 3. Delete Biodata
            f"DELETE FROM cfi_biodata WHERE user_id IN ({user_ids_str});",
            
            # 4. Finally, delete the Users
            f"DELETE FROM \"user\" WHERE id IN ({user_ids_str});"
        ]
        
        try:
            for q in queries:
                db.session.execute(text(q))
            db.session.commit()
            print("Successfully wiped all test users and their related records. Your DB is clean and ready for export!")
        except Exception as e:
            db.session.rollback()
            print("Error during deletion:")
            print(e)
