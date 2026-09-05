import re

file_path = 'app/subject_reading/routes.py'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# Fix is_orphaned bug
old_orphaned = '''
        is_orphaned = (ue_status is None)
        is_stale_paid = (ue_status == "active" and row.get("completed_at") is not None)
'''
new_orphaned = '''
        is_sace = False
        from flask import session
        for s in session.get('admin_subjects', []):
            if s.startswith('sace'):
                is_sace = True
                break
        is_orphaned = (ue_status is None) and not is_sace
        is_stale_paid = (ue_status == "active" and row.get("completed_at") is not None)
'''
text = text.replace(old_orphaned, new_orphaned)

# Fix _finalize_and_send_certificate
old_finalize = '''def _finalize_and_send_certificate(user_id: int):
    enr = _get_enrollment()
    if not enr:
        abort(400)'''
new_finalize = '''def _finalize_and_send_certificate(user_id: int):
    enr = _get_enrollment()
    if not enr:
        # Gracefully ensure enrollment if missing
        _ensure_enrollment_row()
        enr = _get_enrollment()
        if not enr:
            abort(400)'''
text = text.replace(old_finalize, new_finalize)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
