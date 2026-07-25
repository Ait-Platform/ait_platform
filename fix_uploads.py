import re

with open('app/program_culturalfire/routes.py', 'r') as f:
    content = f.read()

mod_code = '''
            from app.program_culturalfire.helpers import moderate_video_with_gemini
            if file.filename.lower().endswith(('.mp4', '.mov', '.avi', '.mkv')):
                if not moderate_video_with_gemini(filepath):
                    import os
                    if os.path.exists(filepath):
                        os.remove(filepath)
                    flash("Upload rejected: Inappropriate content detected by AI moderator.", "danger")
                    return redirect(request.url)
'''

mod_code_ad = '''
    from app.program_culturalfire.helpers import moderate_video_with_gemini
    if file.filename.lower().endswith(('.mp4', '.mov', '.avi', '.mkv')):
        if not moderate_video_with_gemini(file_path):
            import os
            if os.path.exists(file_path):
                os.remove(file_path)
            flash("Upload rejected: Inappropriate content detected by AI moderator.", "danger")
            return redirect(url_for('cultural_bp.advertiser_dashboard'))
'''

mod_code_mc = '''
    from app.program_culturalfire.helpers import moderate_video_with_gemini
    if file.filename.lower().endswith(('.mp4', '.mov', '.avi', '.mkv')):
        if not moderate_video_with_gemini(file_path):
            import os
            if os.path.exists(file_path):
                os.remove(file_path)
            return jsonify({'success': False, 'message': 'Upload rejected: Inappropriate content detected by AI moderator.'})
'''

# 1. Segment Form
content = content.replace(
    'file.save(filepath)\n\n            if submission:',
    f'file.save(filepath)\n{mod_code}\n            if submission:'
)

# 2. Upload Ad
content = content.replace(
    'file.save(file_path)\n    \n    from app.models.culturalfire import CfiShowAd',
    f'file.save(file_path)\n{mod_code_ad}\n    from app.models.culturalfire import CfiShowAd'
)

# 3. MC Recording
content = content.replace(
    'file.save(file_path)\n\n    recording_type = request.form.get("recording_type", "act_intro")',
    f'file.save(file_path)\n{mod_code_mc}\n    recording_type = request.form.get("recording_type", "act_intro")'
)

# 4. Ramp Walk
content = content.replace(
    'file.save(filepath)\n            submission.video_url = filename',
    f'file.save(filepath)\n{mod_code}\n            submission.video_url = filename'
)
content = content.replace(
    'file.save(filepath)\n\n        submission = CfiTalentSubmission(',
    f'file.save(filepath)\n{mod_code}\n        submission = CfiTalentSubmission('
)


with open('app/program_culturalfire/routes.py', 'w') as f:
    f.write(content)
print("Done")
