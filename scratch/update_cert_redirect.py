import re

file_path = 'app/program_sace/routes.py'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# Replace both instances in email_certificate
# First instance: error handler for empty email
text = text.replace('flash("Email address is required.", "error")\n        return redirect(url_for("reading_bp.subject_home"))', 
                    'flash("Email address is required.", "error")\n        return redirect(url_for("sace_bp.reading_hub"))')

# Second instance: success redirect at the end
text = text.replace('flash("Failed to email certificate. Please try again.", "error")\n        return redirect(url_for("sace_bp.post_test_results"))\n        \n    return redirect(url_for("reading_bp.subject_home"))', 
                    'flash("Failed to email certificate. Please try again.", "error")\n        return redirect(url_for("sace_bp.post_test_results"))\n        \n    return redirect(url_for("sace_bp.reading_hub"))')

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
