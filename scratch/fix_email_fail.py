import re

file_path = 'app/program_sace/routes.py'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

old_fail = '''    except Exception as e:
        current_app.logger.error(f"Failed to email SACE workshop certificate: {e}")
        flash("Failed to email certificate. Please try again later.", "error")
        
    return redirect(url_for("reading_bp.subject_home"))'''

new_fail = '''    except Exception as e:
        current_app.logger.error(f"Failed to email SACE workshop certificate: {e}")
        flash("Failed to email certificate. Please try again.", "error")
        return redirect(url_for("sace_bp.post_test_results"))
        
    return redirect(url_for("reading_bp.subject_home"))'''

text = text.replace(old_fail, new_fail)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
