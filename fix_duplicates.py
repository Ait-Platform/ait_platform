import re

content = open('app/program_healthcore/routes.py', 'r').read()

# Remove duplicate add_nutrition
old_nutrition_pattern = r'@healthcore_bp\.route\("/program/healthcore/engine/nutrition/add", methods=\["POST"\]\)\n@login_required\ndef add_nutrition\(\):\n    from app\.models\.healthcore import HcNutrition\n    from datetime import datetime\n\n    log_date_str = request\.form\.get\("log_date"\)\n.*?return redirect\(url_for\("healthcore_bp\.nutrition_dashboard"\)\)'

# Remove duplicate add_lifestyle
old_lifestyle_pattern = r'@healthcore_bp\.route\("/program/healthcore/engine/lifestyle/add", methods=\["POST"\]\)\n@login_required\ndef add_lifestyle\(\):\n    from app\.models\.healthcore import HcLifestyle\n    from datetime import datetime\n\n    log_date_str = request\.form\.get\("log_date"\)\n.*?return redirect\(url_for\("healthcore_bp\.lifestyle_dashboard"\)\)'

# Remove duplicate add_timeline
old_timeline_pattern = r'@healthcore_bp\.route\("/program/healthcore/engine/timeline/add", methods=\["POST"\]\)\n@login_required\ndef add_timeline\(\):\n    from app\.models\.healthcore import HcTimelineEvent\n    from datetime import datetime\n\n    start_date_str = request\.form\.get\("start_date"\)\n.*?return redirect\(url_for\("healthcore_bp\.timeline_dashboard"\)\)'

# We need to only remove the ones without @healthcore_onboarded_required
content = re.sub(r'@healthcore_bp\.route\("/program/healthcore/engine/nutrition/add", methods=\["POST"\]\)\n@login_required\ndef add_nutrition\(\):.*?return redirect\(url_for\("healthcore_bp\.nutrition_dashboard"\)\)', '', content, flags=re.DOTALL)
content = re.sub(r'@healthcore_bp\.route\("/program/healthcore/engine/lifestyle/add", methods=\["POST"\]\)\n@login_required\ndef add_lifestyle\(\):.*?return redirect\(url_for\("healthcore_bp\.lifestyle_dashboard"\)\)', '', content, flags=re.DOTALL)
content = re.sub(r'@healthcore_bp\.route\("/program/healthcore/engine/timeline/add", methods=\["POST"\]\)\n@login_required\ndef add_timeline\(\):.*?return redirect\(url_for\("healthcore_bp\.timeline_dashboard"\)\)', '', content, flags=re.DOTALL)

open('app/program_healthcore/routes.py', 'w').write(content)
