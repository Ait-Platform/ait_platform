import re

# 1. Update routes.py
with open(r'D:\Users\yeshk\Documents\ait_platform\app\program_culturalfire\routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_route = """def live_showcase_dashboard():
    return render_template("program_culturefire/live_hub.html")"""
new_route = """def live_showcase_dashboard():
    origin = request.args.get('origin')
    if origin:
        session['live_hub_origin'] = origin
    return render_template("program_culturefire/live_hub.html", origin=session.get('live_hub_origin', 'talent'))"""

if old_route in content:
    content = content.replace(old_route, new_route)
    with open(r'D:\Users\yeshk\Documents\ait_platform\app\program_culturalfire\routes.py', 'w', encoding='utf-8') as f:
        f.write(content)

# 2. Update live_hub.html
with open(r'D:\Users\yeshk\Documents\ait_platform\templates\program_culturefire\live_hub.html', 'r', encoding='utf-8') as f:
    html = f.read()

old_back = """<a href="javascript:history.back()" class="bg-blue-600 text-white px-3 py-1 rounded hover:bg-blue-700">"""
new_back = """{% set back_route = 'cultural_bp.' + origin + '_dashboard' if origin in ['talent', 'parent', 'sponsor', 'supporter'] else 'cultural_bp.cultural_fire_home' %}
    <a href="{{ url_for(back_route) }}" class="bg-blue-600 text-white px-3 py-1 rounded hover:bg-blue-700">"""

if old_back in html:
    html = html.replace(old_back, new_back)
    with open(r'D:\Users\yeshk\Documents\ait_platform\templates\program_culturefire\live_hub.html', 'w', encoding='utf-8') as f:
        f.write(html)

# 3. Update the 4 dashboards
dashboards = ['talent', 'parent', 'sponsor', 'supporter']
for d in dashboards:
    path = rf'D:\Users\yeshk\Documents\ait_platform\templates\program_culturefire\{d}_dashboard.html'
    try:
        with open(path, 'r', encoding='utf-8') as f:
            d_html = f.read()
        
        # We look for url_for('cultural_bp.live_showcase_dashboard') and replace with url_for(..., origin='talent') etc
        d_html = d_html.replace(
            "url_for('cultural_bp.live_showcase_dashboard')",
            f"url_for('cultural_bp.live_showcase_dashboard', origin='{d}')"
        )
        with open(path, 'w', encoding='utf-8') as f:
            f.write(d_html)
    except Exception as e:
        print(f"Failed on {d}: {e}")

print("Fixed back button looping via session origin.")
