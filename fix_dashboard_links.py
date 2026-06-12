import os

# 1. Update dashboards to pass enrollment_id
dashboards = ['talent', 'parent', 'supporter']
for d in dashboards:
    path = rf'D:\Users\yeshk\Documents\ait_platform\templates\program_culturefire\{d}_dashboard.html'
    try:
        with open(path, 'r', encoding='utf-8') as f:
            d_html = f.read()
        
        # We look for url_for('cultural_bp.live_showcase_dashboard', origin='...') 
        # and append enrollment_id=enrollment.id
        old_str = f"url_for('cultural_bp.live_showcase_dashboard', origin='{d}')"
        new_str = f"url_for('cultural_bp.live_showcase_dashboard', origin='{d}', enrollment_id=enrollment.id)"
        
        if old_str in d_html:
            d_html = d_html.replace(old_str, new_str)
            with open(path, 'w', encoding='utf-8') as f:
                f.write(d_html)
            print(f"Updated {d}_dashboard.html")
    except Exception as e:
        print(f"Failed on {d}: {e}")

# 2. Update routes.py
with open(r'D:\Users\yeshk\Documents\ait_platform\app\program_culturalfire\routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_route = """def live_showcase_dashboard():
    origin = request.args.get('origin')
    if origin:
        session['live_hub_origin'] = origin
    return render_template("program_culturefire/live_hub.html", origin=session.get('live_hub_origin', 'talent'))"""

new_route = """def live_showcase_dashboard():
    origin = request.args.get('origin')
    enrollment_id = request.args.get('enrollment_id')
    if origin:
        session['live_hub_origin'] = origin
        if enrollment_id:
            session['live_hub_enrollment_id'] = enrollment_id

    origin = session.get('live_hub_origin', 'talent')
    enrollment_id = session.get('live_hub_enrollment_id')
    return render_template("program_culturefire/live_hub.html", origin=origin, enrollment_id=enrollment_id)"""

if old_route in content:
    content = content.replace(old_route, new_route)
    with open(r'D:\Users\yeshk\Documents\ait_platform\app\program_culturalfire\routes.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Updated routes.py")

# 3. Update live_hub.html
with open(r'D:\Users\yeshk\Documents\ait_platform\templates\program_culturefire\live_hub.html', 'r', encoding='utf-8') as f:
    html = f.read()

old_back = """{% set back_route = 'cultural_bp.' + origin + '_dashboard' if origin in ['talent', 'parent', 'sponsor', 'supporter'] else 'cultural_bp.cultural_fire_home' %}
    <a href="{{ url_for(back_route) }}" class="bg-blue-600 text-white px-3 py-1 rounded hover:bg-blue-700">"""

new_back = """{% if origin == 'sponsor' %}
        {% set back_route = url_for('cultural_bp.sponsor_dashboard') %}
    {% elif origin in ['talent', 'parent', 'supporter'] %}
        {% set back_route = url_for('cultural_bp.' + origin + '_dashboard', enrollment_id=enrollment_id) %}
    {% else %}
        {% set back_route = url_for('cultural_bp.cultural_fire_home') %}
    {% endif %}
    <a href="{{ back_route }}" class="bg-blue-600 text-white px-3 py-1 rounded hover:bg-blue-700">"""

if old_back in html:
    html = html.replace(old_back, new_back)
    with open(r'D:\Users\yeshk\Documents\ait_platform\templates\program_culturefire\live_hub.html', 'w', encoding='utf-8') as f:
        f.write(html)
    print("Updated live_hub.html")
