import re

filepath = 'app/program_uip/routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

search_code = '''    # Attach photos manually
    for ref in escalations:
        ref.photos = UipDocument.query.filter_by(
            organization_id=org.id,
            interaction_id=ref.interaction_id,
            category="RP_QUERY_PHOTO"
        ).all()
    
    return render_template("program_uip/dashboards/municipal_officer.html", org=org, escalations=escalations)'''

replace_code = '''    # Attach photos manually
    for ref in escalations:
        ref.photos = UipDocument.query.filter_by(
            organization_id=org.id,
            interaction_id=ref.interaction_id,
            category="RP_QUERY_PHOTO"
        ).all()
        
    # Check if MO is also a ratepayer
    from app.models.uip import UipMemberProfile
    from sqlalchemy import func
    is_ratepayer = UipMemberProfile.query.filter(
        UipMemberProfile.organization_id == org.id,
        func.lower(UipMemberProfile.email) == current_user.email.lower()
    ).first() is not None
    
    return render_template("program_uip/dashboards/municipal_officer.html", org=org, escalations=escalations, is_ratepayer=is_ratepayer)'''

content = content.replace(search_code, replace_code)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)

filepath_html = 'templates/program_uip/dashboards/municipal_officer.html'
with open(filepath_html, 'r', encoding='utf-8') as f:
    content_html = f.read()

search_html = '''            <a href="{{ url_for('uip_bp.router_page', org_slug=org.slug) }}" class="inline-flex items-center text-sm font-bold text-slate-500 hover:text-indigo-600 transition" style="white-space: nowrap;">
                <i class="fas fa-hat-cowboy mr-2"></i> Switch Hat
            </a>'''

replace_html = '''            {% if is_ratepayer %}
            <a href="{{ url_for('uip_bp.verify_ratepayer', org_slug=org.slug) }}" class="inline-flex items-center text-sm font-bold text-emerald-600 hover:text-emerald-700 transition" style="white-space: nowrap;">
                <i class="fas fa-home mr-2"></i> View My Property
            </a>
            {% else %}
            <a href="{{ url_for('uip_bp.router_page', org_slug=org.slug) }}" class="inline-flex items-center text-sm font-bold text-slate-500 hover:text-indigo-600 transition" style="white-space: nowrap;">
                <i class="fas fa-arrow-left mr-2"></i> Main Menu
            </a>
            {% endif %}'''

content_html = content_html.replace(search_html, replace_html)

with open(filepath_html, 'w', encoding='utf-8') as f:
    f.write(content_html)
