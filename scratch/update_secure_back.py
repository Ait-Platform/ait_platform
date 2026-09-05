import re

# Update routes.py to pass return_to
routes_path = 'app/program_sace/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    routes = f.read()

old_secure = '''    # Log that the user viewed this document
    interaction = SaceWorkshopInteraction(
        user_id=current_user.id if current_user.is_authenticated else 1,
        activity_slug="viewed_document",
        response_data="Document opened in secure viewer"
    )
    db.session.add(interaction)
    db.session.commit()

    return render_template("program_sace/secure_viewer.html", doc_title=doc_title, doc_url=doc_url)'''

new_secure = '''    # Log that the user viewed this document
    interaction = SaceWorkshopInteraction(
        user_id=current_user.id if current_user.is_authenticated else 1,
        activity_slug="viewed_document",
        response_data="Document opened in secure viewer"
    )
    db.session.add(interaction)
    db.session.commit()

    return_to = request.args.get('return_to', 'hub')
    return render_template("program_sace/secure_viewer.html", doc_title=doc_title, doc_url=doc_url, return_to=return_to)'''

routes = routes.replace(old_secure, new_secure)

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(routes)


# Update secure_viewer.html to use return_to
viewer_path = 'templates/program_sace/secure_viewer.html'
with open(viewer_path, 'r', encoding='utf-8') as f:
    viewer = f.read()

old_link = '''<a href="{{ url_for('sace_bp.reading_hub') }}" class="px-4 py-2 bg-indigo-600 text-white text-sm font-bold rounded hover:bg-indigo-500 transition">
                Return to Hub
            </a>'''
            
new_link = '''
            {% if return_to == 'control_centre' %}
            <a href="{{ url_for('sace_bp.provisioning_map') }}" class="px-4 py-2 bg-indigo-600 text-white text-sm font-bold rounded hover:bg-indigo-500 transition">
                Return to Control Centre
            </a>
            {% else %}
            <a href="{{ url_for('sace_bp.reading_hub') }}" class="px-4 py-2 bg-indigo-600 text-white text-sm font-bold rounded hover:bg-indigo-500 transition">
                Return to Hub
            </a>
            {% endif %}
'''
viewer = viewer.replace(old_link, new_link)

with open(viewer_path, 'w', encoding='utf-8') as f:
    f.write(viewer)
