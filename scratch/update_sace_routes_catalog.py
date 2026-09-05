import re

with open('app/program_sace/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_hub = """@sace_bp.route('/sace/hub')
@login_required
def selection_hub():
    return render_template('program_sace/sace_selection_hub.html')"""

new_hub = """@sace_bp.route('/sace/catalog')
@login_required
def catalog():
    activities = [
        {
            "slug": "reading",
            "name": "Litre Reading Workshop",
            "desc": "Interactive reading methodology for early childhood development.",
            "icon": "fa-book-open"
        }
    ]
    return render_template('program_sace/sace_catalog.html', activities=activities)

@sace_bp.route('/sace/hub/<activity_slug>')
@login_required
def selection_hub(activity_slug):
    return render_template('program_sace/sace_selection_hub.html', activity_slug=activity_slug)"""

content = content.replace(old_hub, new_hub)

with open('app/program_sace/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Added catalog route and updated selection hub route")
