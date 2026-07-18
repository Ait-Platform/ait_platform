with open('templates/program_billing/manual_capture.html', 'r', encoding='utf-8') as f:
    html = f.read()

# Find where {% block content %} starts
block_idx = html.find('{% block content %}')

if block_idx != -1:
    header = '{% extends "layout.html" %}\n{% block title %}Account Setup Wizard{% endblock %}\n{% block flashes %}{% endblock %}\n\n'
    body = html[block_idx:]
    
    # We want the flash to be INSIDE the tile, but NOT duplicated.
    # Currently it's at line 59: {% include "partials/flash_messages.html" ignore missing %}
    # We leave it there. By having block flashes empty, it won't render at the top.
    
    # Also, we should remove the trailing duplicate {% endblock %} at the bottom if there is one.
    # Let's count {% endblock %}. There should be only ONE for the content block, and ONE for the flashes we just defined in the header.
    # Wait, the header has: {% block flashes %}{% endblock %}.
    # The body has: {% block content %} ... {% endblock %}.
    
    new_html = header + body
    
    # ensure no duplicate block content endblock
    blocks = new_html.split('{% endblock %}')
    # The last block should just be what follows the last endblock
    # But wait, there was a duplicate help-modal at the bottom of the file which might have an extra endblock?
    # Let's clean the very bottom if it has extra junk.
    
    with open('templates/program_billing/manual_capture.html', 'w', encoding='utf-8') as f:
        f.write(new_html)
    print("Cleaned up manual_capture.html")
