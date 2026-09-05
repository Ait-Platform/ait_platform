from flask import redirect, url_for
from app.uip import uip_bp

@uip_bp.route("/")
def uip_start():
    # A simple landing/entry route that redirects to the demo tenant for now.
    # We will assume 'manor-gardens' since we seeded it earlier.
    return redirect(url_for('uip_bp.dashboard', org_slug='manor-gardens'))
