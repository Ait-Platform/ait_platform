from flask import Blueprint

practice_crm_bp = Blueprint(
    "practice_crm_bp", 
    __name__, 
    template_folder="templates"
)

from . import routes
