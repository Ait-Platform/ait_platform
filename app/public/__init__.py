from flask import Blueprint

public_bp = Blueprint('public_bp', __name__)

from . import routes
