from flask import Blueprint

# 1) Define the ONE blueprint object
checkout_bp = Blueprint("checkout_bp", __name__, url_prefix="/checkout")

# 2) Now import routes so decorators attach to THIS checkout_bp
from . import routes  # noqa: E402

