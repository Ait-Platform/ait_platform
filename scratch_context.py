from app import create_app, db
from app.admin.loss.routes import _build_context
app = create_app()
app.app_context().push()

ctx, scores, row = _build_context(32, 415)
print('ctx:', ctx)
print('scores:', scores)
print('row:', row)
