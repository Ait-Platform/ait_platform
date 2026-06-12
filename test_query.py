from app import create_app
from app.extensions import db
from app.utils.queries import BRIDGE_QUERY
from sqlalchemy import text

app = create_app()
app.app_context().push()
r = db.session.execute(text(BRIDGE_QUERY), {'email':'home2@gmail.com'}).fetchall()
print([(x.slug, x.access_level) for x in r])
