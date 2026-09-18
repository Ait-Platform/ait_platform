import os
os.environ["DATABASE_URL"] = "postgresql://dummy:dummy@localhost/dummy"
try:
    from app import create_app
    app = create_app()
    for rule in app.url_map.iter_rules():
        if "patch" in str(rule) or "dev" in str(rule):
            print(rule)
except BaseException as e:
    import traceback
    traceback.print_exc()
