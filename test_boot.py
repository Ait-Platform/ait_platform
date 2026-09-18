try:
    from app import create_app
    app = create_app()
    print("App created successfully")
except BaseException as e:
    import traceback
    traceback.print_exc()
