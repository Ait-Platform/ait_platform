try:
    from app.models.uip import *
    print("Models imported successfully")
except BaseException as e:
    import traceback
    traceback.print_exc()
