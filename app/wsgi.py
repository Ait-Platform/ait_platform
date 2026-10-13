from app import create_app
# Web workers must not perform migrations or boot-time database repairs.
app = create_app(operator_mode=True)
