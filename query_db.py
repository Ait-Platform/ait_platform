try:
    from app import create_app
    from app.extensions import db
    from app.models.uip_governance import UipOrganogramSeat
    
    app = create_app()
    with app.app_context():
        groups = db.session.query(UipOrganogramSeat.group_level).distinct().all()
        print('Groups:', groups)
        seats = UipOrganogramSeat.query.all()
        print('Total seats:', len(seats))
        for s in seats:
            print(s.title, s.group_level)
except Exception as e:
    print("ERROR:", e)
