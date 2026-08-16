with open('app/models/mechanic.py', 'r', encoding='utf-8') as f:
    content = f.read()

model_original = '''class MechVehicle(db.Model):
    __tablename__ = 'mech_vehicles'
    id = db.Column(db.Integer, primary_key=True)
    client_id = db.Column(db.Integer, db.ForeignKey('mech_clients.id'), nullable=False)
    make = db.Column(db.String(50))
    model = db.Column(db.String(50))
    year = db.Column(db.Integer)
    vin = db.Column(db.String(50), unique=True)
    license_plate = db.Column(db.String(20))
    mileage = db.Column(db.Integer)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)'''

model_new = '''class MechVehicle(db.Model):
    __tablename__ = 'mech_vehicles'
    id = db.Column(db.Integer, primary_key=True)
    client_id = db.Column(db.Integer, db.ForeignKey('mech_clients.id'), nullable=False)
    make = db.Column(db.String(50))
    model = db.Column(db.String(50))
    year = db.Column(db.Integer)
    vin = db.Column(db.String(50), unique=True)
    license_plate = db.Column(db.String(20))
    engine_no = db.Column(db.String(50))
    gvm = db.Column(db.String(20))
    tare = db.Column(db.String(20))
    disk_license_no = db.Column(db.String(50))
    mileage = db.Column(db.Integer)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)'''

content = content.replace(model_original, model_new)

with open('app/models/mechanic.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated MechVehicle model")
