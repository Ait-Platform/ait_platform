with open('app/models/culturalfire.py', 'r') as f:
    content = f.read()

flag_model = '''
class CfiVideoFlag(db.Model):
    __tablename__ = 'cfi_video_flags'
    id = db.Column(db.Integer, primary_key=True)
    video_id = db.Column(db.String(100), nullable=False)
    reporter_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    created_at = db.Column(db.DateTime, default=db.func.now())
'''

content = content + "\n" + flag_model + "\n"

with open('app/models/culturalfire.py', 'w') as f:
    f.write(content)
print("Done")
