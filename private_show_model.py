class CfiPrivateShowGroup(db.Model):
    __tablename__ = 'cfi_private_show_groups'
    id = db.Column(db.Integer, primary_key=True)
    show_id = db.Column(db.Integer, db.ForeignKey('cfi_shows.id'), nullable=False, unique=True)
    group_id = db.Column(db.Integer, db.ForeignKey('cfi_groups.id'), nullable=False)
    created_at = db.Column(db.DateTime, default=db.func.now())
