from app.extensions import db
from datetime import datetime
import json

class ThuneeGame(db.Model):
    __tablename__ = 'thunee_games'
    id = db.Column(db.String(36), primary_key=True) # UUID for shareable link
    status = db.Column(db.String(20), default='lobby') # lobby, bidding, playing, finished
    trump_suit = db.Column(db.String(10), nullable=True)
    active_contract = db.Column(db.String(20), nullable=True) # Thunee, Blind, Double, Khanuck
    caller_seat = db.Column(db.Integer, nullable=True) # 0-3
    current_turn = db.Column(db.Integer, default=0) # 0-3
    dealer_seat = db.Column(db.Integer, default=0) # 0-3
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    # State tracking
    deck_state = db.Column(db.Text, nullable=True) # JSON serialized deck
    round_number = db.Column(db.Integer, default=1) # 1 (4 cards), 2 (2 cards remaining)

class ThuneePlayer(db.Model):
    __tablename__ = 'thunee_players'
    id = db.Column(db.Integer, primary_key=True)
    game_id = db.Column(db.String(36), db.ForeignKey('thunee_games.id'), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=True) # Null for bot
    is_bot = db.Column(db.Boolean, default=False)
    bot_name = db.Column(db.String(50), nullable=True)
    seat = db.Column(db.Integer) # 0, 1, 2, 3
    team = db.Column(db.String(1)) # A (0,2) or B (1,3)
    hand_state = db.Column(db.Text, nullable=True) # JSON serialized cards
    connected = db.Column(db.Boolean, default=True)

class ThuneeTrick(db.Model):
    __tablename__ = 'thunee_tricks'
    id = db.Column(db.Integer, primary_key=True)
    game_id = db.Column(db.String(36), db.ForeignKey('thunee_games.id'), nullable=False)
    trick_number = db.Column(db.Integer) # 1 to 6
    lead_suit = db.Column(db.String(10), nullable=True)
    winner_seat = db.Column(db.Integer, nullable=True)
    cards_played = db.Column(db.Text, default='[]') # JSON list of dicts: {seat: int, card: str, suit: str}

class ThuneeScore(db.Model):
    __tablename__ = 'thunee_scores'
    id = db.Column(db.Integer, primary_key=True)
    game_id = db.Column(db.String(36), db.ForeignKey('thunee_games.id'), nullable=False)
    team_a_balls = db.Column(db.Integer, default=0)
    team_b_balls = db.Column(db.Integer, default=0)
    team_a_points = db.Column(db.Integer, default=0) # Card points in hand
    team_b_points = db.Column(db.Integer, default=0)
