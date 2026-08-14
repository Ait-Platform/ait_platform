import uuid
import json
from flask import Blueprint, render_template, request, jsonify, redirect, url_for, flash
from flask_login import login_required, current_user
from app.extensions import db
from app.models.thunee import ThuneeGame, ThuneePlayer, ThuneeTrick, ThuneeScore
from .engine import deal_initial, deal_remaining, determine_trick_winner, calculate_points
from . import thunee_bp

@thunee_bp.route('/thunee')
@login_required
def index():
    return render_template('program_thunee/lobby.html')

@thunee_bp.route('/thunee/create', methods=['POST'])
@login_required
def create_game():
    game_id = str(uuid.uuid4())
    game = ThuneeGame(id=game_id, status='lobby')
    db.session.add(game)
    
    # Add creator as seat 0 (Team A)
    player = ThuneePlayer(game_id=game_id, user_id=current_user.id, seat=0, team='A')
    db.session.add(player)
    
    # Create empty score
    score = ThuneeScore(game_id=game_id)
    db.session.add(score)
    
    db.session.commit()
    return redirect(url_for('thunee_bp.table', game_id=game_id))

@thunee_bp.route('/thunee/join/<game_id>', methods=['POST'])
@login_required
def join_game(game_id):
    game = ThuneeGame.query.get_or_404(game_id)
    if game.status != 'lobby':
        flash('Game already in progress.', 'error')
        return redirect(url_for('thunee_bp.index'))
        
    players = ThuneePlayer.query.filter_by(game_id=game_id).all()
    if len(players) >= 4:
        flash('Game is full.', 'error')
        return redirect(url_for('thunee_bp.index'))
        
    # Check if user already in game
    for p in players:
        if p.user_id == current_user.id:
            return redirect(url_for('thunee_bp.table', game_id=game_id))
            
    # Assign seat
    taken_seats = [p.seat for p in players]
    available_seats = [s for s in range(4) if s not in taken_seats]
    seat = available_seats[0]
    team = 'A' if seat in (0, 2) else 'B'
    
    player = ThuneePlayer(game_id=game_id, user_id=current_user.id, seat=seat, team=team)
    db.session.add(player)
    db.session.commit()
    
    return redirect(url_for('thunee_bp.table', game_id=game_id))

@thunee_bp.route('/thunee/add_bot/<game_id>', methods=['POST'])
@login_required
def add_bot(game_id):
    game = ThuneeGame.query.get_or_404(game_id)
    if game.status != 'lobby':
        return jsonify({'error': 'Game not in lobby'}), 400
        
    players = ThuneePlayer.query.filter_by(game_id=game_id).all()
    if len(players) >= 4:
        return jsonify({'error': 'Game is full'}), 400
        
    taken_seats = [p.seat for p in players]
    available_seats = [s for s in range(4) if s not in taken_seats]
    seat = available_seats[0]
    team = 'A' if seat in (0, 2) else 'B'
    
    bot_names = ['Bot alpha', 'Bot bravo', 'Bot charlie', 'Bot delta']
    
    player = ThuneePlayer(game_id=game_id, is_bot=True, bot_name=bot_names[seat], seat=seat, team=team)
    db.session.add(player)
    db.session.commit()
    
    return jsonify({'success': True})

@thunee_bp.route('/thunee/start/<game_id>', methods=['POST'])
@login_required
def start_game(game_id):
    game = ThuneeGame.query.get_or_404(game_id)
    players = ThuneePlayer.query.filter_by(game_id=game_id).order_by(ThuneePlayer.seat).all()
    
    if len(players) != 4:
        return jsonify({'error': 'Need 4 players'}), 400
        
    game.status = 'bidding'
    game.dealer_seat = 0
    game.current_turn = 1 # Player left of dealer starts bidding
    deal_initial(game, players)
    db.session.commit()
    
    return jsonify({'success': True})

@thunee_bp.route('/thunee/table/<game_id>')
@login_required
def table(game_id):
    game = ThuneeGame.query.get_or_404(game_id)
    return render_template('program_thunee/table.html', game=game)

@thunee_bp.route('/thunee/api/state/<game_id>')
@login_required
def game_state(game_id):
    game = ThuneeGame.query.get_or_404(game_id)
    players = ThuneePlayer.query.filter_by(game_id=game_id).order_by(ThuneePlayer.seat).all()
    score = ThuneeScore.query.filter_by(game_id=game_id).first()
    
    # Hide other players' cards
    player_states = []
    for p in players:
        is_me = (p.user_id == current_user.id)
        state = {
            'seat': p.seat,
            'team': p.team,
            'name': p.bot_name if p.is_bot else (f"Player {p.user_id}" if p.user_id else "Unknown"),
            'is_bot': p.is_bot,
            'hand': json.loads(p.hand_state) if is_me and p.hand_state else len(json.loads(p.hand_state or '[]')),
            'is_me': is_me
        }
        player_states.append(state)
        
    trick = ThuneeTrick.query.filter_by(game_id=game_id).order_by(ThuneeTrick.trick_number.desc()).first()
    
    response = {
        'game_id': game.id,
        'status': game.status,
        'trump_suit': game.trump_suit,
        'active_contract': game.active_contract,
        'current_turn': game.current_turn,
        'players': player_states,
        'score': {
            'team_a_balls': score.team_a_balls,
            'team_b_balls': score.team_b_balls,
            'team_a_points': score.team_a_points,
            'team_b_points': score.team_b_points
        },
        'trick': {
            'trick_number': trick.trick_number if trick else 0,
            'lead_suit': trick.lead_suit if trick else None,
            'cards_played': json.loads(trick.cards_played) if trick else []
        }
    }
    
    return jsonify(response)
