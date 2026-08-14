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
    
    run_bots(game)
    return jsonify({'success': True})

from .engine import process_bot_turn, is_valid_play, determine_trick_winner, calculate_points

def run_bots(game):
    players = ThuneePlayer.query.filter_by(game_id=game.id).order_by(ThuneePlayer.seat).all()
    changed = False
    
    while True:
        trick = ThuneeTrick.query.filter_by(game_id=game.id).order_by(ThuneeTrick.trick_number.desc()).first()
        acted = process_bot_turn(game, players, trick)
        if not acted:
            break
        changed = True
        
        # Check if trick is full after bot action
        if trick and trick.cards_played:
            played = json.loads(trick.cards_played)
            if len(played) == 4:
                # Resolve trick
                winner_seat = determine_trick_winner(played, trick.lead_suit, game.trump_suit)
                trick.winner_seat = winner_seat
                
                points = calculate_points(played)
                score = ThuneeScore.query.filter_by(game_id=game.id).first()
                if winner_seat in (0, 2):
                    score.team_a_points += points
                else:
                    score.team_b_points += points
                    
                game.current_turn = winner_seat
                
                if game.round_number == 6:
                    game.status = 'finished'
                else:
                    new_trick = ThuneeTrick(game_id=game.id, trick_number=trick.trick_number + 1)
                    db.session.add(new_trick)
                    
        db.session.commit()
    return changed

@thunee_bp.route('/thunee/call_trump/<game_id>', methods=['POST'])
@login_required
def call_trump(game_id):
    game = ThuneeGame.query.get_or_404(game_id)
    if game.status != 'bidding':
        return jsonify({'error': 'Not bidding phase'}), 400
        
    me = ThuneePlayer.query.filter_by(game_id=game_id, user_id=current_user.id).first()
    if not me or game.current_turn != me.seat:
        return jsonify({'error': 'Not your turn'}), 400
        
    suit = request.json.get('suit')
    if suit not in ['Hearts', 'Diamonds', 'Clubs', 'Spades']:
        return jsonify({'error': 'Invalid suit'}), 400
        
    game.trump_suit = suit
    game.caller_seat = me.seat
    game.active_contract = 'Trump'
    game.status = 'playing'
    
    players = ThuneePlayer.query.filter_by(game_id=game_id).all()
    deal_remaining(game, players)
    
    # First trick
    game.current_turn = game.caller_seat
    trick = ThuneeTrick(game_id=game.id, trick_number=1)
    db.session.add(trick)
    
    db.session.commit()
    
    run_bots(game)
    return jsonify({'success': True})

@thunee_bp.route('/thunee/play_card/<game_id>', methods=['POST'])
@login_required
def play_card(game_id):
    game = ThuneeGame.query.get_or_404(game_id)
    if game.status != 'playing':
        return jsonify({'error': 'Not playing phase'}), 400
        
    me = ThuneePlayer.query.filter_by(game_id=game_id, user_id=current_user.id).first()
    if not me or game.current_turn != me.seat:
        return jsonify({'error': 'Not your turn'}), 400
        
    card = request.json.get('card') # {'suit': 'Hearts', 'rank': 'J'}
    if not card:
        return jsonify({'error': 'No card provided'}), 400
        
    hand = json.loads(me.hand_state)
    # Check if card is in hand
    if not any(c['suit'] == card['suit'] and c['rank'] == card['rank'] for c in hand):
        return jsonify({'error': 'Card not in hand'}), 400
        
    trick = ThuneeTrick.query.filter_by(game_id=game_id).order_by(ThuneeTrick.trick_number.desc()).first()
    lead_suit = trick.lead_suit if trick else None
    
    if not is_valid_play(card, hand, lead_suit):
        return jsonify({'error': 'Invalid play (must follow suit)'}), 400
        
    # Play the card
    hand = [c for c in hand if not (c['suit'] == card['suit'] and c['rank'] == card['rank'])]
    me.hand_state = json.dumps(hand)
    
    played_list = json.loads(trick.cards_played)
    played_list.append({'seat': me.seat, 'card': card})
    
    if not trick.lead_suit:
        trick.lead_suit = card['suit']
        
    trick.cards_played = json.dumps(played_list)
    
    if len(played_list) == 4:
        # Trick over
        winner_seat = determine_trick_winner(played_list, trick.lead_suit, game.trump_suit)
        trick.winner_seat = winner_seat
        
        points = calculate_points(played_list)
        score = ThuneeScore.query.filter_by(game_id=game.id).first()
        if winner_seat in (0, 2):
            score.team_a_points += points
        else:
            score.team_b_points += points
            
        game.current_turn = winner_seat
        
        if trick.trick_number == 6:
            game.status = 'finished'
        else:
            new_trick = ThuneeTrick(game_id=game.id, trick_number=trick.trick_number + 1)
            db.session.add(new_trick)
    else:
        game.current_turn = (game.current_turn + 1) % 4
        
    db.session.commit()
    
    run_bots(game)
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
