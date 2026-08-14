import random
import json

SUITS = ['Hearts', 'Diamonds', 'Clubs', 'Spades']
RANKS = ['J', '9', 'A', '10', 'K', 'Q']
RANK_VALUES = {'J': 30, '9': 20, 'A': 11, '10': 10, 'K': 3, 'Q': 2} # Some play A=1, we use 11
TRICK_HIERARCHY = {'J': 6, '9': 5, 'A': 4, '10': 3, 'K': 2, 'Q': 1}

def create_deck():
    return [{'suit': suit, 'rank': rank} for suit in SUITS for rank in RANKS]

def deal_initial(game, players):
    """Deals the first 4 cards to 4 players, updates deck_state and hand_state"""
    deck = create_deck()
    random.shuffle(deck)
    
    # Deal 4 cards to each player
    hands = {p.seat: [] for p in players}
    for _ in range(4):
        for p in players:
            hands[p.seat].append(deck.pop())
            
    # Save states
    game.deck_state = json.dumps(deck)
    game.round_number = 1
    for p in players:
        p.hand_state = json.dumps(hands[p.seat])

def deal_remaining(game, players):
    """Deals the remaining 2 cards to 4 players"""
    if not game.deck_state:
        return
    deck = json.loads(game.deck_state)
    
    for p in players:
        hand = json.loads(p.hand_state or '[]')
        hand.append(deck.pop())
        hand.append(deck.pop())
        p.hand_state = json.dumps(hand)
        
    game.deck_state = json.dumps(deck)
    game.round_number = 2

def determine_trick_winner(cards_played, lead_suit, trump_suit):
    """
    cards_played is a list of dicts: [{'seat': int, 'card': {'suit': str, 'rank': str}}, ...]
    Returns winning seat int.
    """
    if not cards_played:
        return None
        
    highest_seat = cards_played[0]['seat']
    highest_card = cards_played[0]['card']
    
    for play in cards_played[1:]:
        card = play['card']
        seat = play['seat']
        
        # If new card is trump and current highest is not trump, it wins
        if card['suit'] == trump_suit and highest_card['suit'] != trump_suit:
            highest_card = card
            highest_seat = seat
        # If both are trump, highest rank wins
        elif card['suit'] == trump_suit and highest_card['suit'] == trump_suit:
            if TRICK_HIERARCHY[card['rank']] > TRICK_HIERARCHY[highest_card['rank']]:
                highest_card = card
                highest_seat = seat
        # If neither is trump, and new card is lead suit, check rank
        elif card['suit'] == lead_suit and highest_card['suit'] == lead_suit:
            if TRICK_HIERARCHY[card['rank']] > TRICK_HIERARCHY[highest_card['rank']]:
                highest_card = card
                highest_seat = seat
                
    return highest_seat

def calculate_points(cards_played):
    points = 0
    for play in cards_played:
        points += RANK_VALUES[play['card']['rank']]
    return points

def is_valid_play(card, hand, lead_suit):
    """Checks if playing 'card' is valid given the 'hand' and 'lead_suit'"""
    if not lead_suit:
        return True # First to play can play anything
        
    # Must follow suit if possible
    has_lead_suit = any(c['suit'] == lead_suit for c in hand)
    if has_lead_suit and card['suit'] != lead_suit:
        return False
        
    return True

def process_bot_turn(game, players, current_trick):
    """
    If the current_turn belongs to a bot, execute its action.
    Returns True if a bot acted, False otherwise.
    """
    current_player = next((p for p in players if p.seat == game.current_turn), None)
    if not current_player or not current_player.is_bot:
        return False
        
    if game.status == 'bidding':
        # Bot bidding logic: Just call Trump randomly if they have to, or pass.
        # For simplicity, if it's the bot's turn to bid, they set trump to the suit they have most of.
        hand = json.loads(current_player.hand_state)
        suits_count = {}
        for c in hand:
            suits_count[c['suit']] = suits_count.get(c['suit'], 0) + 1
        best_suit = max(suits_count, key=suits_count.get) if suits_count else 'Hearts'
        
        game.trump_suit = best_suit
        game.caller_seat = current_player.seat
        game.active_contract = 'Trump'
        game.status = 'playing'
        
        # Deal remaining cards
        deal_remaining(game, players)
        # The player who called trump leads
        game.current_turn = game.caller_seat
        return True
        
    elif game.status == 'playing':
        # Bot playing logic
        hand = json.loads(current_player.hand_state)
        if not hand:
            return False
            
        lead_suit = current_trick.lead_suit if current_trick else None
        valid_cards = [c for c in hand if is_valid_play(c, hand, lead_suit)]
        if not valid_cards:
            valid_cards = hand # Fallback just in case
            
        # Basic Strategy: Play highest valid card if leading. Otherwise play lowest valid card.
        if not lead_suit:
            # Leading
            valid_cards.sort(key=lambda c: TRICK_HIERARCHY[c['rank']], reverse=True)
            chosen_card = valid_cards[0]
        else:
            # Following (simple bot: just play lowest valid card)
            valid_cards.sort(key=lambda c: TRICK_HIERARCHY[c['rank']])
            chosen_card = valid_cards[0]
            
        # Execute play
        hand.remove(chosen_card)
        current_player.hand_state = json.dumps(hand)
        
        played_list = json.loads(current_trick.cards_played) if current_trick else []
        played_list.append({'seat': current_player.seat, 'card': chosen_card})
        
        if not current_trick.lead_suit:
            current_trick.lead_suit = chosen_card['suit']
            
        current_trick.cards_played = json.dumps(played_list)
        
        if len(played_list) == 4:
            # Trick is over, handled in routes loop
            pass
        else:
            game.current_turn = (game.current_turn + 1) % 4
            
        return True
        
    return False
