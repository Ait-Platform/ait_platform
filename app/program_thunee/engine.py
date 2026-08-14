import random

SUITS = ['Hearts', 'Diamonds', 'Clubs', 'Spades']
RANKS = ['J', '9', 'A', '10', 'K', 'Q']
RANK_VALUES = {'J': 30, '9': 20, 'A': 11, '10': 10, 'K': 3, 'Q': 2} # Some play A=1, we use 11
TRICK_HIERARCHY = {'J': 6, '9': 5, 'A': 4, '10': 3, 'K': 2, 'Q': 1}

def create_deck():
    return [{'suit': suit, 'rank': rank} for suit in SUITS for rank in RANKS]

def deal_initial(game, players):
    """Deals the first 4 cards to 4 players, updates deck_state and hand_state"""
    import json
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
    import json
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
