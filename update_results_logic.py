with open(r'D:\Users\yeshk\Documents\ait_platform\app\program_culturalfire\routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Update voting sum logic
old_vote_count = """        # Count votes for this segment item
        votes = CfiShowcaseVote.query.filter_by(segment_item_id=item.id).count()
        contestant_votes[eid]['total_votes'] += votes
        contestant_votes[eid]['segments'][segment_type_formatted] = votes"""

new_vote_sum = """        # Sum scores for this segment item
        votes_records = CfiShowcaseVote.query.filter_by(segment_item_id=item.id).all()
        score_sum = sum(v.score for v in votes_records)
        contestant_votes[eid]['total_votes'] += score_sum
        contestant_votes[eid]['segments'][segment_type_formatted] = score_sum"""

if old_vote_count in content:
    content = content.replace(old_vote_count, new_vote_sum)

# Update has_ended logic
old_has_ended = """    # Calculate if the show has ended
    from datetime import datetime, timedelta
    
    if show.end_date:
        end_date = show.end_date
    elif show.start_date:
        end_date = show.start_date + timedelta(days=30)
    else:
        end_date = datetime.utcnow().date() + timedelta(days=30)
        
    has_ended = datetime.utcnow().date() > end_date"""

new_has_ended = """    # Calculate if the show has ended dynamically based on judging completion
    judge_count = CfiJudgeAssignment.query.filter_by(show_id=show.id).count()
    segment_count = len(segment_items)
    expected_scores = judge_count * segment_count
    
    actual_scores = 0
    for item in segment_items:
        actual_scores += CfiShowcaseVote.query.filter_by(segment_item_id=item.id).count()

    has_ended = (actual_scores >= expected_scores) and (expected_scores > 0)"""

if old_has_ended in content:
    content = content.replace(old_has_ended, new_has_ended)

# Don't forget pageant_winners has the SAME logic for has_ended!
old_winners_vote_count = """        votes = CfiShowcaseVote.query.filter_by(segment_item_id=item.id).count()
        contestant_votes[eid]['total_votes'] += votes"""

new_winners_vote_sum = """        votes_records = CfiShowcaseVote.query.filter_by(segment_item_id=item.id).all()
        score_sum = sum(v.score for v in votes_records)
        contestant_votes[eid]['total_votes'] += score_sum"""

if old_winners_vote_count in content:
    content = content.replace(old_winners_vote_count, new_winners_vote_sum)

old_winners_has_ended = """    # Optional logic: only allow viewing if ended
    from datetime import datetime, timedelta
    if show.end_date:
        end_date = show.end_date
    elif show.start_date:
        end_date = show.start_date + timedelta(days=30)
    else:
        end_date = datetime.utcnow().date() + timedelta(days=30)
        
    has_ended = datetime.utcnow().date() > end_date"""

new_winners_has_ended = """    # Optional logic: only allow viewing if ended
    judge_count = CfiJudgeAssignment.query.filter_by(show_id=show.id).count()
    segment_count = len(segment_items)
    expected_scores = judge_count * segment_count
    
    actual_scores = 0
    for item in segment_items:
        actual_scores += CfiShowcaseVote.query.filter_by(segment_item_id=item.id).count()

    has_ended = (actual_scores >= expected_scores) and (expected_scores > 0)"""

if old_winners_has_ended in content:
    content = content.replace(old_winners_has_ended, new_winners_has_ended)

with open(r'D:\Users\yeshk\Documents\ait_platform\app\program_culturalfire\routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated results and winners logic successfully.")
