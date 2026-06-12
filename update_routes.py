with open(r'D:\Users\yeshk\Documents\ait_platform\app\program_culturalfire\routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_render = """    return render_template(
        "program_culturefire/pageant_results.html",
        show=show,
        ranked_contestants=ranked_contestants,
        ordered_segments=ordered_segments
    )"""

new_render = """    # Calculate if the show has ended
    from datetime import datetime, timedelta
    
    if show.end_date:
        end_date = show.end_date
    elif show.start_date:
        end_date = show.start_date + timedelta(days=30)
    else:
        end_date = datetime.utcnow().date() + timedelta(days=30)
        
    has_ended = datetime.utcnow().date() > end_date

    return render_template(
        "program_culturefire/pageant_results.html",
        show=show,
        ranked_contestants=ranked_contestants,
        ordered_segments=ordered_segments,
        has_ended=has_ended
    )"""

if old_render in content:
    content = content.replace(old_render, new_render)
else:
    print("WARNING: Could not find old_render block in routes.py")

winners_route = """

@cultural_bp.route("/show/<int:show_id>/winners")
@login_required
def pageant_winners(show_id):
    show = CfiShow.query.get_or_404(show_id)
    if not show.category_item or show.category_item.name != "Pageant":
        flash("Winners are only available for Pageants.", "error")
        return redirect(url_for("cultural_bp.showcase_dashboard"))

    segment_items = CfiSegmentItem.query.filter_by(show_id=show_id).all()
    contestant_votes = {}
    
    for item in segment_items:
        enrollment = item.enrollment
        if not enrollment:
            continue
            
        eid = enrollment.id
        if eid not in contestant_votes:
            contestant_votes[eid] = {
                'enrollment': enrollment,
                'name': enrollment.biodata.full_name if enrollment.biodata else "Unknown",
                'total_votes': 0
            }
            
        votes = CfiShowcaseVote.query.filter_by(segment_item_id=item.id).count()
        contestant_votes[eid]['total_votes'] += votes

    ranked_contestants = sorted(contestant_votes.values(), key=lambda x: x['total_votes'], reverse=True)
    top_3 = ranked_contestants[:3]
    
    # Optional logic: only allow viewing if ended
    from datetime import datetime, timedelta
    if show.end_date:
        end_date = show.end_date
    elif show.start_date:
        end_date = show.start_date + timedelta(days=30)
    else:
        end_date = datetime.utcnow().date() + timedelta(days=30)
        
    has_ended = datetime.utcnow().date() > end_date

    return render_template(
        "program_culturefire/pageant_winners.html",
        show=show,
        top_3=top_3,
        has_ended=has_ended
    )
"""

content += winners_route

with open(r'D:\Users\yeshk\Documents\ait_platform\app\program_culturalfire\routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated routes.py")
