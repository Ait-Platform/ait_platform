with open('app/program_culturalfire/routes.py', 'r') as f:
    content = f.read()

filter_logic = '''
    # Filter out flagged videos
    from app.models.culturalfire import CfiVideoFlag
    flagged_counts = db.session.query(CfiVideoFlag.video_id, db.func.count(CfiVideoFlag.id)).group_by(CfiVideoFlag.video_id).all()
    banned_video_ids = {vid for vid, count in flagged_counts if count >= 3}
    
    filtered_playlist = []
    for item in unified_playlist:
        item_id_str = str(item.get("id"))
        if item_id_str not in banned_video_ids:
            filtered_playlist.append(item)
    
    unified_playlist = filtered_playlist
'''

content = content.replace(
    'return render_template(\n        "program_culturefire/watch_show.html"',
    f'{filter_logic}\n    return render_template(\n        "program_culturefire/watch_show.html"'
)

with open('app/program_culturalfire/routes.py', 'w') as f:
    f.write(content)
print("Done")
