with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

# Delete the old block in vote_resolution
split_point_1 = "# --- AUTO-CLOSE LOGIC ---"
split_point_2 = "return redirect(url_for(\"uip_bp.view_resolution\""

if split_point_1 in text and split_point_2 in text:
    before = text.split(split_point_1)[0]
    after = split_point_2 + text.split(split_point_2, 1)[1]
    
    new_block = """# --- AUTO-CLOSE LOGIC ---
    close_msg = _check_auto_close(org, res, db)
    if close_msg:
        flash(close_msg, "success")
        
    """
    
    text = before + new_block + after

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Removed old dead auto-close logic")
