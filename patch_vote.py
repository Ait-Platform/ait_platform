with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

import re

# We want to replace the block starting at "# --- AUTO-CLOSE LOGIC ---" and ending right before "return redirect(url_for("uip_bp.view_resolution""
# inside vote_resolution.

pattern = r'# --- AUTO-CLOSE LOGIC ---.*?flash\(f"Voting concluded automatically! 100% participation reached\. Resolution REJECTED\.", "warning"\)'
replacement = """# --- AUTO-CLOSE LOGIC ---
    close_msg = _check_auto_close(org, res, db)
    if close_msg:
        flash(close_msg, "success")"""

new_text = re.sub(pattern, replacement, text, flags=re.DOTALL)
if new_text != text:
    print("Regex match successful, updated text.")
else:
    print("Regex match failed. Trying alternative...")
    # alternative: split and replace
    parts = text.split("# --- AUTO-CLOSE LOGIC ---")
    if len(parts) > 1:
        # parts[0] is everything before
        # parts[1] is the old block in vote_resolution
        # wait, chairman_vote_resolution and treasurer_vote_resolution now have # --- AUTO-CLOSE LOGIC ---? No, they don't, I added close_msg = _check_auto_close(org, res, db) without the comment.
        end_marker = 'return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=res.id))'
        block_parts = parts[1].split(end_marker, 1)
        
        new_block = """
    close_msg = _check_auto_close(org, res, db)
    if close_msg:
        flash(close_msg, "success")
        
    """
        new_text = parts[0] + "# --- AUTO-CLOSE LOGIC ---" + new_block + end_marker + block_parts[1]
        print("Manual split replace successful.")

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(new_text)
