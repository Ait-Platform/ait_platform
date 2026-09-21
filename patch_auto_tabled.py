with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

import re

# Update _check_auto_close to transition to TABLED
old_logic = """        if yea_count > nay_count:
            execute_resolution_adoption(org, res, db)
            return f"Voting concluded automatically! 100% participation reached. Resolution ADOPTED."
        else:
            res.status = "REJECTED"
            db.session.commit()
            return f"Voting concluded automatically! 100% participation reached. Resolution REJECTED."
"""

new_logic = """        # Digital voting is purely a temperature check.
        # Regardless of the outcome, the resolution MUST transition to a live meeting (TABLED)
        # for formal political debate and ratification.
        res.status = "TABLED"
        db.session.commit()
        return f"100% participation reached! Digital voting concluded. Resolution is now TABLED for the live meeting."
"""

text = text.replace(old_logic, new_logic)

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)

print("Updated auto-close logic to transition to TABLED instead of ADOPTED/REJECTED")
