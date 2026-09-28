import re
with open("app/program_uip/services/register.py", "r", encoding="utf-8") as f:
    content = f.read()

rel_search = """                already_active = False
                for active_rel in active_rels:
                    if active_rel.member_id == member.id and active_rel.relationship == rel_type:
                        already_active = True
                        continue
                    # Close the previous relationship as at the effective_date"""

rel_replace = """                already_active = False
                for active_rel in active_rels:
                    if active_rel.member_id == member.id and active_rel.relationship == rel_type:
                        already_active = True
                        continue
                    if active_rel.record_source == "MUNICIPAL" and not is_authoritative:
                        raise Exception("Cannot overwrite authoritative municipal relationships.")
                    # Close the previous relationship as at the effective_date"""

content = content.replace(rel_search, rel_replace)

with open("app/program_uip/services/register.py", "w", encoding="utf-8") as f:
    f.write(content)
print("Added relationship protection")
