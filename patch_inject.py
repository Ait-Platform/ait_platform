import re
with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_sql = """        db.session.commit()
        return "Success: DB Upgraded for Digital Committee Room"
    except Exception as e:"""

new_sql = """        db.session.commit()
        
        # ALSO INJECT THE 4 RESOLUTIONS FOR THIS ORG
        from app.models.uip import UipResolution
        from app.models.core import CoreOrganization
        org = CoreOrganization.query.filter_by(slug=org_slug).first()
        if org:
            if UipResolution.query.filter_by(organization_id=org.id).count() == 0:
                foundational_resolutions = [
                    {"title": "Founding Declaration", "desc": "Formal establishment of the Precinct and adoption of the constitution."},
                    {"title": "Access Bundle", "desc": "Batched approval of initial verified members and ratepayers."},
                    {"title": "Manager Designation", "desc": "Delegation of operational authority to precinct staff and supervisors."},
                    {"title": "Token Wallet Authorization", "desc": "Adoption of the AIT platform and authorization of token expenditure."}
                ]
                for res_data in foundational_resolutions:
                    new_res = UipResolution(
                        organization_id=org.id,
                        title=res_data["title"],
                        description=res_data["desc"],
                        status="PROPOSED",
                        voting_scope="EXCO",
                        quorum_target=50
                    )
                    db.session.add(new_res)
                db.session.commit()

        return "Success: DB Upgraded for Digital Committee Room and 4 Genesis Resolutions Injected"
    except Exception as e:"""

text = text.replace(old_sql, new_sql)

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated dev_upgrade_db to inject resolutions")
