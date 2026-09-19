with open("templates/program_uip/dashboards/committee.html", "r", encoding="utf-8") as f:
    text = f.read()

# Wait, does the newly restored committee.html have a Voting Scope column?
# I checked this earlier and it DOES NOT! The table has: Resolution, Status, Action.
