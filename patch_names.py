for file in ["templates/program_uip/dashboards/secretary_workspace.html", "templates/program_uip/dashboards/onboarding_campaign.html", "templates/program_uip/dashboards/secretary_intake.html", "templates/program_uip/dashboards/process_claims.html"]:
    with open(file, "r", encoding="utf-8") as f:
        text = f.read()
    
    text = text.replace("Command Switchboard", "Secretary Dashboard")
    
    with open(file, "w", encoding="utf-8") as f:
        f.write(text)
print("Updated all switchboard references")
