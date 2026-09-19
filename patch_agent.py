with open(r"C:\Users\Sanjith\.gemini\antigravity\brain\62204572-bbdc-4628-9905-cc45ecf51188\agent.md", "a", encoding="utf-8") as f:
    f.write("\n## Future Architectural Roadmap\n")
    f.write("* **Governance Settings Module (Switchboard):** A dynamic configuration page for the Secretary to create and manage custom Sub-Committees (e.g., 'Security Sub-Committee', 'Finance Sub-Committee'). These custom entries will automatically populate the 'Voting Scope' dropdowns across the platform, rather than relying on hardcoded enums.\n")
    f.write("* **Mandatory Digital Voting Engine:** A scheduled background task that actively monitors active resolutions and sends repeated, escalating reminders to any elected committee member who has not yet cast their mandatory vote.\n")
print("Appended roadmap to agent.md")
