with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

# Replace the incorrect terminology in the flash messages
text = text.replace("Auto-advancing to the next unvoted mandate.", "Auto-advancing to the next unvoted resolution.")
text = text.replace("Inbox Zero! You have successfully cast your vote on all active mandates.", "Inbox Zero! You have successfully cast your vote on all active resolutions.")

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated terminology in flash messages")
